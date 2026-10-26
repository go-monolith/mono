package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// A cron service is consumed through the durable consumer
// "<module>-<service>-cron", sent to the server with both Name and Durable so
// that the consumer and its delivery position survive restarts and idle
// periods.
//
// Framework versions up to v0.0.12 sent only a Name. The server treats such a
// consumer as non-durable and deletes it once it has had no pull request for
// about 5s and has nothing awaiting acknowledgement. Shutdown stops the fetch
// loops before the modules and the embedded server last, so a shutdown slower
// than that deleted it, as did a handler running longer than AckWait plus 5s.
// The next start recreated it with the default deliver policy (all), and the
// stream, which keeps up to defaultCronMaxMsgsPerSubj past occurrences per
// service, was delivered to the handler again, back to back. The code in this
// file converts such a consumer to a durable in place, keeping its position,
// and positions a newly created consumer after the occurrences already stored.

// cronStartSequenceKey is the consumer metadata entry that records the stream
// sequence a newly created cron consumer must start at. The consumer is
// created with deliver policy all and the entry, then reset to that sequence,
// and then the entry is removed. Because the create itself writes it, a start
// interrupted before the reset finishes the reset on the next start instead of
// delivering the stored occurrences. Removing it afterwards means a later
// reset by an operator is not undone.
const cronStartSequenceKey = "mono.cron-start-sequence"

// cronConsumerStore is the consumer-management surface used to provision cron
// consumers. The framework's JetStream wrapper (*eventbus.NatsJetStream)
// implements it with concrete methods. It is detected through an optional
// interface assertion; an EventStream without it (such as a test double) gets
// a plain create-or-update of a durable consumer that delivers only new
// occurrences.
type cronConsumerStore interface {
	ConsumerInfo(ctx context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error)
	CreateConsumer(ctx context.Context, stream string, cfg types.ConsumerConfig) (jetstream.Consumer, error)
	UpdateConsumer(ctx context.Context, stream string, cfg types.ConsumerConfig) (jetstream.Consumer, error)
	ResetConsumerToSequence(ctx context.Context, stream, consumer string, seq uint64) error
}

// The framework's JetStream wrapper must keep implementing cronConsumerStore;
// otherwise an existing cron consumer could no longer be updated in place.
var _ cronConsumerStore = (*eventbus.NatsJetStream)(nil)

// ensureCronConsumer creates or updates the durable consumer of a cron service
// and returns it. cfg carries the framework-owned settings; its deliver policy,
// start position and metadata are decided here.
//
// An existing consumer is updated in place with the deliver policy and start
// position it already has, because the server rejects changing them. Its
// acknowledgement state is kept: an occurrence that fired while the
// application was down is delivered, and handled occurrences are not delivered
// again (delivery stays at-least-once, so one whose acknowledgement was lost
// at shutdown is).
//
// A missing consumer is created after the occurrences already stored. They
// fired before the consumer existed: most were handled through an earlier
// consumer, and delivering them would run the job once per stored occurrence,
// up to defaultCronMaxMsgsPerSubj times. They are skipped instead, including
// one that was stored while no consumer existed, and their number is logged
// once. The consumer keeps deliver policy all, the policy earlier framework
// versions send, so rolling back to one of them still starts. When several
// instances start at once, only one creates the consumer; the others update
// it.
func (lm *lifecycleManager) ensureCronConsumer(ctx context.Context, es types.EventStream, stream string, cfg types.ConsumerConfig, entry *types.ServiceEntry) (jetstream.Consumer, error) {
	store, ok := es.(cronConsumerStore)
	if !ok {
		cfg.DeliverPolicy = types.DeliverNewPolicy
		return es.CreateOrUpdateConsumer(ctx, stream, cfg)
	}

	info, err := store.ConsumerInfo(ctx, stream, cfg.Name)
	switch {
	case err == nil:
		consumer, err := lm.updateCronConsumer(ctx, store, stream, cfg, info, entry)
		if !isCronConsumerMissing(err) {
			return consumer, err
		}
		// The consumer was deleted between the lookup and the update. This
		// happens to a non-durable consumer of an earlier framework version
		// whose inactivity timeout expires during startup. Create it instead.
	case !isCronConsumerMissing(err):
		return nil, fmt.Errorf("failed to look up cron consumer %q: %w", cfg.Name, err)
	}

	consumer, err := lm.createCronConsumer(ctx, es, store, stream, cfg, entry)
	if !errors.Is(err, jetstream.ErrConsumerExists) {
		return consumer, err
	}

	// Another instance created the consumer after the lookup, with its own
	// start. Update that one instead of replacing it.
	info, err = store.ConsumerInfo(ctx, stream, cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to look up cron consumer %q created by another instance: %w", cfg.Name, err)
	}
	return lm.updateCronConsumer(ctx, store, stream, cfg, info, entry)
}

// updateCronConsumer updates an existing cron consumer in place. It never
// creates one: if the consumer no longer exists the error matches
// isCronConsumerMissing.
func (lm *lifecycleManager) updateCronConsumer(ctx context.Context, store cronConsumerStore, stream string, cfg types.ConsumerConfig, info *jetstream.ConsumerInfo, entry *types.ServiceEntry) (jetstream.Consumer, error) {
	policy, err := eventbus.DeliverPolicyFromJetStream(info.Config.DeliverPolicy)
	if err != nil {
		return nil, fmt.Errorf("cron consumer %q: %w", cfg.Name, err)
	}
	cfg.DeliverPolicy = policy
	cfg.OptStartSeq = info.Config.OptStartSeq
	cfg.OptStartTime = info.Config.OptStartTime

	// A recorded start the consumer has not reached means its creation was
	// interrupted before the reset. Keep the entry until the reset is done.
	// Otherwise the update drops it: an update replaces the metadata.
	start, recorded := cronStartSequence(info.Config.Metadata)
	interrupted := recorded && info.Delivered.Stream+1 < start
	if interrupted {
		cfg.Metadata = cronStartMetadata(start)
	}

	consumer, err := store.UpdateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to update cron consumer %q: %w", cfg.Name, err)
	}

	if info.Config.Durable == "" {
		// When an update turns a non-durable consumer into a durable, the
		// server still gives it the non-durable default inactivity threshold
		// (5s), because it evaluates the threshold against the consumer's
		// previous configuration. The update response does not always show it.
		// A second update, made to what is now a durable, clears it.
		if consumer, err = store.UpdateConsumer(ctx, stream, cfg); err != nil {
			return nil, fmt.Errorf("failed to clear the inactivity threshold of cron consumer %q: %w", cfg.Name, err)
		}
		lm.logger.Info("Cron consumer converted to durable",
			"service", entry.Name,
			"stream", stream,
			"consumer", cfg.Name)
	}

	if interrupted {
		positioned, err := positionCronConsumer(ctx, store, stream, cfg, start)
		if err != nil {
			return nil, err
		}
		if positioned {
			lm.logger.Info("Cron consumer positioned after an interrupted creation",
				"service", entry.Name,
				"stream", stream,
				"consumer", cfg.Name,
				"start_sequence", start)
		}
	}
	return consumer, nil
}

// createCronConsumer creates a missing cron consumer positioned after the
// occurrences already stored, and logs how many it skips.
func (lm *lifecycleManager) createCronConsumer(ctx context.Context, es types.EventStream, store cronConsumerStore, stream string, cfg types.ConsumerConfig, entry *types.ServiceEntry) (jetstream.Consumer, error) {
	stored, lastSeq, err := cronStreamPosition(ctx, es, stream, cfg.FilterSubject)
	if err != nil {
		return nil, fmt.Errorf("failed to read cron stream %s before creating consumer %q: %w", stream, cfg.Name, err)
	}

	// Everything up to lastSeq is skipped; an occurrence stored after this
	// point is delivered. On an empty stream there is nothing to skip.
	start := lastSeq + 1
	cfg.DeliverPolicy = types.DeliverAllPolicy
	if start > 1 {
		cfg.Metadata = cronStartMetadata(start)
	}
	// Create-only: if another instance created the consumer with a different
	// start in the meantime, this fails with ErrConsumerExists instead of
	// overwriting it.
	consumer, err := store.CreateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cron consumer %q: %w", cfg.Name, err)
	}

	positioned := false
	if start > 1 {
		if positioned, err = positionCronConsumer(ctx, store, stream, cfg, start); err != nil {
			return nil, err
		}
	}

	if positioned && stored > 0 {
		lm.logger.Warn("Cron consumer created; skipped occurrences stored before it",
			"service", entry.Name,
			"stream", stream,
			"consumer", cfg.Name,
			"skipped", stored)
	} else {
		lm.logger.Info("Cron consumer created",
			"service", entry.Name,
			"stream", stream,
			"consumer", cfg.Name)
	}
	return consumer, nil
}

// positionCronConsumer resets a cron consumer to start and then removes the
// start sequence recorded in its metadata. cfg is the configuration the
// consumer was created or updated with. It re-reads the consumer first and
// leaves one that has already reached start, for example because another
// instance created it with the same start and positioned it, where it is; it
// then reports false.
func positionCronConsumer(ctx context.Context, store cronConsumerStore, stream string, cfg types.ConsumerConfig, start uint64) (bool, error) {
	info, err := store.ConsumerInfo(ctx, stream, cfg.Name)
	if err != nil {
		return false, fmt.Errorf("failed to look up cron consumer %q before positioning it: %w", cfg.Name, err)
	}
	if info.Delivered.Stream+1 >= start {
		return false, nil
	}
	if err := resetConsumerWithRetry(ctx, store, stream, cfg.Name, start); err != nil {
		return false, fmt.Errorf("failed to position cron consumer %q at stream sequence %d: %w", cfg.Name, start, err)
	}
	cfg.Metadata = nil
	if _, err := store.UpdateConsumer(ctx, stream, cfg); err != nil {
		return false, fmt.Errorf("failed to remove the start sequence of cron consumer %q: %w", cfg.Name, err)
	}
	return true, nil
}

// cronStreamPosition returns, from one snapshot of the cron stream, the number
// of occurrences stored on the target subject and the stream's last sequence.
func cronStreamPosition(ctx context.Context, es types.EventStream, stream, subject string) (stored, lastSeq uint64, err error) {
	handle, err := es.Stream(ctx, stream)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get stream: %w", err)
	}
	if handle == nil {
		return 0, 0, errors.New("stream is unavailable")
	}
	info, err := handle.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read stream info: %w", err)
	}
	return info.State.Subjects[subject], info.State.LastSeq, nil
}

// cronStartMetadata returns consumer metadata recording start.
func cronStartMetadata(start uint64) map[string]string {
	return map[string]string{cronStartSequenceKey: strconv.FormatUint(start, 10)}
}

// cronStartSequence returns the start sequence recorded in a cron consumer's
// metadata, if any.
func cronStartSequence(metadata map[string]string) (uint64, bool) {
	value, ok := metadata[cronStartSequenceKey]
	if !ok {
		return 0, false
	}
	start, err := strconv.ParseUint(value, 10, 64)
	if err != nil || start == 0 {
		return 0, false
	}
	return start, true
}

// isCronConsumerMissing reports whether err means the consumer does not exist.
// A lookup reports ErrConsumerNotFound and an update ErrConsumerDoesNotExist.
func isCronConsumerMissing(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist)
}
