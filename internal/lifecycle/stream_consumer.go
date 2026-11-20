package lifecycle

import (
	"context"
	"fmt"

	"github.com/go-monolith/mono/internal/jsprotect"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// A stream consumer service is consumed through the durable consumer
// "<module>-<service>", sent with both Name and Durable so that the server
// keeps it, and its delivery position, across restarts and idle periods.
//
// Framework versions up to v0.1.x sent only a Name. The server treats such a
// consumer as non-durable and deletes it once it has not been pulled for about
// 5s: during a slow shutdown, while every instance is down, and since
// nats-server v2.14.10 also 5s after a server starts unless a fetch loop pulls
// it by then. The next start recreated it from its deliver policy, so with the
// default (all) the whole stream was delivered again. ensureStreamConsumer
// converts such a consumer to a durable in place, keeping its position;
// protectNonDurableConsumers keeps the server from deleting it before then.

// ensureStreamConsumer creates or updates the durable consumer of a stream
// consumer service and returns it. cfg already carries the durable name.
//
// An existing non-durable consumer of an earlier version is converted with two
// updates: the server still applies the non-durable inactivity threshold to the
// update that sets Durable (it evaluates the threshold against the previous
// configuration), and the second update, to what is now a durable, clears it
// (or sets the caller's InactiveThreshold). The first update sends the start
// protection threshold (an hour) rather than none, so that a conversion
// interrupted between the two updates leaves a durable the server keeps for an
// hour instead of five seconds; the next start finishes it.
//
// The deliver policy, start position, ack policy, replay policy, MaxWaiting
// and MemoryStorage of an existing consumer cannot be changed by an update.
// A configuration that changes one of them keeps the existing value and logs
// a warning (see UpdateKeepingImmutable); since the consumer is durable it no
// longer disappears by itself, so deleting it (nats consumer rm <stream>
// <consumer>) is what applies the change.
func (lm *lifecycleManager) ensureStreamConsumer(ctx context.Context, es types.EventStream, stream string, cfg types.ConsumerConfig, entry *types.ServiceEntry) (jetstream.Consumer, error) {
	owner := "stream consumer service"
	if entry != nil {
		owner = StreamConsumerServiceOwner(entry)
	}
	// Settings NATS cannot change on an existing consumer are kept as they
	// are, with a warning (see UpdateKeepingImmutable).
	createOrUpdate := func(cfg types.ConsumerConfig) (jetstream.Consumer, error) {
		consumer, _, err := lm.UpdateKeepingImmutable(ctx, es, stream, cfg, owner, func(c types.ConsumerConfig) (jetstream.Consumer, error) {
			return lm.createOrUpdateStreamConsumer(ctx, es, stream, c)
		})
		return consumer, err
	}

	store, ok := es.(cronConsumerStore)
	if !ok {
		return createOrUpdate(cfg)
	}

	info, err := store.ConsumerInfo(ctx, stream, cfg.Name)
	if err == nil && info != nil && entry != nil && entry.StreamConsumerConfig != nil {
		// A filter the consumer was given while its stream was shared is
		// kept once the stream is no longer shared (the lookup is the one
		// this function makes anyway).
		cfg = KeepRegistrationFilter(cfg, info.Config, entry.StreamConsumerConfig.Stream.Subjects)
	}
	switch {
	case isCronConsumerMissing(err):
		return createOrUpdate(cfg)
	case err != nil:
		return nil, fmt.Errorf("failed to look up consumer %s: %w", cfg.Name, err)
	case info.Config.Durable != "":
		return createOrUpdate(cfg)
	}

	// The first update of the conversion decides which immutable settings
	// are kept; the second one sends the same.
	_, cfg, err = lm.UpdateKeepingImmutable(ctx, es, stream, cfg, owner, func(c types.ConsumerConfig) (jetstream.Consumer, error) {
		return store.UpdateConsumer(ctx, stream, protectedConversion(c))
	})
	if isCronConsumerMissing(err) {
		// Deleted between the lookup and the update (its inactivity
		// threshold expired): create it instead.
		return createOrUpdate(cfg)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to convert consumer %s to durable: %w", cfg.Name, err)
	}
	consumer, err := store.UpdateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to clear the inactivity threshold of consumer %s: %w", cfg.Name, err)
	}
	lm.logger.Info("Stream consumer converted to durable",
		"service", entry.Name,
		"stream", stream,
		"consumer", cfg.Name)
	return consumer, nil
}

func (lm *lifecycleManager) createOrUpdateStreamConsumer(ctx context.Context, es types.EventStream, stream string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := es.CreateOrUpdateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer %s: %w", cfg.Name, err)
	}
	return consumer, nil
}

// protectedConversion returns cfg for the first update of a conversion from a
// non-durable consumer: with at least the start protection threshold, since
// the server would otherwise give the new durable the 5s non-durable default
// until the second update lands.
func protectedConversion(cfg types.ConsumerConfig) types.ConsumerConfig {
	if cfg.InactiveThreshold < jsprotect.Threshold {
		cfg.InactiveThreshold = jsprotect.Threshold
	}
	return cfg
}
