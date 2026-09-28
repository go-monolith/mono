package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Event stream consumer durables are named by types.EventStreamConsumerName.
// Framework versions up to v0.0.11 named them
// "<event-module>-<event>-<version>-<sequence>" instead, where the sequence
// came from a process-global counter that followed module start order. That
// order was not deterministic, so the durable name could change between
// restarts. The code in this file derives the stable names and carries the
// delivery position of those legacy durables over to the stable ones.

// eventConsumerStore is the consumer-management surface used to carry legacy
// durables over to their stable names. The framework's JetStream wrapper
// (*eventbus.NatsJetStream) implements it with concrete methods. It is detected
// through an optional interface assertion, so an EventStream without it (such
// as a test double) skips the migration and only gets the stable names.
type eventConsumerStore interface {
	ConsumerNames(ctx context.Context, stream string) ([]string, error)
	ConsumerInfo(ctx context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error)
	DeleteConsumer(ctx context.Context, stream, consumer string) error
	ResetConsumerToSequence(ctx context.Context, stream, consumer string, seq uint64) error
}

// The framework's JetStream wrapper must keep implementing eventConsumerStore;
// otherwise the migration would silently switch itself off.
var _ eventConsumerStore = (*eventbus.NatsJetStream)(nil)

// eventStreamDurable describes the durable consumer used for one event stream
// consumer registration.
type eventStreamDurable struct {
	// name is the stable durable name.
	name string

	// legacy lists the durables on the same stream that earlier framework
	// versions created for the same event under the order-dependent name.
	legacy []*jetstream.ConsumerInfo

	// sharedLegacy is the number of registrations in this application whose
	// legacy durables share this registration's legacy name prefix on the same
	// stream. Legacy names do not identify the consuming module, so when it is
	// above one the legacy durables cannot be attributed to a registration.
	sharedLegacy int
}

// legacyResetAttempts and legacyResetBackoff bound the retries of a consumer
// reset. A consumer on a replicated stream may not have a leader yet right
// after it is created, which makes the first reset fail transiently.
// legacyResetBackoff is a variable so tests can shorten it.
const legacyResetAttempts = 5

var legacyResetBackoff = 200 * time.Millisecond

// legacyConsumerNameInvalidChars is the character filter of the consumer name
// sanitizer used up to v0.0.11. Unlike types.SanitizeConsumerName it keeps '.'.
var legacyConsumerNameInvalidChars = regexp.MustCompile(`[^a-zA-Z0-9\-_.]`)

// eventStreamConsumerNames returns the stable durable name for each entry,
// index-aligned with entries.
//
// Each name is types.EventStreamConsumerName of the consuming module and the
// event. A name that is already taken on the same stream (the same module
// registering the same event on the same stream again) gets a "-2", "-3", ...
// suffix. Entries are in registration order and a module registers its
// consumers in code order, so the suffixes are deterministic.
func eventStreamConsumerNames(entries []types.EventStreamConsumerEntry) []string {
	names := make([]string, len(entries))
	used := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		stream := entry.Config.Stream.Name
		base := eventStreamConsumerBaseName(entry)
		name := base
		for n := 2; ; n++ {
			if _, taken := used[stream+"\x00"+name]; !taken {
				break
			}
			name = fmt.Sprintf("%s-%d", base, n)
		}
		used[stream+"\x00"+name] = struct{}{}
		names[i] = name
	}
	return names
}

// eventStreamConsumerBaseName returns the durable name of entry before any
// duplicate suffix is applied.
func eventStreamConsumerBaseName(entry types.EventStreamConsumerEntry) string {
	if entry.Module == nil {
		// The registry rejects registrations without a module; this only
		// guards hand-built entries.
		return types.SanitizeConsumerName(entry.EventDef.ModuleName + "-" + entry.EventDef.Name + "-" + entry.EventDef.Version)
	}
	return types.EventStreamConsumerName(entry.Module.Name(), entry.EventDef)
}

// legacyConsumerPrefix returns the part of the legacy durable name that
// precedes "-<sequence>" for def. ok is false when the prefix contains '.',
// which JetStream rejects in consumer names, so no such legacy durable can
// exist.
func legacyConsumerPrefix(def types.BaseEventDefinition) (prefix string, ok bool) {
	prefix = strings.ReplaceAll(def.ModuleName+"-"+def.Name+"-"+def.Version, " ", "-")
	prefix = legacyConsumerNameInvalidChars.ReplaceAllString(prefix, "")
	if strings.Contains(prefix, ".") {
		return "", false
	}
	return prefix, true
}

// isLegacyConsumerName reports whether name is one an earlier framework
// version derived for the event with the given legacy prefix: "<prefix>-<digits>",
// and not one of the stable names the application uses on that stream.
func isLegacyConsumerName(name, prefix string, stable map[string]struct{}) bool {
	if _, ok := stable[name]; ok {
		return false
	}
	sequence, ok := strings.CutPrefix(name, prefix+"-")
	if !ok || sequence == "" {
		return false
	}
	for _, r := range sequence {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// eventConsumerStore returns the consumer-management surface of the event
// stream, or nil when it is unavailable.
func (lm *lifecycleManager) eventConsumerStore() eventConsumerStore {
	if lm.eventBus == nil {
		return nil
	}
	es, err := lm.eventBus.EventStream()
	if err != nil {
		return nil
	}
	store, ok := es.(eventConsumerStore)
	if !ok {
		lm.logger.Debug("Event stream does not support consumer management; legacy durable migration skipped")
		return nil
	}
	return store
}

// planEventStreamDurables derives the stable durable name of every event
// stream consumer registration and finds the legacy durables each one
// replaces. It lists the consumer names of each stream once and fetches the
// info of every name that matches a legacy pattern; only pull consumers count,
// since earlier versions created nothing else. Errors are returned rather than
// ignored, because creating the stable durable without its legacy position
// would replay the stream.
func (lm *lifecycleManager) planEventStreamDurables(ctx context.Context, entries []types.EventStreamConsumerEntry) ([]eventStreamDurable, error) {
	names := eventStreamConsumerNames(entries)
	durables := make([]eventStreamDurable, len(entries))
	for i := range entries {
		durables[i].name = names[i]
	}

	store := lm.eventConsumerStore()
	if store == nil {
		return durables, nil
	}

	stable := make(map[string]map[string]struct{})
	shared := make(map[string]int)
	for i, entry := range entries {
		stream := entry.Config.Stream.Name
		if stable[stream] == nil {
			stable[stream] = make(map[string]struct{})
		}
		stable[stream][names[i]] = struct{}{}
		if prefix, ok := legacyConsumerPrefix(entry.EventDef); ok {
			shared[stream+"\x00"+prefix]++
		}
	}

	consumerNames := make(map[string][]string)
	infos := make(map[string]*jetstream.ConsumerInfo)
	for i, entry := range entries {
		prefix, ok := legacyConsumerPrefix(entry.EventDef)
		if !ok {
			continue
		}
		stream := entry.Config.Stream.Name
		existing, listed := consumerNames[stream]
		if !listed {
			var err error
			existing, err = store.ConsumerNames(ctx, stream)
			if err != nil {
				return nil, fmt.Errorf("failed to look up legacy durables on stream %s: %w", stream, err)
			}
			consumerNames[stream] = existing
		}
		for _, name := range existing {
			if !isLegacyConsumerName(name, prefix, stable[stream]) {
				continue
			}
			info, fetched := infos[stream+"\x00"+name]
			if !fetched {
				var err error
				info, err = store.ConsumerInfo(ctx, stream, name)
				switch {
				case errors.Is(err, jetstream.ErrNotPullConsumer), errors.Is(err, jetstream.ErrConsumerNotFound):
					// A push consumer was not created by the framework, and a
					// consumer deleted since the listing (by another instance or
					// an operator) has nothing left to migrate.
					info = nil
				case err != nil:
					return nil, fmt.Errorf("failed to inspect legacy durable %s on stream %s: %w", name, stream, err)
				}
				infos[stream+"\x00"+name] = info
			}
			if info != nil && info.Config.DeliverSubject == "" {
				durables[i].legacy = append(durables[i].legacy, info)
			}
		}
		durables[i].sharedLegacy = shared[stream+"\x00"+prefix]
	}

	return durables, nil
}

// removeWorkQueueLegacyDurables deletes the legacy durables of a work-queue
// stream. A work-queue stream accepts only one unfiltered consumer, so the
// stable durable cannot be created while a legacy one exists. Deleting is
// lossless there: acknowledged messages are already gone, and the server does
// not purge a work-queue stream when a consumer is deleted, so the stable
// durable receives every message the legacy one had not acknowledged.
//
// The server only accepts work-queue consumers with the "all" deliver policy
// and explicit acknowledgement. A configuration it would reject is reported
// before anything is deleted, so a failed startup leaves the legacy durable in
// place.
func (lm *lifecycleManager) removeWorkQueueLegacyDurables(ctx context.Context, store eventConsumerStore, stream string, cfg types.ConsumerConfig, durable eventStreamDurable) error {
	if cfg.DeliverPolicy != types.DeliverAllPolicy || cfg.AckPolicy != types.AckExplicitPolicy {
		return fmt.Errorf("consumer %s on work-queue stream %s must use the all deliver policy and explicit acknowledgement; legacy durables left in place", durable.name, stream)
	}
	for _, legacy := range durable.legacy {
		err := store.DeleteConsumer(ctx, stream, legacy.Name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			// Already removed since the listing (by another instance or an
			// operator); there is nothing to report.
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to remove legacy durable %s from work-queue stream %s: %w", legacy.Name, stream, err)
		}
		unacknowledged := legacy.NumPending + uint64(max(legacy.NumAckPending, 0))
		lm.logger.Info("Removed legacy event stream consumer durable from work-queue stream; its unacknowledged messages stay in the stream for the new consumer",
			"stream", stream,
			"legacy_consumer", legacy.Name,
			"consumer", durable.name,
			"unacknowledged", unacknowledged)
	}
	return nil
}

// carryOverLegacyPosition positions a newly created stable durable where its
// legacy durables left off, so upgrading does not replay the stream. It never
// deletes legacy durables; it logs a warning for each one instead.
//
// The position is only applied while the stable durable has not delivered
// anything and is still before the legacy floor, which makes the step
// idempotent across restarts and keeps a durable already in use, or already
// positioned by another instance, from being rewound. The resume point is the legacy
// ack floor plus one: the highest floor when a single registration owns the
// legacy durables (each of them acknowledged a contiguous prefix of the
// stream), the lowest when several registrations share them (the choice that
// loses nothing). Legacy durables that never delivered carry no position.
func (lm *lifecycleManager) carryOverLegacyPosition(ctx context.Context, store eventConsumerStore, stream string, consumer jetstream.Consumer, cfg types.ConsumerConfig, retention types.RetentionPolicy, durable eventStreamDurable) error {
	defer lm.warnLegacyDurables(stream, retention, durable)

	var informative []*jetstream.ConsumerInfo
	for _, legacy := range durable.legacy {
		if legacy.Delivered.Consumer > 0 {
			informative = append(informative, legacy)
		}
	}
	if len(informative) == 0 {
		return nil
	}

	var info *jetstream.ConsumerInfo
	if consumer != nil {
		info = consumer.CachedInfo()
	}
	if info == nil {
		var err error
		info, err = store.ConsumerInfo(ctx, stream, durable.name)
		if err != nil {
			return fmt.Errorf("failed to inspect consumer %s on stream %s before carrying over its legacy position: %w", durable.name, stream, err)
		}
	}
	if info.Delivered.Consumer > 0 || info.NumAckPending > 0 {
		// Already consuming: its own position supersedes the legacy one.
		return nil
	}

	floor := informative[0].AckFloor.Stream
	for _, legacy := range informative[1:] {
		if durable.sharedLegacy > 1 {
			floor = min(floor, legacy.AckFloor.Stream)
		} else {
			floor = max(floor, legacy.AckFloor.Stream)
		}
	}
	legacyFloors := make([]string, 0, len(informative))
	for _, legacy := range informative {
		legacyFloors = append(legacyFloors, fmt.Sprintf("%s@%d", legacy.Name, legacy.AckFloor.Stream))
	}

	switch cfg.DeliverPolicy {
	case types.DeliverAllPolicy, types.DeliverByStartSequencePolicy, types.DeliverByStartTimePolicy:
	default:
		// The new consumer starts where its deliver policy says; whatever the
		// legacy durable had not yet processed is not delivered to it.
		var notCarried uint64
		for _, legacy := range informative {
			notCarried = max(notCarried, legacy.NumPending+uint64(max(legacy.NumAckPending, 0)))
		}
		if notCarried > 0 {
			lm.logger.Warn("Cannot carry over the legacy durable position: the consumer's deliver policy does not allow repositioning, so messages the legacy durable had not processed are not delivered to the new consumer",
				"stream", stream,
				"consumer", durable.name,
				"deliver_policy", cfg.DeliverPolicy,
				"legacy_ack_floors", legacyFloors,
				"not_carried_over", notCarried)
		}
		return nil
	}

	if floor == 0 {
		// Nothing was acknowledged, so there is no position to carry over. This
		// is checked after the deliver-policy warning above, which still has to
		// report a backlog the legacy durable delivered but never acknowledged.
		return nil
	}

	if info.Delivered.Stream >= floor {
		// Already at or past the legacy floor: the configured start is later,
		// or another instance has already reset it (a reset reports the floor
		// as the last delivered stream sequence).
		return nil
	}

	resumeSeq := floor + 1
	err := resetConsumerWithRetry(ctx, store, stream, durable.name, resumeSeq)
	if errors.Is(err, jetstream.ErrConsumerInvalidReset) {
		lm.logger.Info("Legacy durable position not applied: the consumer's configured start is later than the legacy ack floor",
			"stream", stream,
			"consumer", durable.name,
			"deliver_policy", cfg.DeliverPolicy,
			"legacy_ack_floors", legacyFloors)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to carry over the legacy durable position to consumer %s on stream %s: %w", durable.name, stream, err)
	}

	if durable.sharedLegacy > 1 {
		lm.logger.Warn("Legacy durables are shared by several consumers of the same event and cannot be attributed; resuming from the lowest ack floor, so some messages may be delivered again",
			"stream", stream,
			"consumer", durable.name,
			"shared_by", durable.sharedLegacy)
	}
	lm.logger.Info("Carried over legacy durable position to the stable consumer name",
		"stream", stream,
		"consumer", durable.name,
		"legacy_ack_floors", legacyFloors,
		"resume_sequence", resumeSeq)
	return nil
}

// warnLegacyDurables logs a warning for every legacy durable that is left in
// place. The framework does not delete them; an operator removes them once the
// stable durable is confirmed to be working.
func (lm *lifecycleManager) warnLegacyDurables(stream string, retention types.RetentionPolicy, durable eventStreamDurable) {
	for _, legacy := range durable.legacy {
		lm.logger.Warn("Legacy event stream consumer durable is no longer used and was left in place; remove it once the new consumer is confirmed to be working (on an interest-retention stream it keeps retaining messages until removed)",
			"stream", stream,
			"legacy_consumer", legacy.Name,
			"consumer", durable.name,
			"legacy_ack_floor", legacy.AckFloor.Stream,
			"legacy_num_pending", legacy.NumPending,
			"retains_messages", retention == types.InterestPolicy,
			"remove_with", fmt.Sprintf("nats consumer rm %s %s", stream, legacy.Name))
	}
}

// resetConsumerWithRetry resets a consumer to seq, retrying transient failures
// (see isTransientResetError) with a linear backoff. Any other error, including
// a rejected reset (jetstream.ErrConsumerInvalidReset), is returned
// immediately.
func resetConsumerWithRetry(ctx context.Context, store eventConsumerStore, stream, consumer string, seq uint64) error {
	var err error
	for attempt := 1; attempt <= legacyResetAttempts; attempt++ {
		err = store.ResetConsumerToSequence(ctx, stream, consumer, seq)
		if err == nil || !isTransientResetError(err) {
			return err
		}
		if attempt == legacyResetAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("consumer reset interrupted: %w (last attempt: %w)", ctx.Err(), err)
		case <-time.After(time.Duration(attempt) * legacyResetBackoff):
		}
	}
	return fmt.Errorf("consumer reset failed after %d attempts: %w", legacyResetAttempts, err)
}

// isTransientResetError reports whether a reset failure may succeed when
// retried: a timeout or missing responder, or a consumer that a replicated
// stream has not finished creating yet.
func isTransientResetError(err error) bool {
	return errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, jetstream.ErrConsumerNotFound)
}
