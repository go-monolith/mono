package lifecycle

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/pkg/types"
)

const (
	// LeftoverReportTimeout bounds the leftover report of one start, so
	// that a slow or unavailable JetStream cannot hold up Start.
	LeftoverReportTimeout = 10 * time.Second

	// LeftoverLookupTimeout bounds one lookup of the report. A stream or
	// consumer that does not answer (its leader is being elected) must not
	// use up the time of all the others.
	LeftoverLookupTimeout = 2 * time.Second

	// LeftoverLookupLimit is the number of consumers without a registration,
	// and separately of streams without one, that one start inspects. An
	// account can hold any number of streams and consumers the application
	// created itself; what is beyond the limit is counted and logged, not
	// inspected.
	LeftoverLookupLimit = 64

	// LeftoverConsumerWarning is logged for a durable consumer the framework
	// created for a registration this application no longer has.
	LeftoverConsumerWarning = "Consumer left behind by a stream registration this application no longer has (renamed or removed). " +
		"It is not deleted: another running version of the application, or another application on the same JetStream, " +
		"may still use it, and deleting it loses its position. " +
		"If nothing uses it, remove it"

	// LeftoverStreamWarning is logged for a stream the framework created
	// for a registration this application no longer has.
	LeftoverStreamWarning = "Stream left behind by a stream registration this application no longer has (renamed or removed). " +
		"It keeps storing what is published to its subjects. It is not deleted: another running version of the application, " +
		"or another application on the same JetStream, may still use it, and deleting it loses its messages. " +
		"If nothing uses it, remove it"
)

// unmanagedStreamPrefixes are the name prefixes of streams the leftover
// report never inspects: KV and object store buckets, and cron service
// streams, which have a report of their own (warnOrphanedCronStreams).
var unmanagedStreamPrefixes = []string{"KV_", "OBJ_", types.CronStreamNamePrefix}

// LeftoverStore is the part of JetStream the leftover report reads.
type LeftoverStore interface {
	StreamNames(ctx context.Context) ([]string, error)
	StreamMetadata(ctx context.Context, stream string) (map[string]string, error)
	ConsumerNames(ctx context.Context, stream string) ([]string, error)
	ConsumerInfo(ctx context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error)
}

// The framework's JetStream wrapper must keep implementing LeftoverStore;
// otherwise the report would silently switch itself off.
var _ LeftoverStore = (*eventbus.NatsJetStream)(nil)

// ReportLeftovers logs a warning for every stream and durable consumer that
// carries the framework's registration marker (RegistrationMetadataKey) but
// belongs to no stream consumer service or event stream consumer of this
// start: its registration was renamed or removed. Durable names and service
// subjects are derived from module, service and event names, so a rename
// creates a new durable and leaves the old one on the server, where it keeps
// messages on an interest-retention stream; a removed service leaves a stream
// that keeps storing.
//
// Nothing is deleted: an instance of another version, still running during a
// rolling deploy, or another application on the same JetStream may own what
// looks left behind, and deleting a stream or consumer loses its messages or
// position. Streams and consumers without the marker are the application's
// own, or were created before the marker existed, and are not reported.
//
// The report is best effort. It is bounded by LeftoverReportTimeout,
// LeftoverLookupTimeout per lookup and LeftoverLookupLimit per kind; what it
// could not inspect is logged at Info, a failure at Debug, and it never fails
// Start.
func (lm *lifecycleManager) ReportLeftovers(ctx context.Context) {
	if lm.eventBus == nil {
		return
	}
	es, err := lm.eventBus.EventStream()
	if err != nil {
		return
	}
	store, ok := es.(LeftoverStore)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, LeftoverReportTimeout)
	defer cancel()

	consumers := leftoverBudget{ctx: ctx}
	for _, stream := range lm.streamPlan.StreamNames() {
		lm.reportLeftoverConsumers(store, stream, &consumers)
	}
	streams := leftoverBudget{ctx: ctx}
	lm.reportLeftoverStreams(store, &streams)

	// A listing that failed because the time ran out left names uncounted,
	// so an expired context alone makes the report incomplete too.
	if skipped := consumers.skipped + streams.skipped; skipped > 0 || ctx.Err() != nil {
		lm.logger.Info("Leftover report incomplete: not every stream and consumer without a registration was inspected",
			"inspected", consumers.lookups+streams.lookups,
			"not_inspected", skipped,
			"timed_out", ctx.Err() != nil)
	}
}

// leftoverBudget counts the lookups one pass of the leftover report makes and
// those it leaves out, because the pass reached LeftoverLookupLimit or the
// report its time limit.
type leftoverBudget struct {
	ctx     context.Context
	lookups int
	skipped int
}

// lookup returns a context for one more lookup and its cancel function, or
// false when the name is left out.
func (b *leftoverBudget) lookup() (context.Context, context.CancelFunc, bool) {
	if b.lookups >= LeftoverLookupLimit || b.ctx.Err() != nil {
		b.skipped++
		return nil, nil, false
	}
	b.lookups++
	ctx, cancel := context.WithTimeout(b.ctx, LeftoverLookupTimeout)
	return ctx, cancel, true
}

// reportLeftoverConsumers logs a warning for every marked consumer on a
// registered stream that no registration uses.
func (lm *lifecycleManager) reportLeftoverConsumers(store LeftoverStore, stream string, budget *leftoverBudget) {
	planned := lm.streamPlan.Stream(stream)
	listCtx, cancel := context.WithTimeout(budget.ctx, LeftoverLookupTimeout)
	names, err := store.ConsumerNames(listCtx, stream)
	cancel()
	if err != nil {
		lm.logger.Debug("Could not list consumers for the leftover report", "stream", stream, "error", err)
		return
	}
	sort.Strings(names)
	for _, consumer := range names {
		if _, registered := planned.Consumers[consumer]; registered {
			continue
		}
		ctx, cancel, ok := budget.lookup()
		if !ok {
			continue
		}
		info, err := store.ConsumerInfo(ctx, stream, consumer)
		cancel()
		if err != nil || info == nil {
			// A push consumer was not created by the framework, and one
			// deleted since the listing is no longer left behind.
			lm.logger.Debug("Could not inspect consumer for the leftover report", "stream", stream, "consumer", consumer, "error", err)
			continue
		}
		owner := info.Config.Metadata[RegistrationMetadataKey]
		if owner == "" {
			continue
		}
		lm.logger.Warn(LeftoverConsumerWarning,
			"stream", stream,
			"consumer", consumer,
			"created_for", owner,
			"retains_messages", planned.Retention == types.InterestPolicy,
			"remove_with", fmt.Sprintf("nats consumer rm %s %s", stream, consumer))
	}
}

// reportLeftoverStreams logs a warning for every marked stream that no
// registration uses.
func (lm *lifecycleManager) reportLeftoverStreams(store LeftoverStore, budget *leftoverBudget) {
	listCtx, cancel := context.WithTimeout(budget.ctx, LeftoverLookupTimeout)
	names, err := store.StreamNames(listCtx)
	cancel()
	if err != nil {
		lm.logger.Debug("Could not list streams for the leftover report", "error", err)
		return
	}
	sort.Strings(names)
	for _, stream := range names {
		if lm.streamPlan.Stream(stream) != nil || isUnmanagedStreamName(stream) {
			continue
		}
		ctx, cancel, ok := budget.lookup()
		if !ok {
			continue
		}
		metadata, err := store.StreamMetadata(ctx, stream)
		cancel()
		if err != nil {
			lm.logger.Debug("Could not inspect stream for the leftover report", "stream", stream, "error", err)
			continue
		}
		owner := metadata[RegistrationMetadataKey]
		if owner == "" {
			continue
		}
		lm.logger.Warn(LeftoverStreamWarning,
			"stream", stream,
			"created_for", owner,
			"remove_with", "nats stream rm "+stream)
	}
}

// isUnmanagedStreamName reports whether the leftover report skips the stream
// by its name (see unmanagedStreamPrefixes).
func isUnmanagedStreamName(name string) bool {
	for _, prefix := range unmanagedStreamPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
