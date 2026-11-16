package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStream in a cluster is not usable the moment the embedded server is: the
// meta group has no leader until a quorum of the servers is routed, a
// replicated stream or consumer can only be updated once its replicas are
// current, and a server that restarted has to catch up first. Servers that
// start together (a Kubernetes StatefulSet) would otherwise fail their first
// JetStream call, roll back and restart, each restart tearing down the routes
// the others were waiting for. Start therefore waits for JetStream before it
// starts plugins and modules, and retries every JetStream setup step while it
// fails with an error that clears by itself, each bounded by the JetStream
// ready timeout (types.DefaultJetStreamReadyTimeout unless configured).

// jetStreamRetryMinBackoff and jetStreamRetryMaxBackoff bound the wait between
// two attempts of a retried JetStream step; the wait doubles from the first to
// the second.
const (
	jetStreamRetryMinBackoff = 250 * time.Millisecond
	jetStreamRetryMaxBackoff = 2 * time.Second
	// jetStreamWaitLogInterval is how often a step still waiting is logged.
	jetStreamWaitLogInterval = 10 * time.Second
	// jetStreamAttemptTimeout bounds one attempt of a retried step. Without it
	// a JetStream request inherits the whole retry budget as its deadline: a
	// request sent while the cluster has no leader gets no answer and would
	// wait out the budget instead of being retried once the leader is there.
	jetStreamAttemptTimeout = 15 * time.Second
	// jetStreamProbeTimeout bounds one readiness probe (a single request).
	jetStreamProbeTimeout = 2 * time.Second
)

// nonDurableProtectThreshold is the inactivity threshold given at startup to
// the framework's consumers that the server still treats as non-durable (see
// protectNonDurableConsumers). It only has to outlast the start: the setup
// then converts stream consumer and cron consumers to durables, and a legacy
// event stream durable keeps it, so the server removes one that is no longer
// used an hour after its last pull instead of five seconds.
const nonDurableProtectThreshold = time.Hour

// jetStreamReadiness reports whether JetStream answers. The framework's
// JetStream wrapper implements it; an EventStream without it (a test double)
// is treated as always ready.
type jetStreamReadiness interface {
	Ready(ctx context.Context) error
}

// consumerProtector lists consumers and extends the inactivity threshold of a
// non-durable one. The framework's JetStream wrapper implements it; without it
// (a test double) nothing is protected.
type consumerProtector interface {
	StreamNames(ctx context.Context) ([]string, error)
	ConsumerNames(ctx context.Context, stream string) ([]string, error)
	ExtendInactiveThreshold(ctx context.Context, stream, consumer string, d time.Duration) (bool, error)
}

// The framework's JetStream wrapper must keep implementing both; otherwise the
// wait and the protection would silently switch themselves off.
var (
	_ jetStreamReadiness = (*eventbus.NatsJetStream)(nil)
	_ consumerProtector  = (*eventbus.NatsJetStream)(nil)
)

// eventStream returns the event bus's JetStream access, or false when
// JetStream is not enabled.
func (lm *lifecycleManager) eventStream() (types.EventStream, bool) {
	if lm.eventBus == nil {
		return nil, false
	}
	es, err := lm.eventBus.EventStream()
	if err != nil || es == nil {
		return nil, false
	}
	return es, true
}

// waitForJetStream waits until JetStream answers, retrying while it is not
// available yet. It reports whether JetStream is enabled: without it (the
// event bus has no JetStream access, or the server answers "JetStream not
// enabled") it returns false at once.
func (lm *lifecycleManager) waitForJetStream(ctx context.Context) (bool, error) {
	es, ok := lm.eventStream()
	if !ok {
		return false, nil
	}
	ready, ok := es.(jetStreamReadiness)
	if !ok {
		return true, nil
	}
	err := lm.retryJetStream(ctx, "wait for JetStream", func(ctx context.Context) error {
		// One request, answered at once when a leader exists: probe often.
		probeCtx, cancel := context.WithTimeout(ctx, jetStreamProbeTimeout)
		defer cancel()
		return ready.Ready(probeCtx)
	})
	switch {
	case err == nil:
		return true, nil
	case isJetStreamNotEnabled(err):
		return false, nil
	default:
		return false, fmt.Errorf("JetStream is not available: %w", err)
	}
}

// isJetStreamNotEnabled reports whether err says JetStream is not enabled on
// the server or for the account.
func isJetStreamNotEnabled(err error) bool {
	if errors.Is(err, jetstream.ErrJetStreamNotEnabled) || errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == jetstream.JSErrCodeJetStreamNotEnabled || apiErr.ErrorCode == jetstream.JSErrCodeJetStreamNotEnabledForAccount)
}

// protectNonDurableConsumers extends, before any module starts, the inactivity
// threshold of the framework's consumers that the server still treats as
// non-durable, so that the server cannot delete them before the setup has
// converted them to durables or carried their position over.
//
// Earlier framework versions created stream consumer service consumers, cron
// consumers (up to v0.0.12) and event stream consumers (up to v0.0.11) with a
// Name only. Since nats-server v2.14.10 such a consumer is deleted about five
// seconds after the server starts unless it is pulled, and the setup runs only
// after every module has started; a deleted consumer would be recreated from
// its deliver policy and replay its stream. A consumer is taken to be the
// framework's when its name starts with the name of a registered module or
// plugin followed by "-" (<module>-<service>, <module>-<service>-cron,
// <event-module>-<event>-<version>-<n>). Only non-durable pull consumers left
// at the server's default threshold are changed, and only their threshold: a
// consumer whose threshold was chosen explicitly is not the framework's. The
// pass is bounded by the JetStream ready timeout as a whole. It runs right
// after JetStream answers, before plugins and modules start, since the
// threshold runs from when the consumer's leader is elected.
//
// Best effort: a failure is logged and the start goes on.
func (lm *lifecycleManager) protectNonDurableConsumers(ctx context.Context, moduleNames []string) {
	es, ok := lm.eventStream()
	if !ok || len(moduleNames) == 0 {
		return
	}
	protector, ok := es.(consumerProtector)
	if !ok {
		return
	}
	prefixes := make([]string, 0, len(moduleNames))
	for _, name := range moduleNames {
		prefixes = append(prefixes, types.SanitizeConsumerName(name)+"-")
	}
	// One bound for the whole pass: it is best effort and must not hold the
	// start up for a retry budget per stream or consumer.
	ctx, cancel := context.WithTimeout(ctx, lm.jetStreamReadyTimeout())
	defer cancel()

	var streams []string
	if err := lm.retryJetStream(ctx, "list streams", func(ctx context.Context) error {
		var err error
		streams, err = protector.StreamNames(ctx)
		return err
	}); err != nil {
		lm.logger.Warn("Could not list streams to protect non-durable consumers", "error", err)
		return
	}

	protected := 0
	for _, stream := range streams {
		var names []string
		if err := lm.retryJetStream(ctx, "list consumers of "+stream, func(ctx context.Context) error {
			var err error
			names, err = protector.ConsumerNames(ctx, stream)
			return err
		}); err != nil {
			lm.logger.Warn("Could not list consumers to protect non-durable ones", "stream", stream, "error", err)
			continue
		}
		for _, name := range names {
			if !hasAnyPrefix(name, prefixes) {
				continue
			}
			if ctx.Err() != nil {
				lm.logger.Warn("Stopped protecting non-durable consumers: out of time", "stream", stream)
				return
			}
			var extended bool
			err := lm.retryJetStream(ctx, "protect consumer "+name, func(ctx context.Context) error {
				var err error
				extended, err = protector.ExtendInactiveThreshold(ctx, stream, name, nonDurableProtectThreshold)
				return err
			})
			switch {
			case err != nil:
				lm.logger.Warn("Could not protect a non-durable consumer; the server may delete it before it is set up",
					"stream", stream, "consumer", name, "error", err)
			case extended:
				protected++
				lm.logger.Debug("Protected non-durable consumer until setup", "stream", stream, "consumer", name)
			}
		}
	}
	if protected > 0 {
		lm.logger.Info("Protected non-durable consumers until they are set up",
			"count", protected, "inactive_threshold", nonDurableProtectThreshold)
	}
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// retryJetStream runs step until it succeeds, retrying while it fails with an
// error isTransientJetStreamError accepts, for at most the JetStream ready
// timeout. Any other error, or the end of ctx, ends it at once. Each attempt
// receives a context bounded by jetStreamAttemptTimeout and by the overall
// timeout. what names the step in logs and errors.
func (lm *lifecycleManager) retryJetStream(ctx context.Context, what string, step func(context.Context) error) error {
	budget := lm.jetStreamReadyTimeout()
	stepCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	start := time.Now()
	lastLog := time.Time{}
	backoff := jetStreamRetryMinBackoff
	for attempt := 1; ; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(stepCtx, jetStreamAttemptTimeout)
		err := step(attemptCtx)
		cancelAttempt()
		if err == nil {
			if attempt > 1 {
				lm.logger.Info("JetStream step succeeded after waiting",
					"step", what, "attempts", attempt, "waited", time.Since(start).Round(time.Millisecond).String())
			}
			return nil
		}
		if ctx.Err() != nil || !isTransientJetStreamError(err) {
			return err
		}
		if stepCtx.Err() != nil {
			return fmt.Errorf("%s: still failing after %s (WithJetStreamReadyTimeout): %w", what, budget, err)
		}
		if time.Since(lastLog) >= jetStreamWaitLogInterval {
			lastLog = time.Now()
			fields := []any{"step", what, "attempt", attempt, "waited", time.Since(start).Round(time.Millisecond).String(), "reason", err}
			if hasJetStreamCode(err, 10005) {
				// Retried because the servers may not all be known yet, but a
				// replica count above the number of servers never succeeds.
				fields = append(fields, "hint", "no suitable peers: check that Replicas (WithJetStreamDefaultReplicas, "+
					"CronServiceConfig.Replicas, StreamConfig.Replicas) does not exceed the number of servers")
			}
			lm.logger.Info("Waiting for JetStream", fields...)
		}
		select {
		case <-stepCtx.Done():
			if ctx.Err() != nil {
				return err
			}
			return fmt.Errorf("%s: still failing after %s (WithJetStreamReadyTimeout): %w", what, budget, err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, jetStreamRetryMaxBackoff)
	}
}

// transientJetStreamCodes are the JetStream API error codes that clear by
// themselves while a cluster forms or a server catches up.
var transientJetStreamCodes = map[jetstream.ErrorCode]bool{
	10004: true, // JSClusterIncompleteErr: incomplete results
	10005: true, // JSClusterNoPeersErr: no suitable peers for placement (peers not known yet)
	10008: true, // JSClusterNotAvailErr: JetStream system temporarily unavailable
	10118: true, // JSStreamOfflineErr: offline while its leader recovers
	10119: true, // JSConsumerOfflineErr
	// Not 10194/10195 (offline with a reason): the reason is an unsupported
	// configuration or API level, or a decoding error, which do not clear.
}

// hasJetStreamCode reports whether err carries the JetStream API error code.
func hasJetStreamCode(err error, code jetstream.ErrorCode) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == code
}

// isTransientJetStreamError reports whether a JetStream call may succeed when
// retried: it timed out or found no responder (no leader yet), or JetStream
// reported itself temporarily unavailable. A timeout counts only for the call
// itself; the caller checks whether its own context ended.
func isTransientJetStreamError(err error) bool {
	if errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) {
		return true
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return transientJetStreamCodes[apiErr.ErrorCode]
	}
	return false
}
