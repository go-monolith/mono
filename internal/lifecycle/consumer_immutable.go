package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/pkg/types"
)

// errCodeConsumerInvalidConfig is the JetStream API error code (10012) the
// server answers a consumer update with when it changes a setting that cannot
// be changed on an existing consumer. It is the generic consumer
// configuration code, so it alone does not identify such a change; the
// consumer is read back to find out (see UpdateKeepingImmutable).
const errCodeConsumerInvalidConfig jetstream.ErrorCode = 10012

// DefaultConsumerMaxWaiting is the MaxWaiting the server gives a pull consumer
// whose configuration leaves it at zero. It is only used to tell whether a
// configuration with MaxWaiting zero asks for another value than an existing
// consumer has; an integration test checks it against the server.
const DefaultConsumerMaxWaiting = 512

// KeptConsumerSettingWarning is logged for every setting of an existing
// consumer that the configuration changes although NATS cannot change it.
const KeptConsumerSettingWarning = "Consumer setting cannot be changed on an existing consumer; the existing value is kept and the configured one is not applied. " +
	"To apply it, delete the consumer: it is then created again and starts from its deliver policy, so already handled messages may be delivered again"

// KeptConsumerSetting is one setting of an existing consumer that was kept
// instead of the value a configuration asks for.
type KeptConsumerSetting struct {
	// Name is the ConsumerConfig field.
	Name string

	// Configured and Kept are the value the configuration asks for and the
	// value the consumer keeps.
	Configured string
	Kept       string

	// Note says what keeping the value means for the handler, when it means
	// more than "nothing changes".
	Note string
}

// IsConsumerInvalidConfig reports whether err is the server rejecting a
// consumer configuration with the generic code (10012).
func IsConsumerInvalidConfig(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeConsumerInvalidConfig
}

// KeepImmutableConsumerSettings returns cfg with the settings NATS cannot
// change on an existing consumer taken from existing, and the settings for
// which that differs from what cfg asks for:
//
//   - DeliverPolicy with OptStartSeq and OptStartTime, kept together: they
//     only decide where a new consumer starts;
//   - AckPolicy, ReplayPolicy, MaxWaiting and MemoryStorage.
//
// Everything else in cfg is unchanged. An error is returned when existing
// carries a policy the framework does not know.
func KeepImmutableConsumerSettings(cfg types.ConsumerConfig, existing jetstream.ConsumerConfig) (types.ConsumerConfig, []KeptConsumerSetting, error) {
	deliver, err := eventbus.DeliverPolicyFromJetStream(existing.DeliverPolicy)
	if err != nil {
		return cfg, nil, err
	}
	ack, err := eventbus.AckPolicyFromJetStream(existing.AckPolicy)
	if err != nil {
		return cfg, nil, err
	}
	replay, err := eventbus.ReplayPolicyFromJetStream(existing.ReplayPolicy)
	if err != nil {
		return cfg, nil, err
	}

	var kept []KeptConsumerSetting
	keep := func(name string, configured, existing any, note string) {
		kept = append(kept, KeptConsumerSetting{Name: name, Configured: fmt.Sprint(configured), Kept: fmt.Sprint(existing), Note: note})
	}

	if cfg.DeliverPolicy != deliver {
		keep("DeliverPolicy", deliverPolicyName(cfg.DeliverPolicy), deliverPolicyName(deliver), "")
	}
	if cfg.DeliverPolicy == types.DeliverByStartSequencePolicy && deliver == types.DeliverByStartSequencePolicy && cfg.OptStartSeq != existing.OptStartSeq {
		keep("OptStartSeq", cfg.OptStartSeq, existing.OptStartSeq, "")
	}
	if cfg.DeliverPolicy == types.DeliverByStartTimePolicy && deliver == types.DeliverByStartTimePolicy && !sameStartTime(cfg.OptStartTime, existing.OptStartTime) {
		keep("OptStartTime", startTimeText(cfg.OptStartTime), startTimeText(existing.OptStartTime),
			"a start time computed when the application starts differs on every start; use a fixed time")
	}
	cfg.DeliverPolicy, cfg.OptStartSeq, cfg.OptStartTime = deliver, existing.OptStartSeq, existing.OptStartTime

	if cfg.AckPolicy != ack {
		keep("AckPolicy", ackPolicyName(cfg.AckPolicy), ackPolicyName(ack),
			"the handler must keep acknowledging the way the kept policy requires")
		cfg.AckPolicy = ack
	}
	if cfg.ReplayPolicy != replay {
		keep("ReplayPolicy", replayPolicyName(cfg.ReplayPolicy), replayPolicyName(replay), "")
		cfg.ReplayPolicy = replay
	}
	if wanted := configuredMaxWaiting(cfg.MaxWaiting); wanted != existing.MaxWaiting {
		keep("MaxWaiting", wanted, existing.MaxWaiting, "")
		cfg.MaxWaiting = existing.MaxWaiting
	}
	if cfg.MemoryStorage != existing.MemoryStorage {
		keep("MemoryStorage", cfg.MemoryStorage, existing.MemoryStorage, "")
		cfg.MemoryStorage = existing.MemoryStorage
	}
	return cfg, kept, nil
}

// configuredMaxWaiting returns the MaxWaiting a configuration asks for: zero,
// or a negative value, asks for the server default.
func configuredMaxWaiting(configured int) int {
	if configured <= 0 {
		return DefaultConsumerMaxWaiting
	}
	return configured
}

// sameStartTime reports whether two start times are the same instant, or
// both unset.
func sameStartTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// startTimeText renders a start time for a log line.
func startTimeText(t *time.Time) string {
	if t == nil {
		return "none"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// deliverPolicyName names a deliver policy for a log line.
func deliverPolicyName(p types.DeliverPolicy) string {
	switch p {
	case types.DeliverAllPolicy:
		return "all"
	case types.DeliverLastPolicy:
		return "last"
	case types.DeliverNewPolicy:
		return "new"
	case types.DeliverByStartSequencePolicy:
		return "by start sequence"
	case types.DeliverByStartTimePolicy:
		return "by start time"
	case types.DeliverLastPerSubjectPolicy:
		return "last per subject"
	default:
		return fmt.Sprintf("unknown (%d)", p)
	}
}

// ackPolicyName names an ack policy for a log line.
func ackPolicyName(p types.AckPolicy) string {
	switch p {
	case types.AckExplicitPolicy:
		return "explicit"
	case types.AckAllPolicy:
		return "all"
	case types.AckNonePolicy:
		return "none"
	default:
		return fmt.Sprintf("unknown (%d)", p)
	}
}

// replayPolicyName names a replay policy for a log line.
func replayPolicyName(p types.ReplayPolicy) string {
	switch p {
	case types.ReplayInstantPolicy:
		return "instant"
	case types.ReplayOriginalPolicy:
		return "original"
	default:
		return fmt.Sprintf("unknown (%d)", p)
	}
}

// UpdateKeepingImmutable creates or updates a consumer with update and, when
// the server rejects the configuration because it changes a setting that
// cannot be changed on an existing consumer, repeats the update once with the
// existing values of those settings and logs a warning for each of them. It
// returns the consumer and the configuration that was applied.
//
// A deployment that changed such a setting used to fail Start on every
// restart: the consumer is durable and does not go away by itself. Keeping
// the existing value starts the application, and the warning, repeated at
// every start while the configuration differs, says the change was not
// applied and how to apply it.
//
// The consumer is read back only after the update failed with the consumer
// configuration code, so a start that succeeds makes no extra request and
// sends exactly the configuration it is given. The original error is
// returned when the consumer agrees with cfg on these settings (the update
// was rejected for another reason) or cannot be read back; a read-back that
// failed for a reason that clears by itself returns that error instead, so
// the step is retried. A consumer that was deleted in between gets the
// configured update once more. When the repeated update fails too, for
// example because another setting does not fit a kept policy, its error is
// returned with the kept settings and the command that deletes the consumer.
func (lm *lifecycleManager) UpdateKeepingImmutable(
	ctx context.Context,
	es types.EventStream,
	stream string,
	cfg types.ConsumerConfig,
	owner string,
	update func(types.ConsumerConfig) (jetstream.Consumer, error),
) (jetstream.Consumer, types.ConsumerConfig, error) {
	consumer, err := update(cfg)
	if err == nil || !IsConsumerInvalidConfig(err) {
		return consumer, cfg, err
	}
	handle, lookupErr := es.Stream(ctx, stream)
	if lookupErr != nil || handle == nil {
		return nil, cfg, readBackError(cfg.Name, err, lookupErr)
	}
	existing, lookupErr := handle.Consumer(ctx, cfg.Name)
	if errors.Is(lookupErr, jetstream.ErrConsumerNotFound) {
		// Deleted since the rejected update: there is nothing to keep. The
		// update is sent again as configured, and creates the consumer or
		// reports it missing, as the caller expects.
		consumer, err = update(cfg)
		return consumer, cfg, err
	}
	if lookupErr != nil || existing == nil || existing.CachedInfo() == nil {
		return nil, cfg, readBackError(cfg.Name, err, lookupErr)
	}
	keptCfg, kept, keepErr := KeepImmutableConsumerSettings(cfg, existing.CachedInfo().Config)
	if keepErr != nil || len(kept) == 0 {
		return nil, cfg, err
	}
	consumer, retryErr := update(keptCfg)
	if retryErr != nil {
		names := make([]string, 0, len(kept))
		for _, setting := range kept {
			names = append(names, setting.Name)
		}
		return nil, cfg, fmt.Errorf("the update that keeps the existing %s of consumer %s, which NATS cannot change, was rejected as well; "+
			"another setting may not fit a kept value (to apply the configuration as it is, delete the consumer: nats consumer rm %s %s): %w",
			strings.Join(names, ", "), cfg.Name, stream, cfg.Name, retryErr)
	}
	for _, setting := range kept {
		args := []any{
			"registration", owner,
			"stream", stream,
			"consumer", cfg.Name,
			"setting", setting.Name,
			"configured", setting.Configured,
			"kept", setting.Kept,
			"apply_with", fmt.Sprintf("nats consumer rm %s %s", stream, cfg.Name),
		}
		if setting.Note != "" {
			args = append(args, "note", setting.Note)
		}
		lm.logger.Warn(KeptConsumerSettingWarning, args...)
	}
	return consumer, keptCfg, nil
}

// readBackError returns the error of an update whose consumer could not be
// read back: the rejection itself, unless the lookup failed for a reason that
// clears by itself (a timeout, no responders while a cluster forms). Then the
// lookup error is returned, so that the setup step is retried instead of
// failing Start with a rejection that could have been resolved.
func readBackError(consumer string, updateErr, lookupErr error) error {
	if lookupErr != nil && isTransientJetStreamError(lookupErr) {
		return fmt.Errorf("failed to read back consumer %s after its update was rejected (%v): %w", consumer, updateErr, lookupErr)
	}
	return updateErr
}
