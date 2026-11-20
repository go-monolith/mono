package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// errCodeStreamInvalidConfig is the JetStream API error code (10052) the
// server answers a stream update with when it changes the storage type, or
// the retention policy to or from work-queue. It is the generic invalid
// configuration code, so it alone does not identify such a change; the stream
// is read back to find out (see explainImmutableStreamChange).
const errCodeStreamInvalidConfig jetstream.ErrorCode = 10052

// immutableStreamHelp is the way out named by every ErrStreamConfigImmutable.
// The framework does not delete the stream itself: its messages and the
// position of every consumer on it would be lost, and no other stream can
// take over its subjects without a moment in which a publish is dropped.
const immutableStreamHelp = "NATS cannot change the storage type of an existing stream, or its retention to or from work-queue; keep the existing value, " +
	"or register a new service name or event version on a new stream and retire the old one once it is drained " +
	"(deleting the stream loses its messages and consumer positions)"

// oneWayStreamHelp is the way out named when a setting that can only be
// switched on is configured off.
const oneWayStreamHelp = "NATS cannot switch DenyDelete, DenyPurge, Sealed, AllowMsgTTL or AllowMsgSchedules off again on an existing stream; " +
	"keep the setting enabled, or register a new service name or event version on a new stream and retire the old one once it is drained " +
	"(deleting the stream loses its messages and consumer positions)"

// OneWayStreamSetting is a stream setting that NATS lets a configuration
// switch on for an existing stream but never off again.
type OneWayStreamSetting struct {
	// Name is the StreamConfig field.
	Name string

	// Existing and Configured read the setting from the stream as the
	// server holds it and from the configuration of a registration.
	Existing   func(jetstream.StreamConfig) bool
	Configured func(types.StreamConfig) bool
}

// OneWayStreamSettings lists the settings a stream cannot have switched off
// once they are on. Sealed comes first: sealing a stream also switches
// DenyDelete and DenyPurge on (see SealedStreamConfig).
var OneWayStreamSettings = []OneWayStreamSetting{
	{"Sealed", func(c jetstream.StreamConfig) bool { return c.Sealed }, func(c types.StreamConfig) bool { return c.Sealed }},
	{"DenyDelete", func(c jetstream.StreamConfig) bool { return c.DenyDelete }, func(c types.StreamConfig) bool { return c.DenyDelete }},
	{"DenyPurge", func(c jetstream.StreamConfig) bool { return c.DenyPurge }, func(c types.StreamConfig) bool { return c.DenyPurge }},
	{"AllowMsgTTL", func(c jetstream.StreamConfig) bool { return c.AllowMsgTTL }, func(c types.StreamConfig) bool { return c.AllowMsgTTL }},
	{"AllowMsgSchedules", func(c jetstream.StreamConfig) bool { return c.AllowMsgSchedules }, func(c types.StreamConfig) bool { return c.AllowMsgSchedules }},
}

// SealedStreamConfig returns cfg as a sealed stream has to be written. When
// the server seals a stream it switches DenyDelete and DenyPurge on and
// AllowRollup off, and it refuses to switch the first two off again. A
// configuration with Sealed alone would therefore be rejected on every start
// after the one that sealed the stream, and one with Sealed and AllowRollup
// already when sealing (roll-ups need the purge permission). A configuration
// that is not sealed is returned unchanged.
func SealedStreamConfig(cfg types.StreamConfig) types.StreamConfig {
	if cfg.Sealed {
		cfg.DenyDelete = true
		cfg.DenyPurge = true
		cfg.AllowRollup = false
	}
	return cfg
}

// NormalisedStreamConfig returns cfg with the one-way settings as the server
// ends up holding them, to compare a configuration with an existing stream or
// with another registration without reporting what the server changed by
// itself:
//
//   - a subject delete marker TTL enables per-message TTLs;
//   - message schedules and a subject delete marker TTL need roll-ups, for
//     which the server clears DenyPurge (see ClearsDenyPurge);
//   - sealing enables DenyDelete and DenyPurge (see SealedStreamConfig).
func NormalisedStreamConfig(cfg types.StreamConfig) types.StreamConfig {
	if cfg.SubjectDeleteMarkerTTL > 0 {
		cfg.AllowMsgTTL = true
	}
	if ClearsDenyPurge(cfg) {
		cfg.DenyPurge = false
	}
	return SealedStreamConfig(cfg)
}

// ClearsDenyPurge reports whether the server switches DenyPurge off for cfg
// by itself: message schedules and a subject delete marker TTL need roll-ups,
// which need the purge permission. A configuration that sets AllowRollup
// together with DenyPurge is rejected outright instead, and a sealed stream
// keeps DenyPurge.
func ClearsDenyPurge(cfg types.StreamConfig) bool {
	return !cfg.Sealed && !cfg.AllowRollup && (cfg.AllowMsgSchedules || cfg.SubjectDeleteMarkerTTL > 0)
}

// CheckSealedStream returns an error wrapping ErrInvalidConfiguration when
// cfg seals a stream that runs message schedules. The server accepts the
// update that seals such a stream and rejects every write after it, and a
// sealed stream cannot be unsealed, so the stream would fail every later
// start for good. The check runs before anything is written.
func CheckSealedStream(cfg types.StreamConfig, owner string) error {
	if cfg.Sealed && cfg.AllowMsgSchedules {
		return fmt.Errorf("%w: stream %s of %s is configured with Sealed and AllowMsgSchedules; "+
			"NATS rejects every write of a sealed stream that has message schedules enabled, and a sealed stream cannot be unsealed, "+
			"so the stream is not sealed: remove Sealed, or use a stream without message schedules",
			monoerrors.ErrInvalidConfiguration, cfg.Name, owner)
	}
	return nil
}

// OneWayStreamDiff returns one phrase per one-way setting that the existing
// stream has on and cfg, as the server would hold it (NormalisedStreamConfig),
// has off ("has DenyPurge enabled and is configured with it disabled"). When
// the stream is sealed and cfg is not, only Sealed is named: DenyDelete and
// DenyPurge were switched on by the sealing, not by a configuration.
//
// A configuration that has DenyPurge on but also something that makes the
// server switch it off (ClearsDenyPurge) is named as such, since the
// developer did not disable it.
func OneWayStreamDiff(existing jetstream.StreamConfig, cfg types.StreamConfig) []string {
	configuredDenyPurge := cfg.DenyPurge
	cfg = NormalisedStreamConfig(cfg)
	unsealing := existing.Sealed && !cfg.Sealed
	var diffs []string
	for _, setting := range OneWayStreamSettings {
		if !setting.Existing(existing) || setting.Configured(cfg) {
			continue
		}
		if unsealing && (setting.Name == "DenyDelete" || setting.Name == "DenyPurge") {
			continue
		}
		if setting.Name == "DenyPurge" && configuredDenyPurge {
			diffs = append(diffs, "has DenyPurge enabled and is configured with AllowMsgSchedules or SubjectDeleteMarkerTTL, which switches it off")
			continue
		}
		diffs = append(diffs, fmt.Sprintf("has %s enabled and is configured with it disabled", setting.Name))
	}
	return diffs
}

// IsStreamInvalidConfig reports whether err is the server rejecting a stream
// configuration with the generic invalid configuration code (10052).
func IsStreamInvalidConfig(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeStreamInvalidConfig
}

// jetStreamStorage maps a StorageType onto JetStream's, as the stream is
// created: anything other than MemoryStorage is file storage.
func jetStreamStorage(s types.StorageType) jetstream.StorageType {
	if s == types.MemoryStorage {
		return jetstream.MemoryStorage
	}
	return jetstream.FileStorage
}

// storageName names a storage type in an error message.
func storageName(s jetstream.StorageType) string {
	if s == jetstream.MemoryStorage {
		return "memory"
	}
	return "file"
}

// retentionName names a retention policy in an error message.
func retentionName(r jetstream.RetentionPolicy) string {
	switch r {
	case jetstream.WorkQueuePolicy:
		return "work-queue"
	case jetstream.InterestPolicy:
		return "interest"
	default:
		return "limits"
	}
}

// jetStreamRetention maps a RetentionPolicy onto JetStream's.
func jetStreamRetention(r types.RetentionPolicy) jetstream.RetentionPolicy {
	switch r {
	case types.WorkQueuePolicy:
		return jetstream.WorkQueuePolicy
	case types.InterestPolicy:
		return jetstream.InterestPolicy
	default:
		return jetstream.LimitsPolicy
	}
}

// immutableStreamDiff describes how two stream configurations differ in the
// settings NATS cannot change on an existing stream: the storage type, and
// whether the retention policy is work-queue (limits and interest can be
// changed into each other, so they do not count as a difference). It returns
// one phrase per difference, each built with haveVerb and wantVerb ("exists
// with file storage and is configured with memory storage"), and nothing when
// the two agree.
func immutableStreamDiff(haveStorage, wantStorage jetstream.StorageType, haveRetention, wantRetention jetstream.RetentionPolicy, haveVerb, wantVerb string) []string {
	var diffs []string
	if haveStorage != wantStorage {
		diffs = append(diffs, fmt.Sprintf("%s %s storage and %s %s storage", haveVerb, storageName(haveStorage), wantVerb, storageName(wantStorage)))
	}
	if (haveRetention == jetstream.WorkQueuePolicy) != (wantRetention == jetstream.WorkQueuePolicy) {
		diffs = append(diffs, fmt.Sprintf("%s %s retention and %s %s retention",
			haveVerb, retentionName(haveRetention), wantVerb, retentionName(wantRetention)))
	}
	return diffs
}

// explainImmutableStreamChange returns createErr unless it is the server
// rejecting a stream write for a reason the configuration alone does not
// show:
//
//   - the configuration changes a setting NATS cannot change on an existing
//     stream (storage type, retention to or from work-queue), or switches off
//     a setting that can only be switched on (OneWayStreamSettings). The
//     error wraps ErrStreamConfigImmutable and createErr and names owner
//     (what registered the stream), the stream and the settings;
//   - the configuration seals a stream that does not exist yet, which the
//     server only allows for an existing stream. The error wraps
//     ErrInvalidConfiguration and createErr.
//
// The stream is read back only after the write failed with the invalid
// configuration code, so a start that succeeds makes no extra request. Any
// other failed lookup, or a stream that agrees with cfg on these settings
// (the write was rejected for another reason), returns createErr unchanged.
func explainImmutableStreamChange(ctx context.Context, es types.EventStream, cfg types.StreamConfig, owner string, createErr error) error {
	if !IsStreamInvalidConfig(createErr) {
		return createErr
	}
	stream, err := es.Stream(ctx, cfg.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) && cfg.Sealed {
		return fmt.Errorf("%w: stream %s of %s is configured with Sealed but does not exist yet; "+
			"a stream can only be sealed after it was created, so start once without Sealed and set it afterwards: %w",
			monoerrors.ErrInvalidConfiguration, cfg.Name, owner, createErr)
	}
	if err != nil || stream == nil {
		return createErr
	}
	info := stream.CachedInfo()
	if info == nil {
		return createErr
	}
	immutable := immutableStreamDiff(
		info.Config.Storage, jetStreamStorage(cfg.Storage),
		info.Config.Retention, jetStreamRetention(cfg.Retention),
		"exists with", "is configured with")
	oneWay := OneWayStreamDiff(info.Config, cfg)
	if len(immutable) == 0 && len(oneWay) == 0 {
		return createErr
	}
	var help []string
	if len(immutable) > 0 {
		help = append(help, immutableStreamHelp)
	}
	if len(oneWay) > 0 {
		help = append(help, oneWayStreamHelp)
	}
	return fmt.Errorf("%w: stream %s of %s %s; %s: %w",
		monoerrors.ErrStreamConfigImmutable, cfg.Name, owner,
		strings.Join(append(immutable, oneWay...), ", and "), strings.Join(help, "; "), createErr)
}

// eventStreamOwner names an event stream consumer registration in errors.
func eventStreamOwner(entry types.EventStreamConsumerEntry) string {
	module := "<unknown>"
	if entry.Module != nil {
		module = entry.Module.Name()
	}
	return fmt.Sprintf("event stream consumer of module %s for event %s.%s", module, entry.EventDef.ModuleName, entry.EventDef.Name)
}
