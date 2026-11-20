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
// rejecting a stream update that changes a setting NATS cannot change on an
// existing stream. In that case it returns an error that wraps
// ErrStreamConfigImmutable and createErr and names owner (what registered the
// stream), the stream and both values.
//
// The stream is read back only after the update failed with the invalid
// configuration code, so a start that succeeds makes no extra request. A
// failed lookup, or a stream that agrees with cfg on these settings (the
// update was rejected for another reason), returns createErr unchanged.
func explainImmutableStreamChange(ctx context.Context, es types.EventStream, cfg types.StreamConfig, owner string, createErr error) error {
	var apiErr *jetstream.APIError
	if !errors.As(createErr, &apiErr) || apiErr.ErrorCode != errCodeStreamInvalidConfig {
		return createErr
	}
	stream, err := es.Stream(ctx, cfg.Name)
	if err != nil || stream == nil {
		return createErr
	}
	info := stream.CachedInfo()
	if info == nil {
		return createErr
	}
	diffs := immutableStreamDiff(
		info.Config.Storage, jetStreamStorage(cfg.Storage),
		info.Config.Retention, jetStreamRetention(cfg.Retention),
		"exists with", "is configured with")
	if len(diffs) == 0 {
		return createErr
	}
	return fmt.Errorf("%w: stream %s of %s %s; %s: %w",
		monoerrors.ErrStreamConfigImmutable, cfg.Name, owner, strings.Join(diffs, ", and "), immutableStreamHelp, createErr)
}

// eventStreamOwner names an event stream consumer registration in errors.
func eventStreamOwner(entry types.EventStreamConsumerEntry) string {
	module := "<unknown>"
	if entry.Module != nil {
		module = entry.Module.Name()
	}
	return fmt.Sprintf("event stream consumer of module %s for event %s.%s", module, entry.EventDef.ModuleName, entry.EventDef.Name)
}
