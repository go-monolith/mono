package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/internal/eventbus"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

const (
	// errCodeStreamSubjectsOverlap is the JetStream API error code (10065)
	// the server rejects a stream with whose subjects overlap those of
	// another stream.
	errCodeStreamSubjectsOverlap jetstream.ErrorCode = 10065

	// errCodeStreamUpdate is the JetStream API error code (10069) of a
	// stream update that failed for a reason without a code of its own. One
	// of them is a change of the stream's consumer limits that an existing
	// consumer exceeds, so the code alone does not identify it (see
	// ConsumerLimitViolators).
	errCodeStreamUpdate jetstream.ErrorCode = 10069
)

// SubjectsOverlapLookupLimit is the number of streams holding the subjects of
// a rejected stream that are described in the error.
const SubjectsOverlapLookupLimit = 8

// subjectsOverlapHelp is the way out named by an ErrStreamSubjectsOverlap for
// a stream that already exists.
const subjectsOverlapHelp = "a subject belongs to one stream for as long as that stream exists. Keep the stream name; " +
	"or move to new subjects beside the existing stream (a new service name or event version), switch the publishers and retire the existing stream once it is drained; " +
	"or, when its messages and consumer positions are not needed, delete it (nats stream rm <stream>). " +
	"A stream consumer service that becomes a cron service of the same name, or the reverse, asks for the same subjects on another stream: " +
	"use another service name, or delete the existing stream. " +
	"The framework does not hand the subjects over itself: for a moment no stream would listen, and a publish without acknowledgement would be dropped"

// SubjectHolderStore is the part of JetStream that tells which streams listen
// on a subject. It is an optional interface of an EventStream: without it a
// rejected stream keeps the server's error.
type SubjectHolderStore interface {
	StreamNamesBySubject(ctx context.Context, subject string) ([]string, error)
}

// The framework's JetStream wrapper must keep implementing
// SubjectHolderStore; otherwise the explanation would silently switch itself
// off.
var _ SubjectHolderStore = (*eventbus.NatsJetStream)(nil)

// IsStreamSubjectsOverlap reports whether err is the server rejecting a
// stream because its subjects overlap those of another stream (10065).
func IsStreamSubjectsOverlap(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeStreamSubjectsOverlap
}

// IsStreamUpdateRejected reports whether err is the server failing a stream
// update with the generic code (10069).
func IsStreamUpdateRejected(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeStreamUpdate
}

// consumerLimitsViolation is how the server's description of a stream update
// that an existing consumer's limits forbid begins; the names of the
// consumers follow, separated by commas. The server has no error code for
// it (the generic update code is used for other failures too), so the text
// is what identifies it. An integration test checks it against the server.
const consumerLimitsViolation = "change to limits violates consumers: "

// ConsumerLimitViolators returns the consumers the server names when it
// rejects a stream update because they exceed the stream's new consumer
// limits, and nothing when err is not that rejection.
func ConsumerLimitViolators(err error) []string {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != errCodeStreamUpdate {
		return nil
	}
	_, list, found := strings.Cut(apiErr.Description, consumerLimitsViolation)
	if !found {
		return nil
	}
	var names []string
	for _, name := range strings.Split(list, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// HasConsumerLimits reports whether cfg limits the consumers of its stream.
func HasConsumerLimits(cfg types.StreamConfig) bool {
	return cfg.ConsumerLimits.MaxAckPending > 0 || cfg.ConsumerLimits.InactiveThreshold > 0
}

// WriteStream creates or updates the stream of a stream consumer service or
// an event stream consumer, and turns a rejection the configuration alone
// does not explain into an error that does (see ExplainStreamWriteError).
// owner names the registration.
//
// A stream whose consumer limits (StreamConfig.ConsumerLimits) are lowered is
// written a second time: the server checks the new limits against the
// consumers as they exist, and the framework updates a consumer only after
// its stream, so the update was rejected although the configuration of the
// consumer complies. When the server rejects the write for the consumers it
// names and the configuration carries a MaxAckPending limit, those consumers
// are lowered to it (see LowerMaxAckPending) and the write is repeated once.
// A start that needs no repeat makes no extra request.
func (lm *lifecycleManager) WriteStream(ctx context.Context, es types.EventStream, cfg types.StreamConfig, owner string) (jetstream.Stream, error) {
	stream, err := es.CreateOrUpdateStream(ctx, cfg)
	if err == nil {
		return stream, nil
	}
	if violators := ConsumerLimitViolators(err); len(violators) > 0 && cfg.ConsumerLimits.MaxAckPending > 0 {
		lowered, lowerErr := lm.LowerMaxAckPending(ctx, es, cfg, violators)
		if lowerErr != nil {
			return nil, fmt.Errorf("stream %s of the %s was rejected (%v), and its consumers could not be brought under the new limit: %w",
				cfg.Name, owner, err, lowerErr)
		}
		if lowered > 0 {
			if stream, err = es.CreateOrUpdateStream(ctx, cfg); err == nil {
				return stream, nil
			}
		}
	}
	return nil, lm.ExplainStreamWriteError(ctx, es, cfg, owner, err)
}

// ExplainStreamWriteError returns writeErr, the error of a rejected stream
// write, as an error that says what is in the way, for the rejections the
// configuration alone does not explain:
//
//   - 10052, a setting NATS cannot change on an existing stream:
//     ErrStreamConfigImmutable (see explainImmutableStreamChange);
//   - 10065, subjects another stream listens on: ErrStreamSubjectsOverlap
//     (see ExplainSubjectsOverlap);
//   - 10069 for consumers that exceed the configured consumer limits: the
//     limits and what the framework does and does not change (see
//     ExplainConsumerLimits).
//
// Anything else is returned unchanged. Whatever is looked up is looked up
// only here, after the rejection.
func (lm *lifecycleManager) ExplainStreamWriteError(ctx context.Context, es types.EventStream, cfg types.StreamConfig, owner string, writeErr error) error {
	switch {
	case IsStreamInvalidConfig(writeErr):
		return explainImmutableStreamChange(ctx, es, cfg, owner, writeErr)
	case IsStreamSubjectsOverlap(writeErr):
		return lm.streamPlan.ExplainSubjectsOverlap(ctx, es, cfg, owner, writeErr)
	case len(ConsumerLimitViolators(writeErr)) > 0 && HasConsumerLimits(cfg):
		return ExplainConsumerLimits(cfg, owner, writeErr)
	}
	return writeErr
}

// LowerMaxAckPending lowers the MaxAckPending of violators, the existing
// consumers the server named when it rejected cfg's new consumer limits, to
// the stream's limit (StreamConfig.ConsumerLimits.MaxAckPending), and returns
// how many it changed. Nothing else of a consumer is changed.
//
// It changes something only when that alone lets the stream be written, so
// that a Start that fails anyway leaves every consumer as it was (an instance
// of the previous version may still be pulling from it). Nothing is lowered
// when:
//
//   - one of the violators is not the durable of a registration of the
//     stream: another application's consumer, or one left behind by a renamed
//     registration, is not the framework's to change;
//   - one of them exceeds an InactiveThreshold limit. That is never lowered:
//     the server deletes a consumer that is not pulled for that long, with
//     its position, and nothing pulls it until its fetch loop starts.
//
// Lowering MaxAckPending only slows delivery down, and each registration
// sends its own value right after its stream is written, which the server
// then holds to the limit. A violator that is gone by now is skipped.
func (lm *lifecycleManager) LowerMaxAckPending(ctx context.Context, es types.EventStream, cfg types.StreamConfig, violators []string) (int, error) {
	limit := cfg.ConsumerLimits.MaxAckPending
	planned := lm.streamPlan.Stream(cfg.Name)
	if limit <= 0 || planned == nil || len(violators) == 0 {
		return 0, nil
	}
	owners := make(map[string]string, len(planned.Registrations))
	for _, reg := range planned.Registrations {
		owners[reg.Consumer] = reg.Owner
	}
	for _, name := range violators {
		if _, registered := owners[name]; !registered {
			return 0, nil
		}
	}
	stream, err := es.Stream(ctx, cfg.Name)
	if err != nil {
		return 0, fmt.Errorf("failed to look up stream %s: %w", cfg.Name, err)
	}
	if stream == nil {
		return 0, nil
	}
	// Every violator is read before any is changed.
	var lower []jetstream.ConsumerConfig
	for _, name := range violators {
		consumer, err := stream.Consumer(ctx, name)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("failed to look up consumer %s on stream %s: %w", name, cfg.Name, err)
		}
		info := consumer.CachedInfo()
		if info == nil {
			return 0, nil
		}
		if threshold := cfg.ConsumerLimits.InactiveThreshold; threshold > 0 && info.Config.InactiveThreshold > threshold {
			return 0, nil
		}
		if info.Config.MaxAckPending > limit {
			lower = append(lower, info.Config)
		}
	}
	lowered := 0
	for _, existing := range lower {
		name, before := existing.Durable, existing.MaxAckPending
		if name == "" {
			name = existing.Name
		}
		existing.MaxAckPending = limit
		if err := updateExistingConsumer(ctx, stream, existing); err != nil {
			return lowered, fmt.Errorf("failed to lower MaxAckPending of consumer %s on stream %s from %d to %d: %w",
				name, cfg.Name, before, limit, err)
		}
		lowered++
		lm.logger.Info("Consumer MaxAckPending lowered to the new consumer limit of its stream, which the server would otherwise not accept",
			"registration", owners[name],
			"stream", cfg.Name,
			"consumer", name,
			"from", before,
			"to", limit)
	}
	return lowered, nil
}

// updateExistingConsumer sends cfg, the configuration of an existing consumer
// with one setting changed, as an update. A server that recovered the
// consumer from its store answers the first update of a consumer with several
// filters with the configuration from before, although it applied it; the
// consumer is read back to tell (see eventbus.AppliedDespiteStaleAnswer).
func updateExistingConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig) error {
	_, err := stream.UpdateConsumer(ctx, cfg)
	if errors.Is(err, jetstream.ErrConsumerMultipleFilterSubjectsNotSupported) {
		_, err = eventbus.AppliedDespiteStaleAnswer(ctx, cfg, err, stream.Consumer)
	}
	return err
}

// ExplainConsumerLimits returns the error for a stream update the server
// rejected because consumers that exist on the stream exceed the consumer
// limits of cfg. The server names them in writeErr, which is wrapped. The
// error says when the framework brings consumers under a limit by itself and
// what it leaves to the operator.
func ExplainConsumerLimits(cfg types.StreamConfig, owner string, writeErr error) error {
	var limits []string
	if cfg.ConsumerLimits.MaxAckPending > 0 {
		limits = append(limits, fmt.Sprintf("MaxAckPending %d", cfg.ConsumerLimits.MaxAckPending))
	}
	if cfg.ConsumerLimits.InactiveThreshold > 0 {
		limits = append(limits, fmt.Sprintf("InactiveThreshold %s", cfg.ConsumerLimits.InactiveThreshold))
	}
	return fmt.Errorf("stream %s of the %s could not be updated with its ConsumerLimits (%s): consumers that exist on the stream exceed them, "+
		"and the server checks the limits against the consumers as they are (it names them below). "+
		"The framework lowers a MaxAckPending by itself only when that is all it takes, that is when every consumer named is the consumer of one of this application's registrations "+
		"and none exceeds an InactiveThreshold limit; otherwise it changes nothing. "+
		"A consumer of another application, or one left behind by a renamed registration, has to be changed (nats consumer edit %s <consumer>) or removed by hand. "+
		"An InactiveThreshold is never lowered automatically, because the server deletes a consumer that is not pulled for that long, with its position: "+
		"set it on the consumers in one deployment and on the stream's limits in the next: %w",
		cfg.Name, owner, strings.Join(limits, ", "), cfg.Name, writeErr)
}

// CheckStreamsDisjoint returns an error wrapping ErrStreamSubjectsOverlap
// when two streams of the plan would listen on overlapping subjects: two
// modules that consume one event through streams with different names, for
// example. A subject belongs to one stream, so the server rejects the stream
// written second (10065), on every start; the check fails before anything is
// created and names both streams and the registrations.
//
// Cron service streams are not part of the plan. A registration that
// overlaps one, or a stream that exists but is not registered, is reported
// when the server rejects the write (see ExplainSubjectsOverlap).
func (p *StreamPlan) CheckStreamsDisjoint() error {
	names := p.StreamNames()
	for i, name := range names {
		stream := p.Stream(name)
		for _, otherName := range names[:i] {
			other := p.Stream(otherName)
			for _, subject := range stream.Subjects {
				for _, otherSubject := range other.Subjects {
					if !SubjectsCollide(otherSubject, subject) {
						continue
					}
					return fmt.Errorf("%w: stream %s listens on %q for the %s and stream %s on %q for the %s; "+
						"a subject belongs to one stream, so the server rejects the stream that is written second. "+
						"Give both registrations the same stream name, which makes them share the stream, or subjects that do not overlap",
						monoerrors.ErrStreamSubjectsOverlap,
						otherName, otherSubject, other.subjectOwner(otherSubject),
						name, subject, stream.subjectOwner(subject))
				}
			}
		}
	}
	return nil
}

// subjectOwner names the first registration of the stream that listens on
// subject.
func (s *PlannedStream) subjectOwner(subject string) string {
	for _, reg := range s.Registrations {
		for _, own := range reg.Stream.Subjects {
			if own == subject {
				return reg.Owner
			}
		}
	}
	return "registrations of the stream"
}

// ExplainSubjectsOverlap returns writeErr unless it is the server rejecting
// the stream of a registration because another stream already listens on its
// subjects (10065). Then the streams that hold them are looked up, and the
// error returned wraps ErrStreamSubjectsOverlap and writeErr and names owner
// (the registration), the stream, the subjects, each stream in the way with
// its subjects and what created it, and the way out.
//
// This is what renaming a stream with the same subjects amounts to, and
// turning a stream consumer service into a cron service of the same name or
// the reverse: the existing stream keeps the subjects. Nothing is moved or
// deleted.
//
// The lookup is made only after the rejection, so a start that succeeds makes
// no extra request. writeErr is returned unchanged when es cannot tell which
// streams listen on a subject (SubjectHolderStore), when the lookup fails,
// and when no other stream holds the subjects any more.
func (p *StreamPlan) ExplainSubjectsOverlap(ctx context.Context, es types.EventStream, cfg types.StreamConfig, owner string, writeErr error) error {
	store, ok := es.(SubjectHolderStore)
	if !ok || !IsStreamSubjectsOverlap(writeErr) {
		return writeErr
	}
	held := make(map[string][]string) // stream in the way -> the requested subjects it overlaps
	for _, subject := range cfg.Subjects {
		names, err := store.StreamNamesBySubject(ctx, subject)
		if err != nil {
			return writeErr
		}
		for _, name := range names {
			// An update that adds a subject is rejected with the same code;
			// the stream itself is never in its own way.
			if name != cfg.Name {
				held[name] = append(held[name], subject)
			}
		}
	}
	if len(held) == 0 {
		return writeErr
	}
	holders := make([]string, 0, len(held))
	for name := range held {
		holders = append(holders, name)
	}
	sort.Strings(holders)
	described := make([]string, 0, len(holders))
	for i, name := range holders {
		if i == SubjectsOverlapLookupLimit {
			described = append(described, fmt.Sprintf("%d more", len(holders)-i))
			break
		}
		described = append(described, fmt.Sprintf("stream %s (%s) holds %s",
			name, p.streamOrigin(ctx, es, name), strings.Join(held[name], ", ")))
	}
	return fmt.Errorf("%w: stream %s of the %s cannot listen on its subjects, because %s; %s: %w",
		monoerrors.ErrStreamSubjectsOverlap, cfg.Name, owner, strings.Join(described, ", and "), subjectsOverlapHelp, writeErr)
}

// streamOrigin says what an existing stream listens on and what created it,
// as far as the plan, its name and its metadata tell.
func (p *StreamPlan) streamOrigin(ctx context.Context, es types.EventStream, name string) string {
	var parts []string
	var marker string
	if stream, err := es.Stream(ctx, name); err == nil && stream != nil {
		if info := stream.CachedInfo(); info != nil {
			if len(info.Config.Subjects) > 0 {
				parts = append(parts, "listening on "+strings.Join(info.Config.Subjects, ", "))
			}
			marker = info.Config.Metadata[RegistrationMetadataKey]
		}
	}
	switch {
	case p.Stream(name) != nil:
		parts = append(parts, "the stream of this application's "+p.Marker(name, ""))
	case strings.HasPrefix(name, types.CronStreamNamePrefix):
		parts = append(parts, "the stream of a cron service")
	case marker != "":
		parts = append(parts, fmt.Sprintf("created by the framework for the %s; no registration of this application uses this stream", marker))
	default:
		parts = append(parts, "not marked as created by the framework for a registration")
	}
	return strings.Join(parts, "; ")
}
