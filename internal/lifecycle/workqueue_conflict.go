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

// The JetStream API error codes the server rejects a consumer on a work-queue
// stream with: a second consumer without a filter (10099), and a consumer
// whose filter overlaps that of another one (10100). Both mean the same: a
// work-queue stream delivers each message to one consumer, so no two
// consumers may listen on the same subjects.
const (
	errCodeWorkQueueMultipleUnfiltered jetstream.ErrorCode = 10099
	errCodeWorkQueueNotUnique          jetstream.ErrorCode = 10100
)

// WorkQueueConflictLookupLimit is the number of consumers of a stream that
// are inspected to name the ones in the way of a rejected consumer. A stream
// can hold any number of consumers; the ones beyond the limit are not named.
const WorkQueueConflictLookupLimit = 256

// workQueueRegistrationHelp is the way out when two registrations of the
// application collide.
const workQueueRegistrationHelp = "a work-queue stream delivers each message to one consumer, so two consumers cannot listen on the same subjects; " +
	"give the registrations subjects that do not overlap, or use a stream with interest retention, under which every consumer gets every message " +
	"(the retention of an existing stream cannot be changed to or from work-queue, so that takes a new stream)"

// workQueueExistingHelp is the way out when a consumer that already exists is
// in the way. It is a format that takes the stream name.
const workQueueExistingHelp = "a work-queue stream delivers each message to one consumer, so two consumers cannot listen on the same subjects. " +
	"If the existing consumer belongs to a module or service that was renamed or removed, delete it (nats consumer rm %s <consumer>): " +
	"a work-queue stream is not purged when a consumer is deleted, so the messages it did not acknowledge stay and are delivered to the new consumer. " +
	"It is not deleted automatically: another running version of the application, or another application, may still use it"

// IsWorkQueueConsumerConflict reports whether err is the server rejecting a
// consumer because another consumer of its work-queue stream already covers
// its subjects (10099, 10100).
func IsWorkQueueConsumerConflict(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == errCodeWorkQueueMultipleUnfiltered || apiErr.ErrorCode == errCodeWorkQueueNotUnique
}

// FiltersCollide reports whether two consumers with the subject filters a and
// b can be handed the same message. A consumer without a filter gets every
// message of its stream, so it collides with any other consumer.
func FiltersCollide(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, x := range a {
		for _, y := range b {
			if SubjectsCollide(x, y) {
				return true
			}
		}
	}
	return false
}

// filtersText describes the subject filters of a consumer in an error.
func filtersText(filters []string) string {
	if len(filters) == 0 {
		return "not filtered"
	}
	return "filtered on " + strings.Join(filters, ", ")
}

// CheckWorkQueueConsumers returns an error wrapping
// ErrWorkQueueConsumerConflict when two registrations of the plan put a
// consumer on the same subjects of a work-queue stream: two modules that
// consume one event, a module that registers the same event twice, or
// services whose subjects or filters overlap. The server rejects the second
// consumer (10099 or 10100) on every start, on a new deployment too; the
// check fails before anything is created and names both registrations.
//
// The filter of each consumer is the one it is going to be sent with (see
// EffectiveFilters): registrations of a work-queue stream that listen on
// different subjects, or whose callers filter them on different subjects, do
// not collide.
func (p *StreamPlan) CheckWorkQueueConsumers() error {
	for _, name := range p.StreamNames() {
		planned := p.Stream(name)
		if planned.Retention != types.WorkQueuePolicy {
			continue
		}
		for i, reg := range planned.Registrations {
			filters := p.EffectiveFilters(reg)
			for _, other := range planned.Registrations[:i] {
				otherFilters := p.EffectiveFilters(other)
				if !FiltersCollide(otherFilters, filters) {
					continue
				}
				return fmt.Errorf("%w: work-queue stream %s is registered by the %s (consumer %s, %s) and by the %s (consumer %s, %s); %s",
					monoerrors.ErrWorkQueueConsumerConflict, name,
					other.Owner, other.Consumer, filtersText(otherFilters),
					reg.Owner, reg.Consumer, filtersText(filters),
					workQueueRegistrationHelp)
			}
		}
	}
	return nil
}

// ExplainWorkQueueConflict returns updateErr unless it is the server
// rejecting the consumer of a registration because another consumer of its
// work-queue stream already covers its subjects (10099, 10100). Then the
// consumers of the stream are listed, and the error returned wraps
// ErrWorkQueueConsumerConflict and updateErr and names owner (the
// registration), the consumer, the consumers in the way with their filter and
// what created them, and the way out.
//
// The usual cause is a module or service that was renamed: the name of a
// durable consumer is derived from them, so the rename asks for a new durable
// while the old one still exists. Nothing is deleted: the framework cannot
// tell a consumer left behind from one that another version of the
// application, still running during a rolling deploy, relies on.
//
// The consumers are listed only after the rejection, so a start that succeeds
// makes no extra request. updateErr is returned unchanged when stream is nil,
// when the consumers cannot be listed, and when none of them (among the first
// WorkQueueConflictLookupLimit) is in the way.
func (p *StreamPlan) ExplainWorkQueueConflict(ctx context.Context, stream jetstream.Stream, name, consumer string, filters []string, owner string, updateErr error) error {
	if stream == nil || !IsWorkQueueConsumerConflict(updateErr) {
		return updateErr
	}
	planned := p.Stream(name)
	// The listing sends each consumer on an unbuffered channel, so it is
	// always read to its end: past the limit it is cancelled and what it
	// still sends is dropped, which lets its goroutine finish.
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var inTheWay []string
	inspected := 0
	for info := range stream.ListConsumers(listCtx).Info() {
		if info == nil || inspected >= WorkQueueConflictLookupLimit {
			continue
		}
		if inspected++; inspected == WorkQueueConflictLookupLimit {
			cancel()
		}
		if info.Name == consumer || !FiltersCollide(consumerFilters(info.Config), filters) {
			continue
		}
		inTheWay = append(inTheWay, fmt.Sprintf("%s (%s, %s)",
			info.Name, filtersText(consumerFilters(info.Config)), consumerOrigin(planned, info)))
	}
	if len(inTheWay) == 0 {
		// Nothing to name: the listing failed or was cut short, or the
		// consumer in the way is gone again.
		return updateErr
	}
	return fmt.Errorf("%w: consumer %s (%s) of the %s cannot be created or updated on work-queue stream %s, because of existing consumer %s; %s: %w",
		monoerrors.ErrWorkQueueConsumerConflict, consumer, filtersText(filters), owner, name,
		strings.Join(inTheWay, " and "), fmt.Sprintf(workQueueExistingHelp, name), updateErr)
}

// consumerOrigin says what created an existing consumer, as far as its
// metadata and the plan tell: a registration of this application, a
// registration the application no longer has, or something else.
func consumerOrigin(planned *PlannedStream, info *jetstream.ConsumerInfo) string {
	marker := info.Config.Metadata[RegistrationMetadataKey]
	if planned != nil {
		if _, used := planned.Consumers[info.Name]; used {
			for _, reg := range planned.Registrations {
				if reg.Consumer == info.Name {
					return "the consumer of this application's " + reg.Owner
				}
			}
		}
	}
	if marker != "" {
		return fmt.Sprintf("created by the framework for the %s, which this application does not register", marker)
	}
	return "not marked as created by the framework for a registration"
}
