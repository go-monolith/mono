package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// RegistrationMetadataKey is the metadata entry the framework writes on the
// streams and durable consumers it creates for stream consumer services and
// event stream consumers. Its value names the registrations they were created
// for. A later start recognises by it what the framework created, so that a
// stream or consumer whose registration was renamed or removed can be
// reported (see ReportLeftovers). Streams and consumers created by the
// application itself, or by a version before the entry existed, do not carry
// it.
const RegistrationMetadataKey = "mono.registration"

// StreamRegistration is one registration that puts a durable consumer on a
// stream: a stream consumer service or an event stream consumer.
type StreamRegistration struct {
	// Owner names the registration in errors, logs and metadata.
	Owner string

	// Stream is the stream configuration of the registration, with its own
	// subjects.
	Stream types.StreamConfig

	// Consumer is the name of the registration's durable consumer.
	Consumer string

	// CallerFilter reports whether the registration's consumer configuration
	// carries a subject filter of the caller's, which the framework keeps.
	CallerFilter bool

	// Event reports whether the registration is an event stream consumer
	// rather than a stream consumer service.
	Event bool
}

// PlannedStream is what the registrations of one stream name add up to.
type PlannedStream struct {
	// Registrations lists the registrations of the stream, in setup order.
	Registrations []StreamRegistration

	// Subjects is the union of the registrations' subjects, without the
	// subjects another one covers.
	Subjects []string

	// Shared reports whether the registrations listen on different
	// subjects, so that each consumer needs a filter on its own.
	Shared bool

	// Consumers holds the durable consumer names of the registrations.
	Consumers map[string]struct{}

	// Retention is the retention policy the stream ends up with: that of
	// the registration set up last, whose write is the last one.
	Retention types.RetentionPolicy

	// HasEvents reports whether an event stream consumer is among the
	// registrations. Such a stream may carry the legacy migration marker,
	// which every write of the stream has to keep.
	HasEvents bool

	// existingFiltered records that FilterExistingConsumers has run for the
	// stream in this start.
	existingFiltered bool
}

// StreamPlan holds, per stream name, the registrations of the application. It
// is built once, before the first stream is written, because every
// registration writes the configuration of its stream: without the plan each
// write would replace the subjects of the registrations written before it.
//
// A nil plan is valid and plans nothing: every registration keeps its own
// subjects and gets no filter.
type StreamPlan struct {
	streams map[string]*PlannedStream
}

// StreamConsumerServiceOwner names a stream consumer service registration.
func StreamConsumerServiceOwner(entry *types.ServiceEntry) string {
	return fmt.Sprintf("stream consumer service %s.%s", entry.ModuleName, entry.Name)
}

// StreamConsumerServiceDurable returns the name of the durable consumer of a
// stream consumer service.
func StreamConsumerServiceDurable(entry *types.ServiceEntry) string {
	return types.SanitizeConsumerName(entry.ModuleName + "-" + entry.Name)
}

// StreamRegistrations returns the registrations of the stream consumer
// services, in the order given, followed by those of the event stream
// consumers.
func StreamRegistrations(services []*types.ServiceEntry, events []types.EventStreamConsumerEntry) []StreamRegistration {
	regs := make([]StreamRegistration, 0, len(services)+len(events))
	for _, entry := range services {
		if entry == nil || entry.Type != types.ServiceTypeStreamConsumer || entry.StreamConsumerConfig == nil {
			continue
		}
		regs = append(regs, StreamRegistration{
			Owner:        StreamConsumerServiceOwner(entry),
			Stream:       entry.StreamConsumerConfig.Stream,
			Consumer:     StreamConsumerServiceDurable(entry),
			CallerFilter: hasSubjectFilter(entry.StreamConsumerConfig.Consumer),
		})
	}
	names := eventStreamConsumerNames(events)
	for i, entry := range events {
		regs = append(regs, StreamRegistration{
			Owner:        eventStreamOwner(entry),
			Stream:       entry.Config.Stream,
			Consumer:     names[i],
			CallerFilter: hasSubjectFilter(entry.Config.Consumer),
			Event:        true,
		})
	}
	return regs
}

// NewStreamPlan groups the registrations by stream name. It returns an error
// before anything is written when the registrations of one stream cannot be
// combined:
//
//   - wrapping ErrStreamConfigImmutable when two of them disagree on a setting
//     NATS cannot change on an existing stream (see CheckStreamRegistrations);
//   - wrapping ErrInvalidConfiguration when two of their subjects overlap
//     without one covering the other (see CheckStreamSubjects).
func NewStreamPlan(regs []StreamRegistration) (*StreamPlan, error) {
	if err := CheckStreamRegistrations(regs); err != nil {
		return nil, err
	}
	if err := CheckStreamSubjects(regs); err != nil {
		return nil, err
	}
	plan := &StreamPlan{streams: make(map[string]*PlannedStream)}
	for _, reg := range regs {
		stream := plan.streams[reg.Stream.Name]
		if stream == nil {
			stream = &PlannedStream{Consumers: make(map[string]struct{})}
			plan.streams[reg.Stream.Name] = stream
		}
		stream.Registrations = append(stream.Registrations, reg)
		stream.Consumers[reg.Consumer] = struct{}{}
		stream.Retention = reg.Stream.Retention
		stream.HasEvents = stream.HasEvents || reg.Event
	}
	for _, stream := range plan.streams {
		var all []string
		first := sortedSubjects(stream.Registrations[0].Stream.Subjects)
		for _, reg := range stream.Registrations {
			all = append(all, reg.Stream.Subjects...)
			if !slices.Equal(first, sortedSubjects(reg.Stream.Subjects)) {
				stream.Shared = true
			}
		}
		stream.Subjects = MinimiseSubjects(all)
	}
	return plan, nil
}

// hasSubjectFilter reports whether a consumer configuration carries a subject
// filter.
func hasSubjectFilter(cfg types.ConsumerConfig) bool {
	return cfg.FilterSubject != "" || len(cfg.FilterSubjects) > 0
}

// sortedSubjects returns the distinct subjects in sorted order, to compare
// two subject lists as sets.
func sortedSubjects(subjects []string) []string {
	sorted := slices.Clone(subjects)
	sort.Strings(sorted)
	return slices.Compact(sorted)
}

// Stream returns what is planned for the named stream, or nil when no
// registration of the application uses it.
func (p *StreamPlan) Stream(name string) *PlannedStream {
	if p == nil {
		return nil
	}
	return p.streams[name]
}

// StreamNames returns the names of the planned streams in sorted order.
func (p *StreamPlan) StreamNames() []string {
	if p == nil {
		return nil
	}
	names := slices.Collect(maps.Keys(p.streams))
	sort.Strings(names)
	return names
}

// Subjects returns the subjects the named stream must be written with: the
// union of the subjects of all its registrations, so that one registration's
// write does not drop the subjects of another. A stream the plan does not
// know keeps own, the subjects of the registration being set up.
func (p *StreamPlan) Subjects(stream string, own []string) []string {
	planned := p.Stream(stream)
	if planned == nil {
		return own
	}
	return slices.Clone(planned.Subjects)
}

// Marker returns the value of RegistrationMetadataKey for the named stream:
// the owners of all its registrations, so that every write of a shared stream
// sends the same value. A stream the plan does not know gets own.
func (p *StreamPlan) Marker(stream, own string) string {
	planned := p.Stream(stream)
	if planned == nil {
		return own
	}
	owners := make([]string, 0, len(planned.Registrations))
	for _, reg := range planned.Registrations {
		owners = append(owners, reg.Owner)
	}
	sort.Strings(owners)
	return strings.Join(slices.Compact(owners), "; ")
}

// FilterConsumer returns cfg with a filter on own, the subjects of the
// registration, when its stream is shared with registrations that listen on
// other subjects: the stream then stores their messages too, and without a
// filter the consumer would hand them to the wrong handler.
//
// cfg is returned unchanged when the registrations of the stream all listen
// on the same subjects (the consumer is then sent exactly as before, and a
// filter set while the stream was shared is removed by the update), and when
// the caller set a filter of its own.
func (p *StreamPlan) FilterConsumer(stream string, own []string, cfg types.ConsumerConfig) types.ConsumerConfig {
	planned := p.Stream(stream)
	if planned == nil || !planned.Shared || hasSubjectFilter(cfg) {
		return cfg
	}
	cfg.FilterSubject, cfg.FilterSubjects = splitFilters(MinimiseSubjects(own))
	return cfg
}

// splitFilters returns the filters as a consumer configuration takes them: a
// single one as FilterSubject, several as FilterSubjects. The server rejects
// a configuration that sets both.
func splitFilters(filters []string) (string, []string) {
	if len(filters) == 1 {
		return filters[0], nil
	}
	return "", filters
}

// FilterExistingConsumers gives the durable consumers that already exist on a
// shared stream the filter of their registration, changing nothing else of
// them. It runs once per start and stream, when the first registration of the
// stream is set up and before that registration's own consumer is created:
//
//   - a work-queue stream accepts a new filtered consumer only when the
//     consumers it already has are filtered on other subjects, whatever order
//     the registrations are set up in;
//   - on the other streams it shortens the time in which an instance that is
//     already running pulls another registration's messages through a
//     consumer that is still unfiltered.
//
// A stream that is not shared, a registration whose caller set a filter and
// a consumer that does not exist yet are left alone. stream may be nil (an
// EventStream that returns no handle), which skips the step.
func (p *StreamPlan) FilterExistingConsumers(ctx context.Context, name string, stream jetstream.Stream) error {
	planned := p.Stream(name)
	if planned == nil || !planned.Shared || planned.existingFiltered || stream == nil {
		return nil
	}
	for _, reg := range planned.Registrations {
		if reg.CallerFilter {
			continue
		}
		consumer, err := stream.Consumer(ctx, reg.Consumer)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to look up consumer %s on shared stream %s: %w", reg.Consumer, name, err)
		}
		info := consumer.CachedInfo()
		if info == nil {
			continue
		}
		cfg := info.Config
		want := MinimiseSubjects(reg.Stream.Subjects)
		if slices.Equal(sortedSubjects(consumerFilters(cfg)), sortedSubjects(want)) {
			continue
		}
		cfg.FilterSubject, cfg.FilterSubjects = splitFilters(want)
		_, err = stream.UpdateConsumer(ctx, cfg)
		if errors.Is(err, jetstream.ErrConsumerMultipleFilterSubjectsNotSupported) {
			// The client reports this when the answer to the update does not
			// carry the filters. A server that recovered the consumer from
			// its store answers the first update that adds several filters
			// with the configuration from before, although it applied them;
			// read the consumer back to tell.
			err = confirmFilters(ctx, stream, reg.Consumer, want, err)
		}
		if err != nil {
			return fmt.Errorf("failed to filter consumer %s on shared stream %s: %w", reg.Consumer, name, err)
		}
	}
	planned.existingFiltered = true
	return nil
}

// consumerFilters returns the subject filters of a consumer configuration.
func consumerFilters(cfg jetstream.ConsumerConfig) []string {
	filters := slices.Clone(cfg.FilterSubjects)
	if cfg.FilterSubject != "" {
		filters = append(filters, cfg.FilterSubject)
	}
	return filters
}

// confirmFilters reads the consumer back and returns nil when it is filtered
// on want, and updateErr, the error of the update that should have set the
// filters, when it is not or cannot be read.
func confirmFilters(ctx context.Context, stream jetstream.Stream, consumer string, want []string, updateErr error) error {
	current, err := stream.Consumer(ctx, consumer)
	if err != nil {
		return fmt.Errorf("%w (reading the consumer back failed: %w)", updateErr, err)
	}
	info := current.CachedInfo()
	if info == nil || !slices.Equal(sortedSubjects(consumerFilters(info.Config)), sortedSubjects(want)) {
		return updateErr
	}
	return nil
}

// WithRegistrationMarker returns a copy of metadata with
// RegistrationMetadataKey set to value. The map passed in, which belongs to
// the caller's configuration, is not modified.
func WithRegistrationMarker(metadata map[string]string, value string) map[string]string {
	marked := make(map[string]string, len(metadata)+1)
	maps.Copy(marked, metadata)
	marked[RegistrationMetadataKey] = value
	return marked
}

// SubjectCovers reports whether every subject matched by sub is also matched
// by pattern, following the NATS wildcards: "*" stands for exactly one token
// and ">" for one or more tokens at the end.
func SubjectCovers(pattern, sub string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(sub, ".")
	for i, token := range p {
		if i >= len(s) {
			return false
		}
		switch token {
		case ">":
			// Covers whatever follows, including a wildcard of sub.
			return true
		case "*":
			// One token: ">" in sub would also match several.
			if s[i] == ">" {
				return false
			}
		default:
			if s[i] != token {
				return false
			}
		}
	}
	return len(p) == len(s)
}

// SubjectsCollide reports whether some subject is matched by both a and b,
// following the NATS wildcards. Two subjects of which one covers the other
// collide too.
func SubjectsCollide(a, b string) bool {
	x := strings.Split(a, ".")
	y := strings.Split(b, ".")
	for i := 0; i < len(x) && i < len(y); i++ {
		if x[i] == ">" || y[i] == ">" {
			// The rest of the other subject has at least one token, which
			// ">" matches whatever it is.
			return true
		}
		if x[i] != "*" && y[i] != "*" && x[i] != y[i] {
			return false
		}
	}
	return len(x) == len(y)
}

// CheckStreamSubjects returns an error wrapping ErrInvalidConfiguration when
// two subjects registered on one stream overlap without one covering the
// other ("a.*.c" and "a.b.*"), unless a third subject of the stream covers
// them both. A stream cannot listen on both, and they cannot be reduced to
// one subject as covered subjects are (see MinimiseSubjects), so the server
// would reject the stream; the check names the registrations instead.
func CheckStreamSubjects(regs []StreamRegistration) error {
	type stream struct {
		subjects []string
		owner    map[string]string // subject -> the first registration that has it
	}
	streams := make(map[string]*stream)
	var names []string
	for _, reg := range regs {
		st := streams[reg.Stream.Name]
		if st == nil {
			st = &stream{owner: make(map[string]string)}
			streams[reg.Stream.Name] = st
			names = append(names, reg.Stream.Name)
		}
		for _, subject := range reg.Stream.Subjects {
			if _, seen := st.owner[subject]; !seen {
				st.owner[subject] = reg.Owner
			}
			st.subjects = append(st.subjects, subject)
		}
	}
	for _, name := range names {
		st := streams[name]
		// Only the subjects the stream is written with can be rejected: a
		// pair that another subject covers is dropped from the union.
		written := MinimiseSubjects(st.subjects)
		for i, subject := range written {
			for _, other := range written[:i] {
				if !SubjectsCollide(other, subject) {
					continue
				}
				return fmt.Errorf("%w: stream %s cannot listen on subject %q of the %s and subject %q of the %s: "+
					"they overlap without one covering the other; use subjects that do not overlap, or separate streams",
					monoerrors.ErrInvalidConfiguration, name, other, st.owner[other], subject, st.owner[subject])
			}
		}
	}
	return nil
}

// MinimiseSubjects returns subjects without duplicates and without the
// subjects another one covers, in their original order. A stream rejects
// subjects that overlap each other, so the union of several registrations'
// subjects ("tasks.>" and "tasks.urgent") has to be reduced first. Subjects
// that overlap without one covering the other ("a.*.c" and "a.b.*") are both
// kept; CheckStreamSubjects rejects them.
func MinimiseSubjects(subjects []string) []string {
	kept := make([]string, 0, len(subjects))
	for i, subject := range subjects {
		covered := false
		for j, other := range subjects {
			if i == j {
				continue
			}
			// Of two equal subjects the first one is kept.
			if other == subject {
				if j < i {
					covered = true
					break
				}
				continue
			}
			if SubjectCovers(other, subject) {
				covered = true
				break
			}
		}
		if !covered {
			kept = append(kept, subject)
		}
	}
	return kept
}

// CheckStreamRegistrations returns an error wrapping ErrStreamConfigImmutable
// when two registrations name the same stream with different storage types,
// or with retention policies of which exactly one is work-queue. Every
// registration writes the stream configuration, so the second one would be
// rejected by the server; checking the registrations first fails before
// anything is created, on a new deployment too, and names both of them. Other
// settings may differ: they can be updated, and the registration set up last
// wins.
func CheckStreamRegistrations(regs []StreamRegistration) error {
	first := make(map[string]int, len(regs))
	for i, reg := range regs {
		j, seen := first[reg.Stream.Name]
		if !seen {
			first[reg.Stream.Name] = i
			continue
		}
		other := regs[j].Stream
		diffs := immutableStreamDiff(
			jetStreamStorage(other.Storage), jetStreamStorage(reg.Stream.Storage),
			jetStreamRetention(other.Retention), jetStreamRetention(reg.Stream.Retention),
			"the first with", "the second with")
		if len(diffs) == 0 {
			continue
		}
		return fmt.Errorf("%w: stream %s is registered by the %s and by the %s, %s; "+
			"NATS cannot change the storage type of an existing stream, or its retention to or from work-queue, "+
			"so every registration of a stream must agree on them",
			monoerrors.ErrStreamConfigImmutable, reg.Stream.Name, regs[j].Owner, reg.Owner, strings.Join(diffs, ", and "))
	}
	return nil
}
