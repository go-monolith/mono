package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/pkg/types"
)

// CronScheduleHeaders are the headers of a schedule message the framework
// sets. Two schedule messages that agree on them and on their payload are the
// same schedule.
var CronScheduleHeaders = []string{
	types.HeaderNatsSchedule,
	types.HeaderNatsScheduleTarget,
	types.HeaderNatsScheduleTimeZone,
	types.HeaderNatsScheduleTTL,
	types.HeaderNatsScheduleSource,
}

// CronScheduleMessage builds the schedule message of a cron service: the
// message on the service's schedule subject that makes the server publish the
// payload to the service subject on every occurrence.
func CronScheduleMessage(entry *types.ServiceEntry) *types.Msg {
	cfg := entry.CronConfig
	hdr := types.Header{
		types.HeaderNatsSchedule:       []string{cfg.Schedule},
		types.HeaderNatsScheduleTarget: []string{entry.Subject},
	}
	if cfg.TimeZone != "" {
		hdr[types.HeaderNatsScheduleTimeZone] = []string{cfg.TimeZone}
	}
	if cfg.TTL > 0 {
		hdr[types.HeaderNatsScheduleTTL] = []string{cfg.TTL.String()}
	}
	if cfg.SourceSubject != "" {
		hdr[types.HeaderNatsScheduleSource] = []string{cfg.SourceSubject}
	}
	if types.IsOneShotCronSchedule(cfg.Schedule) {
		hdr[types.HeaderCronOneShot] = []string{cfg.Schedule}
	}
	return &types.Msg{
		Subject: entry.ScheduleSubject,
		Data:    cfg.Payload,
		Header:  hdr,
	}
}

// SameCronSchedule reports whether stored, the last message on a schedule
// subject, is the schedule want asks for: the same payload and the same value
// of every header in CronScheduleHeaders. Other headers are not compared, so
// one a publish adds on its way does not make every schedule look changed.
func SameCronSchedule(stored *jetstream.RawStreamMsg, want *types.Msg) bool {
	if stored == nil || want == nil || !bytes.Equal(stored.Data, want.Data) {
		return false
	}
	for _, name := range CronScheduleHeaders {
		if stored.Header.Get(name) != want.Header.Get(name) {
			return false
		}
	}
	return true
}

// EnsureCronSchedule publishes the schedule message of a cron service unless
// the stream already holds the same schedule. It reports whether it published.
//
// The server counts an interval ("@every 24h") from the time of the schedule
// message, and a schedule message replaces the one before it on its subject.
// Published at every Start, as it was, the schedule of an interval began anew
// with each start of any instance: a deploy, another instance of a cluster
// coming up, one instance restarting in a loop. An interval longer than the
// time between two starts never fired, and nothing said so. Cron expressions
// and aliases are computed from the wall clock, so they were not affected.
//
// The last message on the schedule subject is therefore read first (one
// request to the stream's leader per cron service and start):
//
//   - the same schedule: nothing is published, and the interval runs on;
//   - another schedule (a changed Schedule, Payload, TimeZone, TTL or
//     SourceSubject): published, which replaces it, and an interval starts
//     from now;
//   - none (a first start, a schedule an earlier Deprecated registration or an
//     operator purged): published;
//   - none, for a one-shot schedule ("@at <time>") whose occurrence the stream
//     holds (see OneShotCronScheduleFired): nothing is published. The server
//     removes a one-shot schedule when it has fired, and one whose time has
//     passed fires at once, so published at every Start the job ran once per
//     start of every instance.
//
// Any other error of the lookup is returned and nothing is published: a
// publish "to be safe" while the stream has no leader would restart the
// interval in exactly the situations the lookup is for. The cron setup step is
// retried while the error clears by itself.
//
// Instances of a version that publishes at every Start still restart the
// interval, until every instance is upgraded.
func EnsureCronSchedule(ctx context.Context, es types.EventStream, streamName string, entry *types.ServiceEntry) (bool, error) {
	want := CronScheduleMessage(entry)
	stream, err := es.Stream(ctx, streamName)
	if err != nil {
		return false, fmt.Errorf("look up stream %q for its schedule: %w", streamName, err)
	}
	stored, err := stream.GetLastMsgForSubject(ctx, entry.ScheduleSubject)
	switch {
	case err == nil:
		if SameCronSchedule(stored, want) {
			return false, nil
		}
	case errors.Is(err, jetstream.ErrMsgNotFound):
		fired, err := OneShotCronScheduleFired(ctx, stream, entry)
		if err != nil || fired {
			return false, err
		}
	default:
		return false, fmt.Errorf("read the schedule stored on %q: %w", entry.ScheduleSubject, err)
	}
	if _, err := es.PublishMsg(ctx, want); err != nil {
		return false, err
	}
	return true, nil
}

// OneShotCronScheduleFired reports whether the one-shot schedule of a cron
// service has fired: the last message on the service subject is an occurrence
// of a one-shot schedule (the server marks it to purge its schedule) and
// carries this schedule in HeaderCronOneShot. It is false for a schedule that
// repeats, and for another time than the one that fired, so changing the time
// of the registration schedules the job once more.
//
// The occurrence is found for as long as the stream keeps it: a one-shot
// service whose occurrences expire (CronServiceConfig.TTL) fires again at the
// first Start after that.
func OneShotCronScheduleFired(ctx context.Context, stream jetstream.Stream, entry *types.ServiceEntry) (bool, error) {
	schedule := entry.CronConfig.Schedule
	if !types.IsOneShotCronSchedule(schedule) {
		return false, nil
	}
	last, err := stream.GetLastMsgForSubject(ctx, entry.Subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read the last occurrence on %q: %w", entry.Subject, err)
	}
	return last.Header.Get(types.HeaderNatsScheduleNext) == types.HeaderNatsScheduleNextPurge &&
		last.Header.Get(types.HeaderCronOneShot) == schedule, nil
}
