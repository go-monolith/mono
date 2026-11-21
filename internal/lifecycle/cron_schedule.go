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
// The last message on the schedule subject is therefore read first (a stream
// lookup and a message lookup per cron service and start, answered by the
// stream's leader):
//
//   - the same schedule: nothing is published, and the interval runs on;
//   - another schedule (a changed Schedule, Payload, TimeZone, TTL or
//     SourceSubject): published, which replaces it, and an interval starts
//     from now;
//   - none (a first start, a schedule an earlier Deprecated registration or an
//     operator purged): published.
//
// A one-shot schedule ("@at <time>") needs more: the server removes it when
// it has fired, and one whose time has passed fires at once, so "none" is also
// what a one-shot schedule that has done its work looks like, and published at
// every Start the job ran once per start of every instance. The framework
// therefore records each one-shot schedule it publishes on the service's
// one-shot subject (types.CronOneShotSubject), and a Start that finds no
// schedule but finds the record of this one publishes nothing. Another time is
// another schedule and fires once more. A one-shot schedule that is stored
// without a record (published by a version before the record existed) gets its
// record, so that it is not published a second time after it fired.
//
// The record is written after the schedule: a process that dies between the
// two publishes the schedule again at its next Start, which runs the job twice
// if it had fired meanwhile. The other order would lose the job. Two instances
// that start at the same moment can both publish a one-shot schedule whose
// time has passed, for the same reason.
//
// Any error of a lookup is returned and nothing is published: a publish "to
// be safe" while the stream has no leader would restart the interval in
// exactly the situations the lookup is for. The cron setup step is retried
// while the error clears by itself.
//
// Instances of a version that publishes at every Start still restart the
// interval, until every instance is upgraded.
func EnsureCronSchedule(ctx context.Context, es types.EventStream, streamName string, entry *types.ServiceEntry) (bool, error) {
	want := CronScheduleMessage(entry)
	oneShot := types.IsOneShotCronSchedule(entry.CronConfig.Schedule)
	stream, err := es.Stream(ctx, streamName)
	if err != nil {
		return false, fmt.Errorf("look up stream %q for its schedule: %w", streamName, err)
	}
	stored, err := stream.GetLastMsgForSubject(ctx, entry.ScheduleSubject)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, fmt.Errorf("read the schedule stored on %q: %w", entry.ScheduleSubject, err)
	}
	found := err == nil
	if found && SameCronSchedule(stored, want) {
		if oneShot {
			return false, RecordOneShotCronSchedule(ctx, es, stream, entry)
		}
		return false, nil
	}
	if !found && oneShot {
		recorded, err := OneShotCronScheduleRecorded(ctx, stream, entry)
		if err != nil || recorded {
			return false, err
		}
	}
	if _, err := es.PublishMsg(ctx, want); err != nil {
		return false, fmt.Errorf("publish the schedule to %q: %w", entry.ScheduleSubject, err)
	}
	if oneShot {
		return true, RecordOneShotCronSchedule(ctx, es, stream, entry)
	}
	return true, nil
}

// OneShotCronScheduleRecorded reports whether the one-shot record of a cron
// service names its current schedule: the framework has published this
// schedule before.
func OneShotCronScheduleRecorded(ctx context.Context, stream jetstream.Stream, entry *types.ServiceEntry) (bool, error) {
	subject := types.CronOneShotSubject(entry.Subject)
	record, err := stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read the one-shot record on %q: %w", subject, err)
	}
	return string(record.Data) == entry.CronConfig.Schedule, nil
}

// RecordOneShotCronSchedule writes the one-shot record of a cron service,
// unless it already names the current schedule. The record replaces the one
// before it (rollup by subject), so the subject holds one message.
func RecordOneShotCronSchedule(ctx context.Context, es types.EventStream, stream jetstream.Stream, entry *types.ServiceEntry) error {
	recorded, err := OneShotCronScheduleRecorded(ctx, stream, entry)
	if err != nil || recorded {
		return err
	}
	return WriteOneShotCronRecord(ctx, es, entry, entry.CronConfig.Schedule)
}

// WriteOneShotCronRecord writes schedule as the one-shot record of a cron
// service. An empty schedule clears the record: a Deprecated registration
// purges its schedule, and the schedule must be published again when the
// registration is enabled once more.
func WriteOneShotCronRecord(ctx context.Context, es types.EventStream, entry *types.ServiceEntry, schedule string) error {
	subject := types.CronOneShotSubject(entry.Subject)
	_, err := es.PublishMsg(ctx, &types.Msg{
		Subject: subject,
		Data:    []byte(schedule),
		Header:  types.Header{jetstream.MsgRollup: []string{jetstream.MsgRollupSubject}},
	})
	if err != nil {
		return fmt.Errorf("write the one-shot record on %q: %w", subject, err)
	}
	return nil
}
