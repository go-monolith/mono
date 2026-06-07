package types

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// cronNameSanitizer replaces characters that are invalid in a JetStream stream
// name (most importantly the subject separator ".") with underscores.
var cronNameSanitizer = strings.NewReplacer(
	".", "_",
	" ", "_",
	"*", "_",
	">", "_",
	"/", "_",
	"\\", "_",
	"\t", "_",
)

// CronStreamName returns the JetStream stream name backing a cron service. The
// stream is per-service and carries the schedule/control sub-subjects together
// with the target service subject. Module/service segments are sanitized because
// JetStream stream names may not contain dots, spaces, or wildcard characters.
func CronStreamName(moduleName, serviceName string) string {
	return fmt.Sprintf("MONO_CRON_%s_%s",
		cronNameSanitizer.Replace(moduleName),
		cronNameSanitizer.Replace(serviceName))
}

// CronStreamNamePrefix is the prefix shared by every cron stream. It is used to
// detect orphaned cron streams (schedules with no matching registration).
const CronStreamNamePrefix = "MONO_CRON_"

// CronScheduleSubject returns the subject the schedule message is stored on,
// derived from the service subject as "<serviceSubject>.schedule". It is a
// sub-subject of the standard services.<module>.<service> tree (the same way
// stream-consumer services allow sub-topics), so it follows the framework's
// subject convention. The durable consumer filters on the concrete target
// subject only, so the schedule message is never delivered to the handler.
func CronScheduleSubject(serviceSubject string) string {
	return serviceSubject + ".schedule"
}

// CronControlSubject returns the subject used to publish schedule cancellations
// (purges), derived as "<serviceSubject>.control". The server requires the purge
// to be published to a subject different from the schedule subject being purged.
func CronControlSubject(serviceSubject string) string {
	return serviceSubject + ".control"
}

// CronSubjectsWildcard returns the wildcard covering the cron sub-subjects
// (schedule + control) for a service, as "<serviceSubject>.>". The stream
// listens on this plus the concrete target service subject.
func CronSubjectsWildcard(serviceSubject string) string {
	return serviceSubject + ".>"
}

// NormalizeCronSchedule normalizes a schedule pattern to the six-field
// seconds-first cron format understood by the embedded NATS message scheduler
// (nats-server v2.14+, ADR-51).
//
// A standard five-field cron expression ("0 0 * * *") gets "0" prepended as
// the seconds field, preserving its conventional meaning (fire at second
// zero). Named aliases ("@daily"), intervals ("@every 5m"), and expressions
// that already have six fields are returned unchanged apart from whitespace.
// Anything else (for example a malformed field count) is also passed through
// and left to the server to reject, which remains the authoritative
// validator. Leading and trailing whitespace is trimmed on every path, and
// internal whitespace is collapsed to single spaces in cron expressions.
func NormalizeCronSchedule(schedule string) string {
	trimmed := strings.TrimSpace(schedule)
	if strings.HasPrefix(trimmed, "@") {
		return trimmed
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 5 {
		return "0 " + strings.Join(fields, " ")
	}
	// Six-field, malformed, or empty — collapse whitespace the same way as
	// the five-field path and let the server validate the field count.
	return strings.Join(fields, " ")
}

// Cron schedule header names recognised by the embedded NATS server's message
// scheduler (nats-server v2.14+, ADR-51). These are written on the schedule
// message that the framework publishes to the schedule subject; the server
// uses them to drive the recurring republish of the payload to the target
// subject.
const (
	// HeaderNatsSchedule carries the schedule pattern: a six-field
	// seconds-first cron expression ("0 0 0 * * *"), a named alias
	// ("@daily"), or an interval ("@every 5m").
	HeaderNatsSchedule = "Nats-Schedule"

	// HeaderNatsScheduleTarget names the subject the server republishes the
	// payload to on each occurrence. It must differ from the schedule subject.
	HeaderNatsScheduleTarget = "Nats-Schedule-Target"

	// HeaderNatsScheduleSource optionally names a subject whose last message is
	// delivered on each occurrence instead of a static payload.
	HeaderNatsScheduleSource = "Nats-Schedule-Source"

	// HeaderNatsScheduleTimeZone optionally sets the IANA time zone used to
	// evaluate the cron expression (e.g. "America/New_York").
	HeaderNatsScheduleTimeZone = "Nats-Schedule-Time-Zone"

	// HeaderNatsScheduleTTL optionally sets a per-message TTL on each delivered
	// occurrence. Requires the stream to enable AllowMsgTTL.
	HeaderNatsScheduleTTL = "Nats-Schedule-TTL"

	// HeaderNatsScheduleNext, when set to HeaderNatsScheduleNextPurge on a
	// message published to a control subject (alongside HeaderNatsScheduler),
	// cancels the named schedule.
	HeaderNatsScheduleNext = "Nats-Schedule-Next"

	// HeaderNatsScheduler names the schedule subject to purge. It accompanies
	// HeaderNatsScheduleNext and must differ from the publish subject.
	HeaderNatsScheduler = "Nats-Scheduler"

	// HeaderNatsScheduleNextPurge is the only value clients may use with
	// HeaderNatsScheduleNext; it cancels (purges) the schedule.
	HeaderNatsScheduleNextPurge = "purge"
)

// CronHandler processes a single message fired by a server-side cron schedule.
//
// Acknowledgement is owned by the framework, NOT the handler: return nil to Ack
// the occurrence, or return a non-nil error to Nak it (the server redelivers up
// to the consumer's MaxDeliver). Unlike StreamConsumerHandler, a CronHandler
// must not call msg.Ack()/Nak() itself. A panic is recovered and treated as an
// error (Nak).
type CronHandler func(ctx context.Context, msg *Msg) error

// CronServiceConfig configures a cron-scheduled service backed by the embedded
// NATS JetStream message scheduler (nats-server v2.14+).
//
// The schedule is registered server-side, so in a multi-node cluster exactly
// one message fires per occurrence (no client-side ticker, no leader election).
// Each occurrence is delivered through a durable pull consumer with explicit
// acknowledgement (at-least-once).
type CronServiceConfig struct {
	// Schedule is the schedule pattern. Required (even when Deprecated). One of:
	// a cron expression, a named alias ("@daily", "@hourly", ...), or an
	// interval ("@every 5m"). Cron expressions may use either the standard
	// five-field format ("0 0 * * *" = daily at midnight) or the six-field
	// seconds-first format the NATS scheduler natively understands
	// ("0 0 0 * * *"); five-field expressions are normalized by prepending a
	// "0" seconds field (see NormalizeCronSchedule). The pattern is validated
	// by the server.
	Schedule string

	// Payload is the static message body delivered to the handler on each
	// occurrence. Mutually exclusive with SourceSubject.
	Payload []byte

	// SourceSubject, when set, delivers the last message seen on this subject
	// instead of a static payload (useful for downsampling). Mutually exclusive
	// with Payload. The subject must be distinct from the service subject.
	SourceSubject string

	// TimeZone optionally sets the IANA time zone used to evaluate the cron
	// expression. Defaults to UTC when empty.
	TimeZone string

	// TTL optionally sets a per-message TTL on each delivered occurrence. When
	// set it must be at least one second.
	TTL time.Duration

	// Deprecated, when true, cancels (purges) the server-side schedule on
	// startup and does not start the consumer, while keeping the registration
	// code in place. This is phase one of the safe two-phase retirement flow:
	// deploy with Deprecated=true to stop the schedule, then remove the
	// RegisterCronService call in a later release. Set back to false to re-arm.
	Deprecated bool
}
