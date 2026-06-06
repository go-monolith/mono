package container

import (
	"fmt"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// minScheduleTTL is the smallest per-message TTL the NATS server accepts.
const minScheduleTTL = time.Second

// RegisterCronService registers a cron-scheduled service backed by the embedded
// NATS JetStream message scheduler (nats-server v2.14+).
//
// The framework publishes a schedule message to an internal schedule subject on
// startup; the server then republishes the payload (or the last message on
// SourceSubject) to the service subject on every scheduled occurrence. Each
// occurrence is delivered to the handler through a durable pull consumer with
// explicit acknowledgement. Because the schedule lives server-side, exactly one
// message fires per occurrence across a cluster.
//
// Validation is fail-fast: the handler must be non-nil, Schedule must be set,
// Payload and SourceSubject are mutually exclusive, TimeZone (when set) must be
// a valid IANA location, and TTL (when set) must be at least one second.
func (c *serviceContainer) RegisterCronService(
	name string,
	config types.CronServiceConfig,
	handler types.CronHandler,
) error {
	if handler == nil {
		return fmt.Errorf("handler is required for cron service '%s'", name)
	}
	if config.Schedule == "" {
		return fmt.Errorf("schedule is required for cron service '%s'", name)
	}
	if len(config.Payload) > 0 && config.SourceSubject != "" {
		return fmt.Errorf("cron service '%s': Payload and SourceSubject are mutually exclusive", name)
	}
	if config.TimeZone != "" {
		if _, err := time.LoadLocation(config.TimeZone); err != nil {
			return fmt.Errorf("cron service '%s': invalid TimeZone %q: %w", name, config.TimeZone, err)
		}
	}
	if config.TTL != 0 && config.TTL < minScheduleTTL {
		return fmt.Errorf("cron service '%s': TTL must be at least %s when set", name, minScheduleTTL)
	}

	// The target subject is the standard service subject. Ticks are republished
	// here by the server and consumed by the durable consumer.
	targetSubject, err := c.computeServiceSubject(name)
	if err != nil {
		return err
	}
	if config.SourceSubject == targetSubject {
		return fmt.Errorf("cron service '%s': SourceSubject must differ from the service subject %q", name, targetSubject)
	}

	// The schedule message is stored on an internal, framework-owned subject
	// (distinct from the target) so the durable consumer — which filters on the
	// concrete target subject — never receives the schedule message itself.
	scheduleSubject := types.CronScheduleSubject(c.boundModule.Name(), name)

	entry := &types.ServiceEntry{
		Type:            types.ServiceTypeCron,
		CronConfig:      &config,
		CronHandler:     handler,
		Subject:         targetSubject,
		ScheduleSubject: scheduleSubject,
	}

	return c.registerService(name, entry)
}
