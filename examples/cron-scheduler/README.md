# Cron Scheduler Example

Demonstrates **cron-scheduled services** — the framework's 5th service type — backed by the
embedded NATS JetStream message scheduler (nats-server v2.14+, ADR-51).

A module registers a single cron service with `RegisterCronService`. The schedule is registered
**server-side**, so in a multi-node deployment exactly one message fires per occurrence (no
client-side ticker, no leader election). Each occurrence is delivered to the handler through a
durable pull consumer with explicit acknowledgement.

```go
func (m *HeartbeatModule) RegisterServices(c mono.ServiceContainer) error {
    return c.RegisterCronService("emit", mono.CronServiceConfig{
        Schedule: "@every 2s",
        Payload:  []byte(`{"event":"heartbeat"}`),
    }, func(ctx context.Context, msg *mono.Msg) error {
        // ... do periodic work ...
        return nil // framework Acks on nil, Naks on error
    })
}
```

## Key points

- **Requires JetStream**: enable it via `WithJetStreamStorageDir(...)`. Registering a cron service
  without JetStream fails fast at startup.
- **Schedule formats**: a cron expression (`"0 0 * * *"`), a named alias (`"@daily"`, `"@hourly"`,
  …), or an interval (`"@every 5m"`, minimum 1s). A `TimeZone` may only be used with cron
  expressions (not `@every`).
- **Acknowledgement is framework-owned**: return `nil` to Ack the occurrence, or a non-nil error to
  Nak it (redelivered up to the consumer's `MaxDeliver`). Do **not** call `msg.Ack()` yourself.
- **Idempotent**: the schedule is (re)published on every startup, so changing the `Schedule`,
  `Payload`, `TimeZone`, or `TTL` and redeploying overwrites the live schedule in place.
- **Retiring a cron service**: set `Deprecated: true` and deploy to purge the schedule (keeping the
  code), then remove the `RegisterCronService` call in a later release.

## Run

```bash
make run-example-4
# or
cd examples/cron-scheduler && go run .
```

You should see a heartbeat printed every 2 seconds, then a clean shutdown on Ctrl+C.
