# Cron Scheduler Example

Demonstrates **cron-scheduled services** — the framework's 5th service type — backed by the
embedded NATS JetStream message scheduler (nats-server v2.14+, ADR-51). A `heartbeat` module
registers one cron service, `emit`, that fires every 2 seconds.

## Run

```bash
make run-example-4
# or
cd examples/cron-scheduler && go run .
```

You should see a heartbeat printed every 2 seconds, then a clean shutdown on Ctrl+C.

## Code Structure

```
examples/cron-scheduler/
├── main.go     # Application with JetStream enabled, signal handling
├── module.go   # HeartbeatModule: registers the "emit" cron service
└── README.md   # This file
```

## Key Patterns

A module registers a cron service with `RegisterCronService`. The schedule is registered
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

- **Requires JetStream**: enable it via `WithJetStreamStorageDir(...)`. Registering a cron service
  without JetStream fails fast at startup.
- **Schedule formats**: a cron expression in the standard five-field format (`"0 0 * * *"` = daily
  at midnight) or the six-field seconds-first format (`"0 0 0 * * *"`; five-field expressions are
  normalized by prepending a `"0"` seconds field), a named alias (`"@daily"`, `"@hourly"`, …), or
  an interval (`"@every 5m"`, minimum 1s). A `TimeZone` may only be used with cron expressions
  (not `@every`).
- **Acknowledgement is framework-owned**: return `nil` to Ack the occurrence, or a non-nil error to
  Nak it (redelivered up to the consumer's `MaxDeliver`). Do **not** call `msg.Ack()` yourself.
- **Restart-safe**: the consumer is durable, so a restart delivers only occurrences not yet
  acknowledged. A consumer missing at startup is recreated after the occurrences already stored,
  so it delivers only new ones, and the number it skipped is logged.
- **Updated in place**: changing the `Schedule`, `Payload`, `TimeZone`, or `TTL` and redeploying
  overwrites the live schedule. An unchanged schedule is left alone at startup, so the interval of
  an `@every` schedule runs on across restarts.
- **Retiring a cron service**: set `Deprecated: true` and deploy to purge the schedule (keeping the
  code), then remove the `RegisterCronService` call in a later release.

## Expected Output

Framework log lines are interleaved and omitted here; times depend on the run:

```
=== Mono-Framework Cron Scheduler Example ===
Demonstrates: server-side cron-scheduled services (JetStream message scheduler)

✓ App created (JetStream enabled)
✓ Module registered
✓ App started — the cron service fires every 2 seconds
Press Ctrl+C to stop.

[20:59:22] heartbeat #1: {"event":"heartbeat"}
[20:59:24] heartbeat #2: {"event":"heartbeat"}
[20:59:26] heartbeat #3: {"event":"heartbeat"}
[20:59:28] heartbeat #4: {"event":"heartbeat"}

Shutting down…
✓ App stopped cleanly
```

## Next Steps

- [k8s-cluster example](../k8s-cluster/README.md): a cron service in a three-server cluster, firing once per occurrence for the whole cluster.
