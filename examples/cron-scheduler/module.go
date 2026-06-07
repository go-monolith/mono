package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-monolith/mono"
)

// HeartbeatModule registers a single cron-scheduled service that emits a
// heartbeat on a fixed interval. The schedule is registered server-side, so in
// a multi-node deployment exactly one heartbeat fires per occurrence — no
// client-side ticker and no leader election.
type HeartbeatModule struct {
	ticks int64
}

// NewHeartbeatModule creates the heartbeat module.
func NewHeartbeatModule() *HeartbeatModule {
	return &HeartbeatModule{}
}

// Name identifies the module.
func (m *HeartbeatModule) Name() string { return "heartbeat" }

// Start is a no-op; the cron schedule is provisioned by the framework.
func (m *HeartbeatModule) Start(_ context.Context) error { return nil }

// Stop is a no-op; the framework drains the cron consumer on shutdown.
func (m *HeartbeatModule) Stop(_ context.Context) error { return nil }

// RegisterServices registers the cron service. The handler is invoked on each
// scheduled occurrence with the static payload. Acknowledgement is owned by the
// framework: returning nil Acks the occurrence, a non-nil error Naks it.
func (m *HeartbeatModule) RegisterServices(c mono.ServiceContainer) error {
	return c.RegisterCronService("emit", mono.CronServiceConfig{
		Schedule: "@every 2s",
		Payload:  []byte(`{"event":"heartbeat"}`),
	}, m.onTick)
}

func (m *HeartbeatModule) onTick(_ context.Context, msg *mono.Msg) error {
	n := atomic.AddInt64(&m.ticks, 1)
	fmt.Printf("[%s] heartbeat #%d: %s\n", time.Now().Format("15:04:05"), n, msg.Data)
	return nil
}
