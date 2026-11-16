package nats

import (
	"context"
	"time"

	"github.com/go-monolith/mono/internal/jsprotect"
	"github.com/nats-io/nats.go/jetstream"
)

// The manager protects the consumers of the store the server has just
// recovered (see package jsprotect) before any application code runs: since
// nats-server v2.14.10 a non-durable consumer is deleted about 5s after the
// start unless it is pulled. In a cluster without quorum yet the pass cannot
// complete; it is repeated in the background until it does, the budget is
// spent, or StopConsumerProtection is called. The lifecycle stops it when
// Start begins and sweeps again itself once JetStream answers, so no pass of
// the manager can run while the framework converts its consumers.

// Timing of the early protection; variables so tests can shorten them.
var (
	protectPassTimeout = 3 * time.Second
	protectRetryEvery  = time.Second
	protectRetryBudget = 2 * time.Minute
)

// startConsumerProtection tries one pass at once and, if it is incomplete,
// keeps trying in the background. m.protectStop stops it; m.protectDone is
// closed when it has ended.
func (m *natsManager) startConsumerProtection(js jetstream.JetStream) {
	stop := make(chan struct{})
	done := make(chan struct{})
	m.protectStop, m.protectDone = stop, done

	// A pass's context ends when stop is closed, so stopping never waits for
	// a pass in flight.
	runCtx, cancelRun := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
		case <-done:
		}
		cancelRun()
	}()
	pass := func() bool {
		ctx, cancel := context.WithTimeout(runCtx, protectPassTimeout)
		defer cancel()
		protected, complete := jsprotect.Protect(ctx, js, jsprotect.Threshold)
		if protected > 0 {
			m.logger.Info("Extended the inactivity threshold of non-durable consumers recovered at start",
				"count", protected, "inactive_threshold", jsprotect.Threshold)
		}
		return complete
	}

	if pass() {
		close(done)
		return
	}
	go func() {
		defer close(done)
		deadline := time.Now().Add(protectRetryBudget)
		for time.Now().Before(deadline) {
			select {
			case <-stop:
				return
			case <-time.After(protectRetryEvery):
			}
			if pass() {
				return
			}
		}
		m.logger.Warn("Could not check every JetStream consumer for an inactivity threshold at start; " +
			"a non-durable consumer not pulled within 5s of the start may have been deleted by the server")
	}()
}

// StopConsumerProtection stops the background consumer protection started
// with the server and waits until it has ended. It is safe to call more than
// once and without JetStream.
func (m *natsManager) StopConsumerProtection() {
	m.mu.Lock()
	stop, done := m.protectStop, m.protectDone
	m.protectStop, m.protectDone = nil, nil
	m.mu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}
