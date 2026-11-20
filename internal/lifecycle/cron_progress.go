package lifecycle

import (
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// DefaultCronProgressInterval is how often the framework tells the server
// that a cron occurrence is still being handled: a third of the consumer's
// AckWait, so that two signals in a row may be lost before the server gives
// the occurrence to another puller.
const DefaultCronProgressInterval = defaultCronAckWait / 3

// cronProgressEvery returns the interval between the in-progress signals of
// a cron occurrence.
func (lm *lifecycleManager) cronProgressEvery() time.Duration {
	if lm.cronProgressInterval > 0 {
		return lm.cronProgressInterval
	}
	return DefaultCronProgressInterval
}

// KeepCronOccurrence signals the server, every interval and for as long as
// the handler of a cron occurrence runs, that the occurrence is in progress
// (which restarts its AckWait). It returns the function that ends this and
// waits until no further signal can be sent, to be called before the
// occurrence is acknowledged.
//
// Without it a handler that ran longer than the consumer's AckWait (30s) had
// its occurrence delivered a second time while it was still running: to
// another instance at once, so the job ran twice at the same time, and to
// this instance once the handler returned. The cost is that a handler that
// never returns holds its occurrence for as long as the process lives;
// another instance takes it over only when this one is gone, AckWait after
// its last signal.
//
// A failed signal is logged once per occurrence: the occurrence may then be
// redelivered, which the handler has to tolerate anyway (delivery is
// at-least-once).
func KeepCronOccurrence(msg *types.Msg, interval time.Duration, logger types.Logger, fields ...any) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		warned := false
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				// The ticker and done can be ready together.
				select {
				case <-done:
					return
				default:
				}
				if err := msg.InProgress(); err != nil && !warned {
					warned = true
					logger.Warn("Cron in-progress signal failed; the occurrence may be delivered again while its handler runs",
						append(append([]any{}, fields...), "error", err)...)
				}
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
