// Package jsprotect keeps JetStream consumers that the server would delete for
// inactivity at start alive until the framework has set them up.
//
// Since nats-server v2.14.10 a pull consumer's inactivity timer starts when
// the consumer becomes leader, which includes the recovery of a server's store
// at start. A consumer created with a Name only (non-durable) is therefore
// deleted about 5s after the server starts unless something pulls it.
// Deleting one replays its stream from the deliver policy when it is
// recreated, and on an interest-retention stream removes the messages only it
// still needed. Earlier framework versions created such consumers (stream
// consumer services up to v0.1.x, cron up to v0.0.12, event stream consumers
// up to v0.0.11), and an application may have its own.
//
// The NATS manager runs Protect right after the embedded server starts, and
// the lifecycle again right after JetStream answers in Start; the framework
// then converts its own consumers to durables.
package jsprotect

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Threshold is the inactivity threshold given to the consumers NeedsProtection
// accepts. It only has to outlast the start: the framework converts its own
// consumers to durables, and any other is removed by the server an hour after
// its last pull instead of five seconds after the server started.
const Threshold = time.Hour

// DefaultInactiveThreshold is the inactivity threshold nats-server gives a
// non-durable consumer that sets none (JsDeleteWaitTimeDefault); the
// consumer's info reports it as the configured threshold.
const DefaultInactiveThreshold = 5 * time.Second

// itemTimeout bounds the read and update of one consumer, so that a consumer
// whose replicas are offline cannot hold up the others.
const itemTimeout = 2 * time.Second

// NeedsProtection reports whether a consumer with this configuration may be
// deleted by the server for inactivity at start: a pull consumer at the
// default threshold (5s) that is either non-durable, or a durable whose
// conversion from a non-durable was interrupted (Durable equal to Name, with
// the 5s threshold the server gave it while it was non-durable). Any other
// threshold is left alone; an explicit 5s cannot be told apart from the
// default.
func NeedsProtection(cfg jetstream.ConsumerConfig) bool {
	if cfg.DeliverSubject != "" {
		return false
	}
	switch cfg.Durable {
	case "":
		return cfg.InactiveThreshold == 0 || cfg.InactiveThreshold == DefaultInactiveThreshold
	case cfg.Name:
		return cfg.InactiveThreshold == DefaultInactiveThreshold
	default:
		return false
	}
}

// Protect runs one pass over every stream of the account and gives each
// consumer NeedsProtection accepts the threshold d, keeping the rest of its
// configuration. Each stream's consumers are listed in one request; a
// consumer that qualifies is read again right before the update, so that a
// consumer the framework converted in the meantime is not written back with
// its old configuration. Each consumer gets one short attempt and a failure
// does not stop the pass. It returns the number of consumers changed and
// whether the pass covered everything (no listing, read or update failed).
func Protect(ctx context.Context, js jetstream.JetStream, d time.Duration) (protected int, complete bool) {
	complete = true
	lister := js.StreamNames(ctx)
	var streams []string
	for name := range lister.Name() {
		streams = append(streams, name)
	}
	if lister.Err() != nil {
		return 0, false
	}
	for _, name := range streams {
		if ctx.Err() != nil {
			return protected, false
		}
		stream, err := js.Stream(ctx, name)
		if err != nil {
			if !errors.Is(err, jetstream.ErrStreamNotFound) {
				complete = false
			}
			continue
		}
		consumers := stream.ListConsumers(ctx)
		var candidates []string
		for info := range consumers.Info() {
			if info != nil && NeedsProtection(info.Config) {
				candidates = append(candidates, info.Name)
			}
		}
		if consumers.Err() != nil {
			complete = false
		}
		for _, consumer := range candidates {
			changed, err := protectOne(ctx, js, name, consumer, d)
			switch {
			case err != nil:
				complete = false
			case changed:
				protected++
			}
		}
	}
	return protected, complete
}

// protectOne reads a consumer and, if it still needs protection, sets its
// threshold to d. A consumer gone since it was listed is not an error.
func protectOne(ctx context.Context, js jetstream.JetStream, stream, name string, d time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, itemTimeout)
	defer cancel()
	consumer, err := js.Consumer(ctx, stream, name)
	if isGone(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cfg := consumer.CachedInfo().Config
	if !NeedsProtection(cfg) {
		return false, nil
	}
	cfg.InactiveThreshold = d
	if _, err := js.UpdateConsumer(ctx, stream, cfg); err != nil {
		if isGone(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isGone(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, jetstream.ErrConsumerDoesNotExist) ||
		errors.Is(err, jetstream.ErrNotPullConsumer)
}
