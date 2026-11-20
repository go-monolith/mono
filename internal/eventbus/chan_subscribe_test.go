package eventbus

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/pkg/types"
)

// goroutinesSettle returns the number of goroutines once it stops changing.
func goroutinesSettle() int {
	n := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		now := runtime.NumGoroutine()
		if now == n && i > 5 {
			return n
		}
		n = now
	}
	return n
}

// TestChanSubscribe_ForwarderEndsWithTheSubscription: every ChanSubscribe
// used to leave its forwarding goroutine behind for the life of the process.
func TestChanSubscribe_ForwarderEndsWithTheSubscription(t *testing.T) {
	conn := setupTestNATS(t)
	eb, err := NewEventBus(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatal(err)
	}
	before := goroutinesSettle()
	const cycles = 200
	for i := 0; i < cycles; i++ {
		ch := make(chan *types.Msg, 4)
		sub, err := eb.ChanSubscribe(fmt.Sprintf("leak.%d", i), ch)
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			err = sub.Unsubscribe()
		} else {
			err = sub.Drain()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		after := goroutinesSettle()
		if after <= before+5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines before, %d after %d subscribe and unsubscribe cycles", before, after, cycles)
		}
	}
}

// TestChanSubscribe_DeliversWhatWasReceived: the messages delivered before a
// Drain are handed over, and the caller's channel is not closed.
func TestChanSubscribe_DeliversWhatWasReceived(t *testing.T) {
	conn := setupTestNATS(t)
	eb, err := NewEventBus(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan *types.Msg, 16)
	sub, err := eb.ChanSubscribe("orders.created", ch)
	if err != nil {
		t.Fatal(err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		if err := eb.Publish("orders.created", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Drain(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		select {
		case msg, open := <-ch:
			if !open {
				t.Fatal("the caller's channel was closed")
			}
			if len(msg.Data) != 1 || msg.Data[0] != byte(i) {
				t.Fatalf("message %d = %v", i, msg.Data)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("message %d was not handed over", i)
		}
	}
	select {
	case msg, open := <-ch:
		t.Fatalf("unexpected receive after the drain: %v (open=%v)", msg, open)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestForwardChanSubscription_GivesUpOnACallerThatStoppedReading: once the
// subscription has ended, the forwarder does not wait for good on a channel
// nobody reads.
func TestForwardChanSubscription_GivesUpOnACallerThatStoppedReading(t *testing.T) {
	in := make(chan *nats.Msg, 4)
	out := make(chan *types.Msg) // nobody reads
	ended := make(chan struct{})
	for i := 0; i < 3; i++ {
		in <- &nats.Msg{Subject: "x"}
	}
	done := make(chan struct{})
	go func() {
		ForwardChanSubscription(in, out, ended)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond) // blocked handing over the first message
	close(ended)
	select {
	case <-done:
	case <-time.After(chanForwardGrace + 3*time.Second):
		t.Fatal("the forwarder did not end although the subscription had")
	}
}
