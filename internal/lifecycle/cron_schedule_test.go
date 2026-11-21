package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/pkg/types"
)

// ScheduleTestStream is a stream that answers GetLastMsgForSubject from a map
// of subjects, or with Err.
type ScheduleTestStream struct {
	jetstream.Stream
	Last map[string]*jetstream.RawStreamMsg
	Err  error
}

func (s *ScheduleTestStream) GetLastMsgForSubject(_ context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	if msg, ok := s.Last[subject]; ok {
		return msg, nil
	}
	return nil, jetstream.ErrMsgNotFound
}

// ScheduleTestEventStream hands out one stream and records what is published.
type ScheduleTestEventStream struct {
	mockEventStream
	StreamHandle jetstream.Stream
	StreamErr    error
	Published    []*types.Msg
}

func (s *ScheduleTestEventStream) Stream(context.Context, string) (jetstream.Stream, error) {
	return s.StreamHandle, s.StreamErr
}

func (s *ScheduleTestEventStream) PublishMsg(_ context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	s.Published = append(s.Published, msg)
	return nil, nil
}

// ScheduleTestEntry returns the entry of a cron service orders.cleanup.
func ScheduleTestEntry(cfg types.CronServiceConfig) *types.ServiceEntry {
	return &types.ServiceEntry{
		Name:            "cleanup",
		ModuleName:      "orders",
		Type:            types.ServiceTypeCron,
		CronConfig:      &cfg,
		Subject:         "services.orders.cleanup",
		ScheduleSubject: "services.orders.cleanup.schedule",
	}
}

// StoredSchedule returns msg as the stream would hand it back, with a header
// the publish path added on the way.
func StoredSchedule(msg *types.Msg) *jetstream.RawStreamMsg {
	hdr := nats.Header{"Mono-Request-Id": []string{"added-on-the-way"}}
	for name, values := range msg.Header {
		hdr[name] = values
	}
	return &jetstream.RawStreamMsg{Subject: msg.Subject, Data: msg.Data, Header: hdr, Sequence: 7}
}

func TestCronScheduleMessage(t *testing.T) {
	entry := ScheduleTestEntry(types.CronServiceConfig{
		Schedule: "0 0 3 * * *", Payload: []byte("p"), TimeZone: "Europe/Berlin", TTL: time.Minute,
	})
	msg := CronScheduleMessage(entry)
	if msg.Subject != entry.ScheduleSubject || string(msg.Data) != "p" {
		t.Fatalf("message = %q on %q", msg.Data, msg.Subject)
	}
	want := map[string]string{
		types.HeaderNatsSchedule:         "0 0 3 * * *",
		types.HeaderNatsScheduleTarget:   entry.Subject,
		types.HeaderNatsScheduleTimeZone: "Europe/Berlin",
		types.HeaderNatsScheduleTTL:      "1m0s",
	}
	for name, value := range want {
		if got := msg.Header.Get(name); got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
	if _, ok := msg.Header[types.HeaderCronOneShot]; ok {
		t.Error("a repeating schedule carries the one-shot header")
	}

	source := CronScheduleMessage(ScheduleTestEntry(types.CronServiceConfig{Schedule: "@every 5m", SourceSubject: "prices.latest"}))
	if got := source.Header.Get(types.HeaderNatsScheduleSource); got != "prices.latest" {
		t.Errorf("source header = %q", got)
	}
	const at = "@at 2026-01-02T15:04:05Z"
	oneShot := CronScheduleMessage(ScheduleTestEntry(types.CronServiceConfig{Schedule: at}))
	if got := oneShot.Header.Get(types.HeaderCronOneShot); got != at {
		t.Errorf("one-shot header = %q, want %q", got, at)
	}
}

func TestSameCronSchedule(t *testing.T) {
	base := types.CronServiceConfig{Schedule: "@every 24h", Payload: []byte("tick")}
	stored := StoredSchedule(CronScheduleMessage(ScheduleTestEntry(base)))
	tests := []struct {
		name string
		cfg  types.CronServiceConfig
		same bool
	}{
		{"unchanged, whatever other headers the stored message has", base, true},
		{"schedule", types.CronServiceConfig{Schedule: "@every 12h", Payload: []byte("tick")}, false},
		{"payload", types.CronServiceConfig{Schedule: "@every 24h", Payload: []byte("tock")}, false},
		{"ttl", types.CronServiceConfig{Schedule: "@every 24h", Payload: []byte("tick"), TTL: time.Minute}, false},
		{"source", types.CronServiceConfig{Schedule: "@every 24h", SourceSubject: "a.b"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SameCronSchedule(stored, CronScheduleMessage(ScheduleTestEntry(tt.cfg))); got != tt.same {
				t.Errorf("SameCronSchedule = %v, want %v", got, tt.same)
			}
		})
	}
	if SameCronSchedule(nil, CronScheduleMessage(ScheduleTestEntry(base))) || SameCronSchedule(stored, nil) {
		t.Error("a missing message is the same schedule as one")
	}
	// A time zone that was removed is a change too.
	withZone := StoredSchedule(CronScheduleMessage(ScheduleTestEntry(types.CronServiceConfig{Schedule: "0 0 3 * * *", TimeZone: "Europe/Berlin"})))
	if SameCronSchedule(withZone, CronScheduleMessage(ScheduleTestEntry(types.CronServiceConfig{Schedule: "0 0 3 * * *"}))) {
		t.Error("a removed time zone was not seen as a change")
	}
}

func TestEnsureCronSchedule(t *testing.T) {
	ctx := context.Background()
	cfg := types.CronServiceConfig{Schedule: "@every 24h", Payload: []byte("tick")}
	entry := ScheduleTestEntry(cfg)
	same := StoredSchedule(CronScheduleMessage(entry))
	other := StoredSchedule(CronScheduleMessage(ScheduleTestEntry(types.CronServiceConfig{Schedule: "@every 1h", Payload: []byte("tick")})))
	lookupErr := errors.New("nats: no response from stream")

	tests := []struct {
		name      string
		stream    *ScheduleTestStream
		streamErr error
		published bool
		wantErr   error
	}{
		{name: "none stored", stream: &ScheduleTestStream{}, published: true},
		{name: "the same schedule stored", stream: &ScheduleTestStream{Last: map[string]*jetstream.RawStreamMsg{entry.ScheduleSubject: same}}},
		{name: "another schedule stored", stream: &ScheduleTestStream{Last: map[string]*jetstream.RawStreamMsg{entry.ScheduleSubject: other}}, published: true},
		{name: "the lookup fails: nothing is published", stream: &ScheduleTestStream{Err: lookupErr}, wantErr: lookupErr},
		{name: "the stream cannot be looked up", stream: &ScheduleTestStream{}, streamErr: lookupErr, wantErr: lookupErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := &ScheduleTestEventStream{StreamHandle: tt.stream, StreamErr: tt.streamErr}
			published, err := EnsureCronSchedule(ctx, es, "MONO_CRON_orders_cleanup", entry)
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if published != tt.published || (len(es.Published) == 1) != tt.published {
				t.Fatalf("published = %v with %d messages sent, want %v", published, len(es.Published), tt.published)
			}
			if tt.published && es.Published[0].Subject != entry.ScheduleSubject {
				t.Errorf("published to %q", es.Published[0].Subject)
			}
		})
	}
}

func TestEnsureCronSchedule_OneShot(t *testing.T) {
	ctx := context.Background()
	const at = "@at 2026-01-02T15:04:05Z"
	entry := ScheduleTestEntry(types.CronServiceConfig{Schedule: at})
	occurrence := func(schedule, next string) *jetstream.RawStreamMsg {
		hdr := nats.Header{}
		if schedule != "" {
			hdr.Set(types.HeaderCronOneShot, schedule)
		}
		if next != "" {
			hdr.Set(types.HeaderNatsScheduleNext, next)
		}
		return &jetstream.RawStreamMsg{Subject: entry.Subject, Header: hdr}
	}
	lookupErr := errors.New("nats: timeout")
	tests := []struct {
		name      string
		last      *jetstream.RawStreamMsg
		published bool
	}{
		{"no occurrence yet", nil, true},
		{"its occurrence is stored", occurrence(at, types.HeaderNatsScheduleNextPurge), false},
		{"the occurrence of another time", occurrence("@at 2025-01-01T00:00:00Z", types.HeaderNatsScheduleNextPurge), true},
		{"an occurrence of a repeating schedule", occurrence("", "2026-01-02T15:04:05Z"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &ScheduleTestStream{Last: map[string]*jetstream.RawStreamMsg{}}
			if tt.last != nil {
				stream.Last[entry.Subject] = tt.last
			}
			es := &ScheduleTestEventStream{StreamHandle: stream}
			published, err := EnsureCronSchedule(ctx, es, "MONO_CRON_orders_cleanup", entry)
			if err != nil || published != tt.published {
				t.Fatalf("published = %v, %v; want %v", published, err, tt.published)
			}
		})
	}

	// A repeating schedule is never held back by an occurrence.
	repeating := ScheduleTestEntry(types.CronServiceConfig{Schedule: "@every 1h"})
	stream := &ScheduleTestStream{Last: map[string]*jetstream.RawStreamMsg{
		repeating.Subject: occurrence("@every 1h", types.HeaderNatsScheduleNextPurge),
	}}
	if fired, err := OneShotCronScheduleFired(ctx, stream, repeating); fired || err != nil {
		t.Errorf("repeating schedule: fired = %v, %v", fired, err)
	}
	// A failed lookup of the occurrence publishes nothing.
	failing := &OccurrenceFailingStream{ScheduleTestStream: ScheduleTestStream{}, Subject: entry.Subject, Err: lookupErr}
	es := &ScheduleTestEventStream{StreamHandle: failing}
	if published, err := EnsureCronSchedule(ctx, es, "MONO_CRON_orders_cleanup", entry); published || !errors.Is(err, lookupErr) || len(es.Published) != 0 {
		t.Errorf("failed occurrence lookup: published = %v, %v, %d messages sent", published, err, len(es.Published))
	}
}

// OccurrenceFailingStream fails the lookup of one subject and answers the
// others as ScheduleTestStream does.
type OccurrenceFailingStream struct {
	ScheduleTestStream
	Subject string
	Err     error
}

func (s *OccurrenceFailingStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if subject == s.Subject {
		return nil, s.Err
	}
	return s.ScheduleTestStream.GetLastMsgForSubject(ctx, subject)
}

func TestIsOneShotCronSchedule(t *testing.T) {
	for schedule, want := range map[string]bool{
		"@at 2026-01-02T15:04:05Z": true,
		"@every 5m":                false,
		"@daily":                   false,
		"0 0 3 * * *":              false,
		"@at":                      false,
	} {
		if got := types.IsOneShotCronSchedule(schedule); got != want {
			t.Errorf("IsOneShotCronSchedule(%q) = %v, want %v", schedule, got, want)
		}
	}
}
