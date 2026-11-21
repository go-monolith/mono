package lifecycle

import (
	"context"
	"errors"
	"slices"
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

	source := CronScheduleMessage(ScheduleTestEntry(types.CronServiceConfig{Schedule: "@every 5m", SourceSubject: "prices.latest"}))
	if got := source.Header.Get(types.HeaderNatsScheduleSource); got != "prices.latest" {
		t.Errorf("source header = %q", got)
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
	entry := ScheduleTestEntry(types.CronServiceConfig{Schedule: at, SourceSubject: "prices.latest"})
	recordSubject := types.CronOneShotSubject(entry.Subject)
	record := func(schedule string) *jetstream.RawStreamMsg {
		return &jetstream.RawStreamMsg{Subject: recordSubject, Data: []byte(schedule)}
	}
	same := StoredSchedule(CronScheduleMessage(entry))
	tests := []struct {
		name string
		last map[string]*jetstream.RawStreamMsg
		// The subjects published to, in order.
		want []string
	}{
		{"never published: the schedule, then its record", nil, []string{entry.ScheduleSubject, recordSubject}},
		{"fired and removed, the record names it: nothing", map[string]*jetstream.RawStreamMsg{recordSubject: record(at)}, nil},
		{"the record of another time: published", map[string]*jetstream.RawStreamMsg{recordSubject: record("@at 2025-01-01T00:00:00Z")}, []string{entry.ScheduleSubject, recordSubject}},
		{"a cleared record (deprecated, enabled again): published", map[string]*jetstream.RawStreamMsg{recordSubject: record("")}, []string{entry.ScheduleSubject, recordSubject}},
		{"pending with its record: nothing", map[string]*jetstream.RawStreamMsg{entry.ScheduleSubject: same, recordSubject: record(at)}, nil},
		{"pending without a record (an earlier version published it): the record only", map[string]*jetstream.RawStreamMsg{entry.ScheduleSubject: same}, []string{recordSubject}},
		// An occurrence on the service subject says nothing: with a source
		// subject it is the source's message, and anyone may publish there.
		{"an occurrence alone is no record", map[string]*jetstream.RawStreamMsg{entry.Subject: {Subject: entry.Subject}}, []string{entry.ScheduleSubject, recordSubject}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := &ScheduleTestEventStream{StreamHandle: &ScheduleTestStream{Last: tt.last}}
			published, err := EnsureCronSchedule(ctx, es, "MONO_CRON_orders_cleanup", entry)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, msg := range es.Published {
				got = append(got, msg.Subject)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("published to %v, want %v", got, tt.want)
			}
			if wantPublished := slices.Contains(tt.want, entry.ScheduleSubject); published != wantPublished {
				t.Errorf("published = %v, want %v", published, wantPublished)
			}
			if n := len(es.Published); n > 0 {
				last := es.Published[n-1]
				if string(last.Data) != at || last.Header.Get(jetstream.MsgRollup) != jetstream.MsgRollupSubject {
					t.Errorf("record = %q with rollup %q", last.Data, last.Header.Get(jetstream.MsgRollup))
				}
			}
		})
	}

	// A repeating schedule writes no record.
	repeating := ScheduleTestEntry(types.CronServiceConfig{Schedule: "@every 1h"})
	es := &ScheduleTestEventStream{StreamHandle: &ScheduleTestStream{}}
	if _, err := EnsureCronSchedule(ctx, es, "MONO_CRON_orders_cleanup", repeating); err != nil || len(es.Published) != 1 {
		t.Errorf("repeating schedule: %d messages published, %v; want the schedule alone", len(es.Published), err)
	}
	// A failed lookup of the record publishes nothing.
	lookupErr := errors.New("nats: timeout")
	failing := &OccurrenceFailingStream{Subject: recordSubject, Err: lookupErr}
	es = &ScheduleTestEventStream{StreamHandle: failing}
	if published, err := EnsureCronSchedule(ctx, es, "MONO_CRON_orders_cleanup", entry); published || !errors.Is(err, lookupErr) || len(es.Published) != 0 {
		t.Errorf("failed record lookup: published = %v, %v, %d messages sent", published, err, len(es.Published))
	}
	// Clearing the record writes an empty one.
	es = &ScheduleTestEventStream{StreamHandle: &ScheduleTestStream{}}
	if err := WriteOneShotCronRecord(ctx, es, entry, ""); err != nil || len(es.Published) != 1 || len(es.Published[0].Data) != 0 || es.Published[0].Subject != recordSubject {
		t.Errorf("clearing the record: %v, %+v", err, es.Published)
	}
}

// OccurrenceFailingStream fails the lookup of one subject and answers the
// others as ScheduleTestStream does.
type OccurrenceFailingStream struct {
	ScheduleTestStream
	Subject string
	Err     error
}

// GetLastMsgForSubject fails for Subject.
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
