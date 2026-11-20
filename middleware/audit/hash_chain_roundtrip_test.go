package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// ParseLog parses a log of one JSON entry per line, as a verifier reads it.
func ParseLog(t *testing.T, log string) []Entry {
	t.Helper()
	var entries []Entry
	for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
		if line == "" {
			continue
		}
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("failed to parse entry %q: %v", line, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

// WriteLog chains the entries and writes them as the module does.
func WriteLog(t *testing.T, entries ...Entry) string {
	t.Helper()
	chain := NewHashChain("")
	var log bytes.Buffer
	for _, entry := range entries {
		data, err := json.Marshal(chain.AddEntry(entry))
		if err != nil {
			t.Fatalf("failed to marshal entry: %v", err)
		}
		log.Write(data)
		log.WriteByte('\n')
	}
	return log.String()
}

// HashBeforeCanonicalDetails is ComputeEntryHash as it was up to v0.2.16: the
// details hashed as json.Marshal gives them.
func HashBeforeCanonicalDetails(t *testing.T, entry Entry) string {
	t.Helper()
	details, err := json.Marshal(entry.Details)
	if err != nil {
		t.Fatalf("failed to marshal details: %v", err)
	}
	return HashEntry(entry, details)
}

type roundTripDetail struct {
	Zeta  string `json:"zeta"`
	Alpha int    `json:"alpha"`
}

// An entry must verify after it was written and parsed again, whatever its
// details were. Empty details failed before: every clean module.stopped entry.
func TestVerifyChain_WrittenLogRoundTrips(t *testing.T) {
	ts := time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC)
	tests := []struct {
		name    string
		details map[string]any
	}{
		{name: "nil details", details: nil},
		{name: "empty details", details: map[string]any{}},
		{name: "flat details", details: map[string]any{"duration_ms": float64(5), "error": "boom <&>", "ok": true}},
		{name: "integer values", details: map[string]any{"count": 3, "big": int64(9223372036854775807)}},
		{name: "nil value", details: map[string]any{"error": nil}},
		{name: "struct value, fields not in key order", details: map[string]any{"detail": roundTripDetail{Zeta: "z", Alpha: 1}}},
		{name: "slice of structs", details: map[string]any{"items": []roundTripDetail{{Zeta: "a"}, {Alpha: 2}}}},
		{name: "nested empty map", details: map[string]any{"inner": map[string]any{}}},
		{name: "time value", details: map[string]any{"at": ts}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := WriteLog(t,
				Entry{Timestamp: ts, EventType: EventModuleStarted, ModuleName: "orders", Details: tt.details},
				Entry{Timestamp: ts.Add(time.Second), EventType: EventModuleStopped, ModuleName: "orders", Details: tt.details},
			)
			entries := ParseLog(t, log)
			if err := VerifyChain(entries); err != nil {
				t.Fatalf("untampered log does not verify: %v\n%s", err, log)
			}

			// Rollback: the verification of versions up to v0.2.16 accepts
			// what this version writes.
			for i, entry := range entries {
				if got := HashBeforeCanonicalDetails(t, entry); got != entry.EntryHash {
					t.Errorf("entry %d does not verify with the earlier verification: %s, want %s", i, got, entry.EntryHash)
				}
			}

			// Tampering is still detected.
			entries[1].ModuleName = "billing"
			if err := VerifyChain(entries); err == nil {
				t.Error("tampered entry verified")
			}
		})
	}
}

// The log of a module's normal run: started, then stopped without an error.
func TestVerifyChain_ModuleLifecycleLog(t *testing.T) {
	buf := &syncWriter{}
	m, err := New(WithOutput(buf), WithHashChaining(""))
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}
	ctx := context.Background()
	m.OnModuleLifecycle(ctx, types.ModuleLifecycleEvent{Type: types.ModuleStartedEvent, ModuleName: "orders", Duration: 5 * time.Millisecond})
	m.OnModuleLifecycle(ctx, types.ModuleLifecycleEvent{Type: types.ModuleStoppedEvent, ModuleName: "orders"})

	entries := ParseLog(t, buf.String())
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if err := VerifyChain(entries); err != nil {
		t.Errorf("untampered log of a normal run does not verify: %v", err)
	}
}

// Hashes of entries that verified up to v0.2.16 must not change: existing
// logs keep verifying. The values were computed with v0.2.16.
func TestComputeEntryHash_UnchangedForEarlierLogs(t *testing.T) {
	ts := time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC)
	tests := []struct {
		name  string
		entry Entry
		want  string
	}{
		{
			name:  "nil details",
			entry: Entry{Timestamp: ts, EventType: EventModuleStarted, ModuleName: "orders"},
			want:  "58c133f4446b7bc401e56835492b447692b334b3a34bf5feb3730f984b34e0a7",
		},
		{
			name:  "flat details",
			entry: Entry{Timestamp: ts, EventType: EventModuleStarted, ModuleName: "orders", Details: map[string]any{"duration_ms": float64(5)}},
			want:  "bfe3c22ff3b84c5cc984f1a4498693020debe1aff78561820281f623b5ee48ff",
		},
		{
			name: "nested details and every field",
			entry: Entry{Timestamp: ts, EventType: EventModuleStopped, ModuleName: "orders", ServiceName: "svc", UserContext: "u1", PrevHash: "abc",
				Details: map[string]any{"error": "boom <&>", "n": float64(1.5), "ok": true, "nested": map[string]any{"b": []any{"x", float64(2)}, "a": nil}}},
			want: "f134f06f39e857a5a3e2f2f71444fefdef344e0a3e5f668ae1fa25d7fc7240ad",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ComputeEntryHash(tt.entry); got != tt.want {
				t.Errorf("hash changed: %s, want %s", got, tt.want)
			}
		})
	}
}

// A log written up to v0.2.16 holds entries whose empty details were hashed as
// {} and left out when written. They verify now, and tampering with one is
// still detected.
func TestVerifyChain_AcceptsEarlierEmptyDetailsHash(t *testing.T) {
	ts := time.Date(2026, 11, 20, 10, 0, 0, 123456789, time.UTC)
	// The hash v0.2.16 gave this entry with empty, non-nil details.
	const earlierHash = "6cf056a3a88ebbd5472106bcceca608e3f676de20c545e54b2eeeb92c6550475"
	stopped := Entry{Timestamp: ts, EventType: EventModuleStopped, ModuleName: "orders"}
	if got := LegacyEmptyDetailsHash(stopped); got != earlierHash {
		t.Fatalf("legacy hash %s, want %s", got, earlierHash)
	}
	stopped.EntryHash = earlierHash
	next := Entry{Timestamp: ts.Add(time.Second), EventType: EventModuleStarted, ModuleName: "orders", PrevHash: earlierHash}
	next.EntryHash = ComputeEntryHash(next)

	if err := VerifyChain([]Entry{stopped, next}); err != nil {
		t.Fatalf("log written by an earlier version does not verify: %v", err)
	}

	tampered := stopped
	tampered.ModuleName = "billing"
	if err := VerifyChain([]Entry{tampered, next}); err == nil {
		t.Error("tampered entry with the earlier hash verified")
	}

	// The earlier hash is accepted only for an entry without details.
	withDetails := stopped
	withDetails.Details = map[string]any{"error": "x"}
	withDetails.EntryHash = LegacyEmptyDetailsHash(withDetails)
	if err := VerifyChain([]Entry{withDetails}); err == nil {
		t.Error("entry with details verified with the empty-details hash")
	}
}
