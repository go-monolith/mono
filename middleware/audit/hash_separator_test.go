package audit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The hash input joins its fields with "|" unescaped, so text can be moved
// between fields without changing the hash. The verifier refuses the entries
// that could be read in two ways.
func TestVerifyChain_RefusesEntriesThatReadTwoWays(t *testing.T) {
	at := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	t.Run("text moved from the service name into the module name", func(t *testing.T) {
		written := NewHashChain("").AddEntry(Entry{Timestamp: at, EventType: EventCustomAuditTrail, ModuleName: "billing", ServiceName: "refund|x"})
		edited := written
		edited.ModuleName, edited.ServiceName = "billing|refund", "x"
		if ComputeEntryHash(edited) != written.EntryHash {
			t.Fatal("the edited entry has another hash: the test proves nothing")
		}
		for name, entry := range map[string]Entry{"as written": written, "edited": edited} {
			if err := VerifyChain([]Entry{entry}); !errors.Is(err, ErrAmbiguousEntry) {
				t.Errorf("%s: VerifyChain = %v, want ErrAmbiguousEntry", name, err)
			}
		}
	})

	t.Run("details taken out of the user context", func(t *testing.T) {
		// Written without details by a user whose context is JSON-like text.
		written := NewHashChain("").AddEntry(Entry{Timestamp: at, EventType: EventCustomAuditTrail, ModuleName: "billing", ServiceName: "refund",
			UserContext: `{"approved_by":"root"}|alice`})
		if err := VerifyChain([]Entry{written}); err != nil {
			t.Fatalf("the entry as written does not verify: %v", err)
		}
		// Edited to say that root approved it, with alice as the user.
		edited := written
		edited.ServiceName = "refund|null"
		edited.Details = map[string]any{"approved_by": "root"}
		edited.UserContext = "alice"
		if ComputeEntryHash(edited) != written.EntryHash {
			t.Fatal("the edited entry has another hash: the test proves nothing")
		}
		err := VerifyChain([]Entry{edited})
		if !errors.Is(err, ErrAmbiguousEntry) || !strings.Contains(err.Error(), "service_name") {
			t.Fatalf("VerifyChain = %v, want ErrAmbiguousEntry naming service_name", err)
		}
	})

	t.Run("the separator in the event type", func(t *testing.T) {
		entry := NewHashChain("").AddEntry(Entry{Timestamp: at, EventType: "custom|x"})
		if err := VerifyChain([]Entry{entry}); !errors.Is(err, ErrAmbiguousEntry) {
			t.Fatalf("VerifyChain = %v, want ErrAmbiguousEntry", err)
		}
	})

	t.Run("the separator in details and user context is fine", func(t *testing.T) {
		chain := NewHashChain("")
		entries := []Entry{
			chain.AddEntry(Entry{Timestamp: at, EventType: EventCustomAuditTrail, ModuleName: "billing", Details: map[string]any{"note": "a|b"}, UserContext: "alice|admin"}),
			chain.AddEntry(Entry{Timestamp: at, EventType: EventCustomAuditTrail, UserContext: "|"}),
		}
		if err := VerifyChain(entries); err != nil {
			t.Fatalf("VerifyChain = %v", err)
		}
	})
}

// With the three name fields free of the separator, no two entries share a
// hash input, whatever their details and user context hold.
func TestHashEntry_OneReadingWithoutSeparatorInNames(t *testing.T) {
	at := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	details := []map[string]any{nil, {"k": "v"}, {"k": "v|w"}, {"k": "null|x"}, {"k": map[string]any{"n": "}|"}}}
	users := []string{"", "u", "null", "null|u", `{"k":"v"}|u`, `{"k":"v|w"}`, "|", "u|", "|u", "{}", "{}|u"}
	seen := map[string]string{}
	for _, eventType := range []EventType{"a", "b"} {
		for _, module := range []string{"", "m", "null"} {
			for _, service := range []string{"", "s", "null", "{}"} {
				for d, det := range details {
					for _, user := range users {
						entry := Entry{Timestamp: at, EventType: eventType, ModuleName: module, ServiceName: service, Details: det, UserContext: user}
						if err := CheckHashable(entry); err != nil {
							t.Fatal(err)
						}
						id := fmt.Sprintf("%s/%s/%s/details %d/%q", eventType, module, service, d, user)
						// The current hash and the one earlier versions
						// gave empty details: the verifier accepts both.
						hashes := []string{ComputeEntryHash(entry)}
						if len(det) == 0 {
							hashes = append(hashes, LegacyEmptyDetailsHash(entry))
						}
						for _, hash := range hashes {
							if other, ok := seen[hash]; ok && other != id {
								t.Fatalf("%s and %s have the same hash", other, id)
							}
							seen[hash] = id
						}
					}
				}
			}
		}
	}
}

func TestCheckHashable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry Entry
		field string
	}{
		{"plain", Entry{EventType: EventModuleStarted, ModuleName: "billing", ServiceName: "refund", UserContext: "a|b", Details: map[string]any{"x": "|"}}, ""},
		{"event type", Entry{EventType: "a|b"}, "event_type"},
		{"module name", Entry{ModuleName: "a|b"}, "module_name"},
		{"service name", Entry{ServiceName: "|"}, "service_name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckHashable(tc.entry)
			if tc.field == "" {
				if err != nil {
					t.Fatalf("CheckHashable = %v", err)
				}
				return
			}
			if !errors.Is(err, ErrAmbiguousEntry) || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("CheckHashable = %v, want ErrAmbiguousEntry naming %s", err, tc.field)
			}
		})
	}
}

// With hash chaining the module does not write an entry the chain could not
// vouch for, and the chain stays where it was; without it the entry is
// written as before.
func TestWriteEntry_RefusesTheSeparatorWithHashChaining(t *testing.T) {
	ctx := context.Background()

	t.Run("with hash chaining", func(t *testing.T) {
		var log bytes.Buffer
		m, err := New(WithOutput(&log), WithHashChaining(""))
		if err != nil {
			t.Fatal(err)
		}
		if err := m.writeEntry(ctx, Entry{EventType: EventCustomAuditTrail, ModuleName: "orders"}); err != nil {
			t.Fatal(err)
		}
		err = m.writeEntry(ctx, Entry{EventType: EventCustomAuditTrail, ModuleName: "orders|billing"})
		if !errors.Is(err, ErrAmbiguousEntry) {
			t.Fatalf("writeEntry = %v, want ErrAmbiguousEntry", err)
		}
		if err := m.writeEntry(ctx, Entry{EventType: EventCustomAuditTrail, ModuleName: "billing"}); err != nil {
			t.Fatal(err)
		}
		entries := ParseLog(t, log.String())
		if len(entries) != 2 {
			t.Fatalf("entries = %d, want 2", len(entries))
		}
		if err := VerifyChain(entries); err != nil {
			t.Fatalf("VerifyChain = %v", err)
		}
	})

	t.Run("without hash chaining", func(t *testing.T) {
		var log bytes.Buffer
		m, err := New(WithOutput(&log))
		if err != nil {
			t.Fatal(err)
		}
		if err := m.writeEntry(ctx, Entry{EventType: EventCustomAuditTrail, ModuleName: "orders|billing"}); err != nil {
			t.Fatalf("writeEntry = %v", err)
		}
		if !strings.Contains(log.String(), `orders|billing`) {
			t.Fatalf("log = %q", log.String())
		}
	})
}
