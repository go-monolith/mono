package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// HashChain manages SHA-256 hash chaining for tamper-evident audit logging.
//
// Each audit entry contains:
//   - prev_hash: SHA-256 hash of previous entry
//   - entry_hash: SHA-256 hash of current entry (includes prev_hash)
//
// This creates a chain where any modification to past entries can be detected
// by verifying the chain using VerifyChain().
type HashChain struct {
	mu       sync.Mutex
	lastHash string
}

// NewHashChain creates a new hash chain with an optional initial hash.
//
// The lastSavedHash parameter allows resuming an existing chain:
//   - Empty string: starts a new chain
//   - Non-empty string: continues from this hash (used as prev_hash for first entry)
func NewHashChain(lastSavedHash string) *HashChain {
	return &HashChain{
		lastHash: lastSavedHash,
	}
}

// AddEntry adds an audit entry to the chain and returns the entry with hashes populated.
// This method is thread-safe.
func (h *HashChain) AddEntry(entry Entry) Entry {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Add hash chaining
	entry.PrevHash = h.lastHash
	entry.EntryHash = ComputeEntryHash(entry)
	h.lastHash = entry.EntryHash

	return entry
}

// Next returns entry with its hashes populated as the next entry of the chain,
// without advancing the chain. The caller writes the entry and calls Commit
// once it is in the log: a chain that advanced over an entry that was never
// written makes every later entry fail verification.
//
// Next and Commit are each thread-safe; a caller that writes from several
// goroutines must hold its own lock from Next to Commit.
func (h *HashChain) Next(entry Entry) Entry {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry.PrevHash = h.lastHash
	entry.EntryHash = ComputeEntryHash(entry)
	return entry
}

// Commit advances the chain to entry, which Next returned and the caller has
// written.
func (h *HashChain) Commit(entry Entry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastHash = entry.EntryHash
}

// ComputeEntryHash computes the SHA-256 hash of an audit entry.
// The hash is computed from a deterministic representation of the entry fields.
// The EntryHash field itself is excluded from the computation.
//
// The details are hashed in the form a reader of the log gets them in (see
// CanonicalDetails), so an entry verifies after it was written and parsed
// again, whatever Go values its details held.
func ComputeEntryHash(entry Entry) string {
	detailsJSON, err := CanonicalDetails(entry.Details)
	if err != nil {
		// If details cannot be marshaled, use empty JSON object
		// Log this unusual condition - Details should always be marshalable
		fmt.Fprintf(os.Stderr, "WARNING: Failed to marshal audit entry details: %v\n", err)
		detailsJSON = []byte("{}")
	}
	return HashEntry(entry, detailsJSON)
}

// CanonicalDetails returns the JSON the details of an entry are hashed as:
// what json.Marshal gives for the details once they went through the log,
// that is, were written as JSON and parsed back into a map[string]any.
//
// Hashing the Go values directly gives another hash than the parsed entry for
// details that do not survive that round trip unchanged:
//   - an empty map is left out of the written entry (omitempty) and parsed as
//     nil, so empty and nil details are both hashed as null;
//   - a struct is written in field order and parsed as a map, whose keys are
//     marshaled sorted;
//   - an integer above 2^53 is parsed as the nearest float64.
//
// For details that are already in the parsed form (strings, float64, bool,
// nil, and maps and slices of them) the result is json.Marshal(details), so
// the hash of an entry that verified before is unchanged.
func CanonicalDetails(details map[string]any) ([]byte, error) {
	if len(details) == 0 {
		return []byte("null"), nil
	}
	written, err := json.Marshal(details)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal audit entry details: %w", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(written, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse audit entry details: %w", err)
	}
	if len(parsed) == 0 {
		// Every value was left out when written (a struct of omitted fields
		// is still a key, so this is defensive).
		return []byte("null"), nil
	}
	canonical, err := json.Marshal(parsed)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal audit entry details: %w", err)
	}
	return canonical, nil
}

// ErrAmbiguousEntry is returned for an entry whose event type, module name or
// service name contains the separator of the hash input ("|"): by the audit
// module when such an entry is to be written with hash chaining, and by
// VerifyChainFrom when a log holds one. See CheckHashable.
var ErrAmbiguousEntry = errors.New("audit entry field contains the hash separator '|'")

// HashSeparator separates the fields of the hash input (see HashEntry).
const HashSeparator = "|"

// CheckHashable returns an error wrapping ErrAmbiguousEntry when the event
// type, module name or service name of entry contains HashSeparator.
//
// The hash input joins the fields with the separator and does not escape it,
// so an entry with module "billing|refund" and service "x" has the hash of
// one with module "billing" and service "refund|x": someone who can edit the
// log could move text between fields and the chain would still verify. With
// these three fields free of the separator the input has one reading only:
// the timestamp before them is formatted without one, the details after them
// are JSON, which ends where its braces close, the previous hash at the end
// is fixed by the chain, and the user context is what is left between the
// two. Refusing the separator in the three fields therefore closes the gap
// without changing the hash of any other entry, so logs and verifiers of
// earlier versions keep working. Details and the user context may contain it.
func CheckHashable(entry Entry) error {
	for _, field := range []struct{ name, value string }{
		{"event_type", string(entry.EventType)},
		{"module_name", entry.ModuleName},
		{"service_name", entry.ServiceName},
	} {
		if strings.Contains(field.value, HashSeparator) {
			return fmt.Errorf("%w: %s %q", ErrAmbiguousEntry, field.name, field.value)
		}
	}
	return nil
}

// HashEntry returns the SHA-256 hash of entry with detailsJSON as the
// representation of its details. The EntryHash field is not part of it.
//
// The fields are joined with HashSeparator, unescaped; CheckHashable says
// which entries that is unambiguous for.
func HashEntry(entry Entry, detailsJSON []byte) string {
	data := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		entry.Timestamp.UTC().Format(time.RFC3339Nano),
		entry.EventType,
		entry.ModuleName,
		entry.ServiceName,
		string(detailsJSON),
		entry.UserContext,
		entry.PrevHash)

	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// LegacyEmptyDetailsHash returns the hash versions up to v0.2.16 gave an
// entry whose details were an empty, non-nil map: they hashed it as {} and
// wrote the entry without details, so it was parsed with nil details and
// never verified. Every clean module.stopped entry was one. VerifyChainFrom
// accepts this hash for an entry without details, so that logs written by
// those versions verify.
func LegacyEmptyDetailsHash(entry Entry) string {
	return HashEntry(entry, []byte("{}"))
}

// VerifyChain verifies the integrity of an audit log chain that starts a new
// chain: its first entry must have an empty prev_hash (a log written with
// WithHashChaining("")). For a log that continues an earlier one, use
// VerifyChainFrom with the earlier log's last entry_hash.
//
// Returns an error if:
//   - The chain is broken (prev_hash doesn't match previous entry_hash)
//   - Any entry_hash is invalid (doesn't match computed hash)
//   - The event type, module name or service name of an entry contains "|"
//     (ErrAmbiguousEntry, see CheckHashable)
//
// Example:
//
//	entries, _ := parseAuditLogFile("audit.log")
//	if err := audit.VerifyChain(entries); err != nil {
//	    log.Fatalf("Audit log tampering detected: %v", err)
//	}
func VerifyChain(entries []Entry) error {
	return VerifyChainFrom(entries, "")
}

// VerifyChainFrom verifies the integrity of an audit log chain whose first
// entry continues from startHash: a log written with
// WithHashChaining(startHash), where startHash is the last entry_hash of the
// previous log. VerifyChainFrom(entries, "") is VerifyChain(entries).
//
// It returns the same errors as VerifyChain; the first entry is checked
// against startHash instead of an empty prev_hash.
//
// Example:
//
//	previous, _ := parseAuditLogFile("audit-2026-11-17.log")
//	current, _ := parseAuditLogFile("audit-2026-11-18.log")
//	last := previous[len(previous)-1].EntryHash
//	if err := audit.VerifyChainFrom(current, last); err != nil {
//	    log.Fatalf("Audit log tampering detected: %v", err)
//	}
func VerifyChainFrom(entries []Entry, startHash string) error {
	if len(entries) == 0 {
		return nil
	}

	prevHash := startHash
	for i, entry := range entries {
		// Verify prev_hash matches
		if entry.PrevHash != prevHash {
			return fmt.Errorf("hash chain broken at entry %d: expected prev_hash %s, got %s",
				i, prevHash, entry.PrevHash)
		}

		// An entry whose hash input can be read in two ways proves nothing
		// about its fields, whatever its hash is.
		if err := CheckHashable(entry); err != nil {
			return fmt.Errorf("entry %d cannot be verified: %w", i, err)
		}

		// Verify entry_hash is correct. An entry without details may carry
		// the hash earlier versions gave empty details.
		computed := ComputeEntryHash(entry)
		legacy := len(entry.Details) == 0 && entry.EntryHash == LegacyEmptyDetailsHash(entry)
		if entry.EntryHash != computed && !legacy {
			return fmt.Errorf("entry %d has invalid hash: expected %s, got %s",
				i, computed, entry.EntryHash)
		}

		prevHash = entry.EntryHash
	}

	return nil
}
