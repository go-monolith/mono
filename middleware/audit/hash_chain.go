package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
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

// ComputeEntryHash computes the SHA-256 hash of an audit entry.
// The hash is computed from a deterministic representation of the entry fields.
// The EntryHash field itself is excluded from the computation.
func ComputeEntryHash(entry Entry) string {
	// Create deterministic representation (exclude EntryHash itself)
	detailsJSON, err := json.Marshal(entry.Details)
	if err != nil {
		// If details cannot be marshaled, use empty JSON object
		// Log this unusual condition - Details should always be marshalable
		fmt.Fprintf(os.Stderr, "WARNING: Failed to marshal audit entry details: %v\n", err)
		detailsJSON = []byte("{}")
	}
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

// VerifyChain verifies the integrity of an audit log chain that starts a new
// chain: its first entry must have an empty prev_hash (a log written with
// WithHashChaining("")). For a log that continues an earlier one, use
// VerifyChainFrom with the earlier log's last entry_hash.
//
// Returns an error if:
//   - The chain is broken (prev_hash doesn't match previous entry_hash)
//   - Any entry_hash is invalid (doesn't match computed hash)
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

		// Verify entry_hash is correct
		computed := ComputeEntryHash(entry)
		if entry.EntryHash != computed {
			return fmt.Errorf("entry %d has invalid hash: expected %s, got %s",
				i, computed, entry.EntryHash)
		}

		prevHash = entry.EntryHash
	}

	return nil
}
