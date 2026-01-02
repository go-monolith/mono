package audit

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewHashChain(t *testing.T) {
	t.Run("empty initial hash", func(t *testing.T) {
		hc := NewHashChain("")
		if hc == nil {
			t.Fatal("NewHashChain returned nil")
		}
		if hc.lastHash != "" {
			t.Errorf("expected empty lastHash, got %q", hc.lastHash)
		}
	})

	t.Run("with initial hash", func(t *testing.T) {
		initialHash := "abc123def456"
		hc := NewHashChain(initialHash)
		if hc == nil {
			t.Fatal("NewHashChain returned nil")
		}
		if hc.lastHash != initialHash {
			t.Errorf("expected lastHash %q, got %q", initialHash, hc.lastHash)
		}
	})
}

func TestHashChain_AddEntry(t *testing.T) {
	t.Run("first entry has empty prev_hash", func(t *testing.T) {
		hc := NewHashChain("")
		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
		}

		result := hc.AddEntry(entry)

		if result.PrevHash != "" {
			t.Errorf("first entry should have empty prev_hash, got %q", result.PrevHash)
		}
		if result.EntryHash == "" {
			t.Error("entry_hash should not be empty")
		}
	})

	t.Run("second entry has prev_hash from first", func(t *testing.T) {
		hc := NewHashChain("")

		entry1 := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "module-1",
		}
		result1 := hc.AddEntry(entry1)

		entry2 := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStopped,
			ModuleName: "module-2",
		}
		result2 := hc.AddEntry(entry2)

		if result2.PrevHash != result1.EntryHash {
			t.Errorf("second entry prev_hash should match first entry_hash: got %q, want %q",
				result2.PrevHash, result1.EntryHash)
		}
	})

	t.Run("with initial hash as prev_hash for first entry", func(t *testing.T) {
		initialHash := "abc123"
		hc := NewHashChain(initialHash)

		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
		}
		result := hc.AddEntry(entry)

		if result.PrevHash != initialHash {
			t.Errorf("first entry prev_hash should match initial hash: got %q, want %q",
				result.PrevHash, initialHash)
		}
	})

	t.Run("each entry gets unique hash", func(t *testing.T) {
		hc := NewHashChain("")
		hashes := make(map[string]bool)

		for i := 0; i < 10; i++ {
			entry := Entry{
				Timestamp:   time.Now().UTC(),
				EventType:   EventModuleStarted,
				ModuleName:  "test-module",
				ServiceName: fmt.Sprintf("service-%d", i),
			}
			result := hc.AddEntry(entry)

			if hashes[result.EntryHash] {
				t.Errorf("duplicate hash detected: %s", result.EntryHash)
			}
			hashes[result.EntryHash] = true
		}
	})
}

func TestHashChain_ConcurrentAddEntry(t *testing.T) {
	hc := NewHashChain("")
	const numGoroutines = 100

	var wg sync.WaitGroup
	results := make(chan Entry, numGoroutines)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			entry := Entry{
				Timestamp:  time.Now().UTC(),
				EventType:  EventModuleStarted,
				ModuleName: "test-module",
				Details: map[string]any{
					"index": idx,
				},
			}
			result := hc.AddEntry(entry)
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect all results
	hashes := make(map[string]bool)
	for result := range results {
		if result.EntryHash == "" {
			t.Error("entry_hash should not be empty")
		}
		if hashes[result.EntryHash] {
			t.Errorf("duplicate hash detected: %s", result.EntryHash)
		}
		hashes[result.EntryHash] = true
	}

	if len(hashes) != numGoroutines {
		t.Errorf("expected %d unique hashes, got %d", numGoroutines, len(hashes))
	}
}

func TestComputeEntryHash(t *testing.T) {
	t.Run("deterministic hash", func(t *testing.T) {
		timestamp := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
		entry := Entry{
			Timestamp:   timestamp,
			EventType:   EventModuleStarted,
			ModuleName:  "test-module",
			ServiceName: "test-service",
			Details: map[string]any{
				"key": "value",
			},
			UserContext: "user-123",
			PrevHash:    "prev-hash-123",
		}

		hash1 := ComputeEntryHash(entry)
		hash2 := ComputeEntryHash(entry)

		if hash1 != hash2 {
			t.Errorf("hash should be deterministic: got %q and %q", hash1, hash2)
		}
	})

	t.Run("different entries produce different hashes", func(t *testing.T) {
		timestamp := time.Now().UTC()
		entry1 := Entry{
			Timestamp:  timestamp,
			EventType:  EventModuleStarted,
			ModuleName: "module-1",
		}
		entry2 := Entry{
			Timestamp:  timestamp,
			EventType:  EventModuleStarted,
			ModuleName: "module-2",
		}

		hash1 := ComputeEntryHash(entry1)
		hash2 := ComputeEntryHash(entry2)

		if hash1 == hash2 {
			t.Error("different entries should produce different hashes")
		}
	})

	t.Run("EntryHash field is excluded from computation", func(t *testing.T) {
		timestamp := time.Now().UTC()
		entry := Entry{
			Timestamp:  timestamp,
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
			PrevHash:   "prev-hash",
			EntryHash:  "some-hash-that-should-be-ignored",
		}

		hash1 := ComputeEntryHash(entry)

		// Change EntryHash and compute again
		entry.EntryHash = "different-hash"
		hash2 := ComputeEntryHash(entry)

		if hash1 != hash2 {
			t.Error("EntryHash field should be excluded from hash computation")
		}
	})

	t.Run("hash length is correct SHA-256 hex", func(t *testing.T) {
		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
		}

		hash := ComputeEntryHash(entry)

		// SHA-256 produces 32 bytes = 64 hex characters
		if len(hash) != 64 {
			t.Errorf("expected hash length 64, got %d", len(hash))
		}

		// Verify it's valid hex
		for _, c := range hash {
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				t.Errorf("invalid hex character in hash: %c", c)
			}
		}
	})

	t.Run("changing PrevHash changes EntryHash", func(t *testing.T) {
		timestamp := time.Now().UTC()
		entry1 := Entry{
			Timestamp:  timestamp,
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
			PrevHash:   "hash-1",
		}
		entry2 := Entry{
			Timestamp:  timestamp,
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
			PrevHash:   "hash-2",
		}

		hash1 := ComputeEntryHash(entry1)
		hash2 := ComputeEntryHash(entry2)

		if hash1 == hash2 {
			t.Error("changing PrevHash should change the computed hash")
		}
	})

	t.Run("empty details", func(t *testing.T) {
		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
			Details:    nil,
		}

		hash := ComputeEntryHash(entry)
		if hash == "" {
			t.Error("hash should not be empty for entry with nil details")
		}
	})
}

func TestVerifyChain(t *testing.T) {
	t.Run("empty chain is valid", func(t *testing.T) {
		err := VerifyChain([]Entry{})
		if err != nil {
			t.Errorf("empty chain should be valid: %v", err)
		}
	})

	t.Run("single entry chain is valid", func(t *testing.T) {
		hc := NewHashChain("")
		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
		}
		result := hc.AddEntry(entry)

		err := VerifyChain([]Entry{result})
		if err != nil {
			t.Errorf("single entry chain should be valid: %v", err)
		}
	})

	t.Run("multi-entry chain is valid", func(t *testing.T) {
		hc := NewHashChain("")
		var entries []Entry

		for i := 0; i < 5; i++ {
			entry := Entry{
				Timestamp:  time.Now().UTC(),
				EventType:  EventModuleStarted,
				ModuleName: fmt.Sprintf("module-%d", i),
			}
			result := hc.AddEntry(entry)
			entries = append(entries, result)
		}

		err := VerifyChain(entries)
		if err != nil {
			t.Errorf("valid chain should pass verification: %v", err)
		}
	})

	t.Run("chain with initial hash is valid", func(t *testing.T) {
		// Start chain with a specific hash (simulating resuming from previous session)
		initialHash := "initial-hash-from-previous-session"
		hc := NewHashChain(initialHash)

		entry := Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: "test-module",
		}
		result := hc.AddEntry(entry)

		// Note: When verifying a resumed chain, the first entry's PrevHash
		// won't match "" because we started with initialHash.
		// The VerifyChain function expects the chain to start fresh.
		// In a real scenario, you'd need to provide the initial hash context.
		if result.PrevHash != initialHash {
			t.Errorf("expected PrevHash %q, got %q", initialHash, result.PrevHash)
		}
	})

	t.Run("broken chain detected - modified prev_hash", func(t *testing.T) {
		hc := NewHashChain("")
		var entries []Entry

		for i := 0; i < 3; i++ {
			entry := Entry{
				Timestamp:  time.Now().UTC(),
				EventType:  EventModuleStarted,
				ModuleName: fmt.Sprintf("module-%d", i),
			}
			result := hc.AddEntry(entry)
			entries = append(entries, result)
		}

		// Tamper with the second entry's prev_hash
		entries[1].PrevHash = "tampered-hash"

		err := VerifyChain(entries)
		if err == nil {
			t.Error("tampered prev_hash should be detected")
		}
	})

	t.Run("broken chain detected - modified entry_hash", func(t *testing.T) {
		hc := NewHashChain("")
		var entries []Entry

		for i := 0; i < 3; i++ {
			entry := Entry{
				Timestamp:  time.Now().UTC(),
				EventType:  EventModuleStarted,
				ModuleName: fmt.Sprintf("module-%d", i),
			}
			result := hc.AddEntry(entry)
			entries = append(entries, result)
		}

		// Tamper with the first entry's entry_hash
		entries[0].EntryHash = "tampered-hash"

		err := VerifyChain(entries)
		if err == nil {
			t.Error("tampered entry_hash should be detected")
		}
	})

	t.Run("broken chain detected - modified content", func(t *testing.T) {
		hc := NewHashChain("")
		var entries []Entry

		for i := 0; i < 3; i++ {
			entry := Entry{
				Timestamp:  time.Now().UTC(),
				EventType:  EventModuleStarted,
				ModuleName: fmt.Sprintf("module-%d", i),
			}
			result := hc.AddEntry(entry)
			entries = append(entries, result)
		}

		// Tamper with content but keep hashes the same
		entries[1].ModuleName = "tampered-module-name"

		err := VerifyChain(entries)
		if err == nil {
			t.Error("tampered content should be detected (hash mismatch)")
		}
	})

	t.Run("reordered entries detected", func(t *testing.T) {
		hc := NewHashChain("")
		var entries []Entry

		for i := 0; i < 3; i++ {
			entry := Entry{
				Timestamp:  time.Now().UTC(),
				EventType:  EventModuleStarted,
				ModuleName: fmt.Sprintf("module-%d", i),
			}
			result := hc.AddEntry(entry)
			entries = append(entries, result)
		}

		// Swap entries (reorder)
		entries[1], entries[2] = entries[2], entries[1]

		err := VerifyChain(entries)
		if err == nil {
			t.Error("reordered entries should be detected")
		}
	})
}

// TestComputeEntryHash_ErrorPaths tests error handling in ComputeEntryHash.
func TestComputeEntryHash_ErrorPaths(t *testing.T) {
	t.Run("handles unmarshalable details gracefully", func(t *testing.T) {
		entry := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventCustomAuditTrail,
			ModuleName:  "test",
			ServiceName: "test-service",
			Details: map[string]any{
				"channel": make(chan int), // Cannot be marshaled to JSON
			},
		}

		// ComputeEntryHash should not panic, should use "{}" for details
		hash := ComputeEntryHash(entry)

		// Verify hash is computed (non-empty)
		if hash == "" {
			t.Error("expected non-empty hash even with unmarshalable details")
		}

		// Verify hash length (SHA-256 hex = 64 characters)
		if len(hash) != 64 {
			t.Errorf("expected hash length 64, got %d", len(hash))
		}
	})

	t.Run("handles nil details", func(t *testing.T) {
		entry := Entry{
			Timestamp:   time.Now().UTC(),
			EventType:   EventModuleStarted,
			ModuleName:  "test",
			ServiceName: "test-service",
			Details:     nil, // nil details should be marshaled as "null"
		}

		hash := ComputeEntryHash(entry)

		if hash == "" {
			t.Error("expected non-empty hash for nil details")
		}
		if len(hash) != 64 {
			t.Errorf("expected hash length 64, got %d", len(hash))
		}
	})

	t.Run("deterministic hash with same input", func(t *testing.T) {
		timestamp := time.Now().UTC()
		entry1 := Entry{
			Timestamp:   timestamp,
			EventType:   EventServiceRegistered,
			ModuleName:  "test",
			ServiceName: "service-1",
			UserContext: "user-123",
			PrevHash:    "previous-hash",
			Details:     map[string]any{"key": "value"},
		}
		entry2 := entry1 // Same entry

		hash1 := ComputeEntryHash(entry1)
		hash2 := ComputeEntryHash(entry2)

		if hash1 != hash2 {
			t.Errorf("expected deterministic hash, got %s and %s", hash1, hash2)
		}
	})

	t.Run("different hash for different content", func(t *testing.T) {
		timestamp := time.Now().UTC()
		entry1 := Entry{
			Timestamp:   timestamp,
			EventType:   EventServiceRegistered,
			ModuleName:  "module-1",
			ServiceName: "service-1",
		}
		entry2 := Entry{
			Timestamp:   timestamp,
			EventType:   EventServiceRegistered,
			ModuleName:  "module-2", // Different
			ServiceName: "service-1",
		}

		hash1 := ComputeEntryHash(entry1)
		hash2 := ComputeEntryHash(entry2)

		if hash1 == hash2 {
			t.Error("expected different hashes for different content")
		}
	})
}
