package kvjetstream

import (
	"context"

	"github.com/go-monolith/mono/pkg/storage"
)

// KVStoragePort is the port interface exposed to consumer modules.
// It defines operations that consumers can perform on a KV bucket.
//
// This interface embeds storage.Storage and extended interfaces for full
// KV capabilities including revision-based optimistic locking and watching.
//
// # Interface Hierarchy
//
//   - storage.Storage: Base operations (Get, Set, Delete, Reset, Close)
//   - storage.StorageWithBucket: Bucket name awareness
//   - storage.StorageWithWatch: Real-time change notifications
//   - storage.StorageWithRevision: Optimistic locking via revisions
//   - storage.StorageWithKeys: Key enumeration
//   - storage.StorageWithStatus: Bucket status information
//
// # Revision-Based Optimistic Locking
//
// KV operations support optimistic locking via revisions:
//   - GetEntry returns the current revision in Entry.Revision
//   - Update requires the expected revision; fails if changed
//   - Create fails if the key already exists
//
// This enables safe concurrent updates without distributed locks.
//
// # Get vs GetEntry
//
// The port provides two ways to retrieve values:
//   - Get(key) / GetWithContext(ctx, key): Returns []byte only, nil if not found
//   - GetEntry(key) / GetEntryWithContext(ctx, key): Returns *Entry with revision metadata
//
// Use Get() for simple value retrieval. Use GetEntry() when you need revision
// for subsequent Update() calls.
//
// # Context and Timeouts
//
// Operations have both context and non-context variants. Callers should use
// contexts with appropriate timeouts to prevent hung operations.
//
// Recommended timeout guidelines:
//   - Read operations (Get, Keys, Status): 5-10 seconds
//   - Write operations (Set, Create, Update, Delete, Purge): 10-30 seconds
//   - Watch operations: Use context cancellation for graceful shutdown
//
// # Usage
//
// Consumer modules access buckets via the plugin:
//
//	type CacheModule struct {
//	    kvPlugin *kvjetstream.PluginModule
//	    cache    kvjetstream.KVStoragePort
//	}
//
//	func (m *CacheModule) Start(ctx context.Context) error {
//	    m.cache = m.kvPlugin.Bucket("cache")
//	    if m.cache == nil {
//	        return fmt.Errorf("bucket 'cache' not found")
//	    }
//
//	    // Simple store (no revision return)
//	    err := m.cache.Set("user:123", []byte(`{"name":"Alice"}`), 0)
//	    if err != nil {
//	        return err
//	    }
//
//	    // Store with revision return
//	    revision, err := m.cache.PutWithRevision("user:123", []byte(`{"name":"Bob"}`), 0)
//	    if err != nil {
//	        return err
//	    }
//	    fmt.Printf("New revision: %d\n", revision)
//
//	    // Simple retrieve (nil if not found)
//	    data, err := m.cache.Get("user:123")
//	    if err != nil {
//	        return err
//	    }
//	    if data == nil {
//	        fmt.Println("Key not found")
//	    }
//
//	    // Retrieve with revision for update
//	    entry, err := m.cache.GetEntry("user:123")
//	    if err != nil {
//	        return err
//	    }
//	    fmt.Printf("Value: %s, Revision: %d\n", entry.Value, entry.Revision)
//
//	    return nil
//	}
//
// # Passing to Functions Expecting storage.Storage
//
// KVStoragePort satisfies storage.Storage, allowing it to be passed to
// generic storage functions:
//
//	func processStorage(s storage.Storage) {
//	    data, _ := s.Get("key")
//	    s.Set("key", []byte("value"), 0)
//	}
//
//	bucket := plugin.Bucket("cache")
//	processStorage(bucket) // Works!
type KVStoragePort interface {
	// Embed base storage interface for basic operations.
	// Provides: Get, GetWithContext, Set, SetWithContext, Delete, DeleteWithContext,
	// Reset, ResetWithContext, Close
	storage.Storage

	// Embed extended interfaces for KV-specific capabilities.
	storage.StorageWithBucket   // BucketName()
	storage.StorageWithWatch    // Watch, WatchWithContext
	storage.StorageWithRevision // Create, Update, Purge, GetEntry, PutWithRevision (+ context)
	storage.StorageWithKeys     // Keys, KeysWithContext
	storage.StorageWithStatus   // Status, StatusWithContext

	// WatchAll creates a watcher for all keys in the bucket.
	// This is a convenience method equivalent to Watch(">", opts...).
	// Pattern ">" matches all keys.
	//
	// Options control behavior:
	//   - WithUpdatesOnly: Only receive future updates (skip current values)
	//   - WithIgnoreDeletes: Filter out delete markers
	//   - WithMetaOnly: Receive only metadata without values
	//   - WithResumeFromRevision: Resume from a specific revision
	WatchAll(ctx context.Context, opts ...WatchOption) (KeyWatcher, error)
}

// KeyWatcher is an alias to storage.KeyWatcher.
// Provides real-time notifications of key changes.
// Must be stopped when no longer needed to release resources.
//
// IMPORTANT: Always defer watcher.Stop() immediately after creation to prevent
// goroutine and channel leaks. The watcher maintains an active goroutine and
// buffered channel until stopped. Failure to call Stop() will leak these resources.
//
// # Lifecycle Management
//
// The watcher starts processing updates immediately upon creation. The goroutine
// continues running until Stop() is called or the underlying connection is lost.
// After Stop() is called:
//   - The Updates channel will be closed
//   - No more entries will be sent
//   - Resources will be released
//
// It is safe to call Stop() immediately after creation if needed. The watcher
// uses internal synchronization to ensure the goroutine is running before
// returning from Watch().
//
// # Usage Pattern
//
//	watcher, err := bucket.Watch("user.*")
//	if err != nil {
//	    return err
//	}
//	defer watcher.Stop() // Always defer Stop() immediately!
//
//	for entry := range watcher.Updates() {
//	    if entry == nil {
//	        // Initial sync complete (only without WithUpdatesOnly)
//	        continue
//	    }
//	    fmt.Printf("Key %s changed to revision %d\n", entry.Key, entry.Revision)
//	}
type KeyWatcher = storage.KeyWatcher
