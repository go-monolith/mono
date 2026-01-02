# Key-Value Storage Plugin (kv-jetstream)

The Key-Value Storage Plugin provides fast, distributed key-value storage using NATS JetStream KV Store. It's ideal for caching, sessions, configuration, and any application state that needs to be shared across modules.

## Overview

The `kv-jetstream` plugin implements bucket-based key-value storage capabilities using JetStream KV Store as the backend. Key features include:

- **Multiple Bucket Support**: Organize keys into separate buckets with independent configurations
- **Flexible Storage**: Choose between disk-based (persistent) and memory-based (fast) storage
- **Revision-Based Locking**: Optimistic locking with revision numbers for concurrent updates
- **Real-Time Notifications**: Watch API for real-time change notifications
- **TTL Support**: Automatically expire keys after a specified duration
- **Compression**: Optional S2 compression for space efficiency
- **Distributed**: Shared across all modules in the application

## Architecture

The plugin uses a hexagonal (ports & adapters) architecture with clean separation:

```
Consumer Module
     │
     │ uses
     ▼
KVStoragePort (interface)       ← Consumer-facing interface
     │
     │ implemented by
     ▼
KVStorageAdapter (struct)       ← Wraps backend, can add logging
     │
     │ wraps
     ▼
KVStorageBackend (interface)    ← Internal backend interface
     │
     │ implemented by
     ▼
JetStreamKVBackend (struct)     ← JetStream KV implementation
```

This design allows for future alternative backends (Redis, etc.) without changing consumer code.

## Interface Hierarchy

The `KVStoragePort` interface embeds multiple interfaces from the `pkg/storage` package, providing a unified storage abstraction:

```
KVStoragePort
     │
     ├── storage.Storage (base)
     │   └── Get, Set, Delete, Reset, Close (+ context variants)
     │
     ├── storage.StorageWithBucket
     │   └── BucketName()
     │
     ├── storage.StorageWithWatch
     │   └── Watch, WatchWithContext
     │
     ├── storage.StorageWithRevision
     │   └── Create, Update, Purge, GetEntry, PutWithRevision (+ context variants)
     │
     ├── storage.StorageWithKeys
     │   └── Keys, KeysWithContext
     │
     ├── storage.StorageWithStatus
     │   └── Status, StatusWithContext
     │
     └── WatchAll (KV-specific)
```

### Embedded Interfaces

| Interface | Methods | Purpose |
|-----------|---------|---------|
| `storage.Storage` | Get, Set, Delete, Reset, Close | Base key-value operations |
| `storage.StorageWithBucket` | BucketName | Bucket identification |
| `storage.StorageWithWatch` | Watch, WatchWithContext | Real-time change notifications |
| `storage.StorageWithRevision` | Create, Update, Purge, GetEntry, PutWithRevision | Optimistic locking |
| `storage.StorageWithKeys` | Keys, KeysWithContext | Key enumeration |
| `storage.StorageWithStatus` | Status, StatusWithContext | Bucket status information |

### Type Aliases

The plugin re-exports types from `pkg/storage` for convenience:

| Plugin Type | Source | Description |
|-------------|--------|-------------|
| `KVEntry` | `storage.Entry` | Key-value entry with revision metadata |
| `BucketStatus` | `storage.BucketStatus` | Bucket status information |
| `KeyWatcher` | `storage.KeyWatcher` | Watch subscription handle |
| `KeyOperation` | `storage.KeyOperation` | Operation type (Put, Delete, Purge) |
| `WatchOption` | `storage.WatchOption` | Watch configuration options |
| `WatchOptions` | `storage.WatchOptions` | Watch options struct |
| `DeleteOption` | `storage.DeleteOption` | Delete configuration options |
| `DeleteOptions` | `storage.DeleteOptions` | Delete options struct |

### Context Variants Pattern

Most methods have two variants:
- **Non-context**: `Get(key)` - Uses `context.Background()` internally
- **Context-aware**: `GetWithContext(ctx, key)` - Accepts explicit context

Use context-aware variants when you need:
- Timeout control
- Cancellation support
- Request tracing/logging

```go
// Simple usage (uses context.Background() internally)
data, err := bucket.Get("key")

// Context-aware usage (explicit timeout/cancellation)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
data, err := bucket.GetWithContext(ctx, "key")
```

## Installation

Import the plugin:

```go
import kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
```

## Signatures

### New

```go
func New(config Config, opts ...Option) (*PluginModule, error)
```

Creates a new key-value storage plugin with the given bucket configurations.

### Bucket Methods

```go
func (p *PluginModule) Bucket(name string) KVStoragePort
func (p *PluginModule) Buckets() []string
func (p *PluginModule) HasBucket(name string) bool
```

Access configured KV storage buckets.

## Quick Start

### 1. Create and Register the Plugin

```go
package main

import (
    "context"
    "time"

    mono "github.com/go-monolith/mono"
    kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
)

func main() {
    ctx := context.Background()

    // Create application with JetStream enabled
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamStorageDir("/tmp/jetstream"),
    )
    if err != nil {
        panic(err)
    }

    // Create KV plugin with bucket configurations
    kvStore, err := kvjetstream.New(kvjetstream.Config{
        Buckets: []kvjetstream.BucketConfig{
            {
                Name:        "cache",
                Description: "Application cache",
                TTL:         1 * time.Hour,  // Auto-expire after 1 hour
                Storage:     kvjetstream.MemoryStorage,
            },
            {
                Name:        "sessions",
                Description: "User sessions",
                MaxBytes:    100 * 1024 * 1024,  // 100MB limit
                Replicas:    1,
            },
        },
    })
    if err != nil {
        panic(err)
    }

    // Register plugin with an alias
    app.RegisterPlugin(kvStore, "kv")

    // Register modules that use KV store
    app.Register(&SessionModule{})

    if err := app.Start(ctx); err != nil {
        panic(err)
    }
}
```

### 2. Create a Consumer Module

```go
type SessionModule struct {
    kv       *kvjetstream.PluginModule
    sessions kvjetstream.KVStoragePort
}

func (m *SessionModule) Name() string { return "sessions" }

// Receive plugin instance
func (m *SessionModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "kv" {
        m.kv = plugin.(*kvjetstream.PluginModule)
    }
}

func (m *SessionModule) Start(ctx context.Context) error {
    // Get bucket from plugin
    m.sessions = m.kv.Bucket("sessions")
    if m.sessions == nil {
        return fmt.Errorf("bucket 'sessions' not found")
    }

    // Store a session (simple set, no revision tracking)
    sessionData := []byte(`{"user_id":"123","role":"admin"}`)
    err := m.sessions.Set("sess:abc123", sessionData, 0) // 0 = no expiration
    if err != nil {
        return err
    }
    fmt.Println("Stored session")

    // Store with revision tracking (for optimistic locking)
    rev, err := m.sessions.PutWithRevision("sess:abc123", sessionData, 0)
    if err != nil {
        return err
    }
    fmt.Printf("Stored session with revision: %d\n", rev)

    // Retrieve the session (value only)
    data, err := m.sessions.Get("sess:abc123")
    if err != nil {
        return err
    }
    fmt.Printf("Retrieved: %s\n", string(data))

    // Retrieve with metadata (for revision-based updates)
    entry, err := m.sessions.GetEntry("sess:abc123")
    if err != nil {
        return err
    }
    fmt.Printf("Retrieved: %s (revision: %d)\n", string(entry.Value), entry.Revision)

    return nil
}

func (m *SessionModule) Stop(ctx context.Context) error {
    return nil
}
```

## Configuration

### BucketConfig

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Bucket name (required, must be unique) |
| `Description` | `string` | Optional bucket description |
| `MaxValueSize` | `int32` | Maximum size per value in bytes (0 = unlimited) |
| `MaxBytes` | `int64` | Maximum total size of bucket (0 = unlimited) |
| `TTL` | `time.Duration` | Time-to-live for keys (0 = no expiry) |
| `Replicas` | `int` | Number of replicas (1-5, default 1) |
| `Storage` | `StorageType` | `FileStorage` (default) or `MemoryStorage` |
| `Compression` | `bool` | Enable S2 compression |

### StorageType

```go
const (
    FileStorage   StorageType = iota  // Disk-based (persistent, default)
    MemoryStorage                      // Memory-based (faster, not persistent)
)
```

### Plugin Options

```go
// Create with custom logger
kvStore, err := kvjetstream.New(config, kvjetstream.WithLogger(myLogger))
```

## Default Config

```go
// Minimal configuration (single bucket)
kvStore, _ := kvjetstream.New(kvjetstream.Config{
    Buckets: []kvjetstream.BucketConfig{
        {
            Name:     "cache",
            Storage:  kvjetstream.FileStorage,  // Persistent by default
            Replicas: 1,
        },
    },
})
```

## API Reference

### Module Methods

| Method | Description |
|--------|-------------|
| `Bucket(name string) KVStoragePort` | Get a bucket by name (returns nil if not found) |
| `Buckets() []string` | List all bucket names |
| `HasBucket(name string) bool` | Check if a bucket exists |

### KVStoragePort Interface

Consumer-facing interface for KV operations. The interface embeds `storage.Storage` for basic operations and additional interfaces for KV-specific capabilities:

```go
type KVStoragePort interface {
    // From storage.Storage (base operations)
    Get(key string) ([]byte, error)
    GetWithContext(ctx context.Context, key string) ([]byte, error)
    Set(key string, val []byte, exp time.Duration) error
    SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error
    Delete(key string) error
    DeleteWithContext(ctx context.Context, key string) error
    Reset() error
    ResetWithContext(ctx context.Context) error
    Close() error

    // From storage.StorageWithBucket
    BucketName() string

    // From storage.StorageWithRevision (optimistic locking)
    Create(key string, val []byte, exp time.Duration) (uint64, error)
    CreateWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error)
    Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error)
    UpdateWithContext(ctx context.Context, key string, val []byte, exp time.Duration, revision uint64) (uint64, error)
    Purge(key string) error
    PurgeWithContext(ctx context.Context, key string) error
    GetEntry(key string) (*Entry, error)
    GetEntryWithContext(ctx context.Context, key string) (*Entry, error)
    PutWithRevision(key string, val []byte, exp time.Duration) (uint64, error)
    PutWithRevisionWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error)

    // From storage.StorageWithKeys
    Keys() ([]string, error)
    KeysWithContext(ctx context.Context) ([]string, error)

    // From storage.StorageWithWatch
    Watch(pattern string, opts ...WatchOption) (KeyWatcher, error)
    WatchWithContext(ctx context.Context, pattern string, opts ...WatchOption) (KeyWatcher, error)

    // KV-specific
    WatchAll(ctx context.Context, opts ...WatchOption) (KeyWatcher, error)

    // From storage.StorageWithStatus
    Status() (*BucketStatus, error)
    StatusWithContext(ctx context.Context) (*BucketStatus, error)
}
```

{% hint style="info" %}
**Convenience vs Revision-Tracking**: Use `Set()` for simple store operations. Use `PutWithRevision()` when you need the revision number for optimistic locking. Use `GetEntry()` instead of `Get()` when you need both value and revision metadata.
{% endhint %}

### Entry

Metadata and value returned for stored keys (from `storage.Entry`):

```go
type Entry struct {
    Bucket    string        // Bucket name
    Key       string        // Key name
    Value     []byte        // Value data (nil for delete markers)
    Revision  uint64        // Revision number (for optimistic locking)
    Timestamp time.Time     // When this revision was created
    Operation KeyOperation  // Put, Delete, or Purge
}
```

### KeyWatcher Interface

```go
type KeyWatcher interface {
    Updates() <-chan *Entry  // nil entry signals initial sync complete
    Stop() error
}
```

### BucketStatus

```go
type BucketStatus struct {
    Bucket       string         // Bucket name
    Values       uint64         // Number of keys
    TTL          time.Duration  // Configured TTL
    BackingStore string         // Storage type description
    Bytes        uint64         // Total bytes used
}
```

## Sentinel Errors

The plugin provides sentinel errors for programmatic error handling:

```go
var (
    ErrKeyNotFound      = errors.New("key not found")
    ErrKeyExists        = errors.New("key already exists")
    ErrRevisionMismatch = errors.New("revision mismatch")
    ErrBucketNotFound   = errors.New("bucket not found")
)
```

## Usage Examples

### Basic CRUD Operations

{% hint style="info" %}
**Get vs GetEntry**: Use `Get()` for simple value retrieval when you just need the bytes. Use `GetEntry()` when you need the revision number for subsequent `Update()` calls (optimistic locking).
{% endhint %}

```go
// Set (create or overwrite) - simple storage, no revision returned
err := bucket.Set("user:123", []byte(`{"name":"Alice"}`), 0) // 0 = no expiration
if err != nil {
    return err
}

// PutWithRevision - when you need the revision number for optimistic locking
rev, err := bucket.PutWithRevision("user:123", []byte(`{"name":"Alice"}`), 0)
if err != nil {
    return err
}
fmt.Printf("Stored with revision: %d\n", rev)

// Get - returns value only
data, err := bucket.Get("user:123")
if err != nil {
    return err
}
fmt.Printf("Value: %s\n", string(data))

// GetEntry - returns value with revision metadata
entry, err := bucket.GetEntry("user:123")
if err != nil {
    return err
}
fmt.Printf("Value: %s, Revision: %d\n", string(entry.Value), entry.Revision)

// Delete (soft delete - leaves tombstone)
err = bucket.Delete("user:123")
if err != nil {
    return err
}

// Purge (hard delete - removes all history)
err = bucket.Purge("user:123")
if err != nil {
    return err
}
```

### Atomic Create (Fails if Key Exists)

```go
// Create only succeeds if key doesn't exist
// Signature: Create(key string, val []byte, exp time.Duration) (uint64, error)
rev, err := bucket.Create("lock:resource", []byte("owner-123"), 0)
if errors.Is(err, kvjetstream.ErrKeyExists) {
    fmt.Println("Resource is already locked")
    return err
}
fmt.Printf("Lock acquired with revision: %d\n", rev)
```

### Optimistic Locking with Update

```go
// Get current value and revision (use GetEntry for revision metadata)
entry, err := bucket.GetEntry("counter")
if err != nil {
    return err
}

// Modify the value
count, _ := strconv.Atoi(string(entry.Value))
count++
newValue := []byte(strconv.Itoa(count))

// Update with revision check (fails if modified by another process)
// Signature: Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error)
newRev, err := bucket.Update("counter", newValue, 0, entry.Revision)
if errors.Is(err, kvjetstream.ErrRevisionMismatch) {
    // Another process modified the value, retry
    return fmt.Errorf("concurrent modification, please retry")
}
fmt.Printf("Updated to revision: %d\n", newRev)
```

### List All Keys

```go
keys, err := bucket.Keys()
if err != nil {
    return err
}
for _, key := range keys {
    fmt.Println(key)
}
```

### Watch for Changes

```go
// Watch all keys
watcher, err := bucket.WatchAll(ctx)
if err != nil {
    return err
}
defer watcher.Stop()

for entry := range watcher.Updates() {
    if entry == nil {
        // Initial sync complete
        fmt.Println("Caught up with existing values")
        continue
    }

    switch entry.Operation {
    case kvjetstream.KeyOperationPut:
        fmt.Printf("Key %s updated: %s\n", entry.Key, entry.Value)
    case kvjetstream.KeyOperationDelete:
        fmt.Printf("Key %s deleted\n", entry.Key)
    }
}
```

### Watch with Pattern

```go
// Watch keys matching a pattern (e.g., "user.*")
// Use Watch for non-context version, WatchWithContext for context-aware
watcher, err := bucket.Watch("user.*")
if err != nil {
    return err
}
defer watcher.Stop()

for entry := range watcher.Updates() {
    if entry != nil {
        fmt.Printf("User %s changed: %s\n", entry.Key, entry.Value)
    }
}
```

### Watch Options

```go
// Only receive new updates (skip existing values)
watcher, err := bucket.Watch(">", kvjetstream.WithUpdatesOnly())

// Ignore delete operations
watcher, err := bucket.Watch(">", kvjetstream.WithIgnoreDeletes())

// Resume from a specific revision
watcher, err := bucket.Watch(">", kvjetstream.WithResumeFromRevision(100))
```

### Using Multiple Buckets

```go
type CacheModule struct {
    kv       *kvjetstream.PluginModule
    cache    kvjetstream.KVStoragePort
    sessions kvjetstream.KVStoragePort
}

func (m *CacheModule) Start(ctx context.Context) error {
    m.cache = m.kv.Bucket("cache")
    m.sessions = m.kv.Bucket("sessions")

    // Use different buckets for different purposes
    m.cache.Set("config:app", configData, 0)
    m.sessions.Set("sess:user123", sessionData, time.Hour) // expires in 1 hour

    return nil
}
```

## Plugin Lifecycle

1. **Startup**: Plugin starts FIRST (before middleware and regular modules)
2. **Bucket Creation**: All configured buckets are created/updated in JetStream
3. **Consumer Access**: Consumer modules receive plugin via `SetPlugin()`
4. **Shutdown**: Plugin stops LAST (after all modules and middleware)

## Performance Characteristics

- **Read Latency**: Sub-millisecond for in-memory, 1-5ms for disk-based
- **Write Latency**: 1-10ms depending on replication factor
- **Throughput**: Up to 50,000 ops/sec for in-memory buckets
- **Storage Overhead**: ~200 bytes per key for metadata
- **Revision Tracking**: Atomic, linearizable updates

## Best Practices

✓ **Do**
- Use KV store for frequently accessed small values
- Set TTL for temporary data (sessions, cache)
- Use optimistic locking for concurrent updates
- Watch for changes instead of polling
- Use separate buckets for different data types
- Monitor bucket size with Status()

✗ **Don't**
- Store large values (>1MB) - use fs-jetstream instead
- Forget to handle ErrRevisionMismatch in loops
- Use Set() when Create() or Update() would be clearer for intent
- Watch without handling nil entries (sync signal)
- Mix different data types in one bucket
- Rely on TTL for critical cleanup logic

## Comparison with File Storage

| Aspect | kv-jetstream | fs-jetstream |
|--------|------------|------------|
| Data Model | Key-value pairs | Objects (binary) |
| Max Value Size | ~1MB recommended | Unlimited |
| Concurrency Control | Revision-based locking | None |
| Real-Time Notifications | Watch API | Not supported |
| Streaming Support | Not supported | Reader/Writer |
| Use Case | Cache, sessions, config | Files, documents, media |

## Related Documentation

- [Storage API](../api/storage.md) - Unified storage interface documentation
- [Plugin System Overview](README.md)
- [File Storage Plugin (fs-jetstream)](fs-jetstream.md)
- [Creating Custom Plugins](creating-plugins.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For large file storage, see [fs-jetstream](fs-jetstream.md). For creating your own plugins, see [Creating Plugins](creating-plugins.md).
