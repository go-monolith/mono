# kv-jetstream

A key-value store plugin for mono-framework using NATS JetStream KV Store as the backend.

## Overview

The `kv-jetstream` plugin provides bucket-based key-value storage capabilities using JetStream KV Store. It implements the `PluginModule` interface, allowing consumer modules to store and retrieve key-value pairs with features like:

- Multiple bucket support with independent configurations
- File and memory storage options
- TTL-based automatic key expiration
- Revision-based optimistic locking
- Real-time change notifications via Watch
- Sentinel errors for programmatic error handling

## Architecture

The plugin follows a hexagonal architecture with clean port/adapter separation:

```
Consumer Module
     │
     │ uses
     ▼
KVStoragePort (interface)       ← Consumer-facing interface
     │
     │ implemented by
     ▼
KVStorageAdapter (struct)       ← Wraps backend, can add logging/metrics
     │
     │ wraps
     ▼
KVStorageBackend (interface)    ← Internal backend interface
     │
     │ implemented by
     ▼
JetStreamKVBackend (struct)     ← JetStream KV implementation
```

## Installation

Import the plugin in your application:

```go
import kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
```

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

    // Create the framework with JetStream enabled
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamStorageDir("/tmp/jetstream"),
    )
    if err != nil {
        panic(err)
    }

    // Create the KV plugin with bucket configurations
    kvStore, err := kvjetstream.New(kvjetstream.Config{
        Buckets: []kvjetstream.BucketConfig{
            {
                Name:        "cache",
                Description: "Application cache",
                TTL:         1 * time.Hour,           // Auto-expire after 1 hour
                Storage:     kvjetstream.MemoryStorage,
            },
            {
                Name:        "sessions",
                Description: "User sessions",
                MaxBytes:    100 * 1024 * 1024,       // 100MB limit
                Replicas:    1,
            },
        },
    })
    if err != nil {
        panic(err)
    }

    // Register the plugin with an alias
    app.RegisterPlugin(kvStore, "kv")

    // Register consumer modules
    app.Register(&SessionModule{})

    if err := app.Start(ctx); err != nil {
        panic(err)
    }
}
```

### 2. Create a Consumer Module

```go
type SessionModule struct {
    kvPlugin *kvjetstream.PluginModule
    sessions kvjetstream.KVStoragePort
}

func (m *SessionModule) Name() string { return "sessions" }

// Receive plugin instance
func (m *SessionModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "kv" {
        m.kvPlugin = plugin.(*kvjetstream.PluginModule)
    }
}

func (m *SessionModule) Start(ctx context.Context) error {
    // Get bucket from plugin
    m.sessions = m.kvPlugin.Bucket("sessions")
    if m.sessions == nil {
        return fmt.Errorf("bucket 'sessions' not found")
    }

    // Store a session
    sessionData := []byte(`{"user_id":"123","role":"admin"}`)
    rev, err := m.sessions.Put(ctx, "sess:abc123", sessionData)
    if err != nil {
        return err
    }
    fmt.Printf("Stored session with revision: %d\n", rev)

    // Retrieve the session
    entry, err := m.sessions.Get(ctx, "sess:abc123")
    if err != nil {
        return err
    }
    fmt.Printf("Retrieved: %s\n", string(entry.Value))

    return nil
}

func (m *SessionModule) Stop(ctx context.Context) error {
    return nil
}
```

## Configuration

### Config

```go
type Config struct {
    Buckets []BucketConfig  // Buckets to create (at least one required)
}
```

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

### Module Options

```go
// Create with custom logger
kvStore, err := kvjetstream.New(config, kvjetstream.WithLogger(myLogger))
```

## API Reference

### Module Methods

| Method | Description |
|--------|-------------|
| `Bucket(name string) KVStoragePort` | Get a bucket by name (returns nil if not found) |
| `Buckets() []string` | List all bucket names |
| `HasBucket(name string) bool` | Check if a bucket exists |

### KVStoragePort Interface

The consumer-facing interface for KV operations:

```go
type KVStoragePort interface {
    BucketName() string

    // CRUD operations
    Put(ctx context.Context, key string, value []byte) (revision uint64, error)
    Get(ctx context.Context, key string) (*KVEntry, error)
    Create(ctx context.Context, key string, value []byte) (revision uint64, error)
    Update(ctx context.Context, key string, value []byte, revision uint64) (revision uint64, error)
    Delete(ctx context.Context, key string, opts ...DeleteOption) error
    Purge(ctx context.Context, key string) error

    // Query operations
    Keys(ctx context.Context) ([]string, error)

    // Watch operations
    Watch(ctx context.Context, pattern string, opts ...WatchOption) (KeyWatcher, error)
    WatchAll(ctx context.Context, opts ...WatchOption) (KeyWatcher, error)

    // Status
    Status(ctx context.Context) (*BucketStatus, error)
}
```

### KVEntry

Metadata and value returned for stored keys:

```go
type KVEntry struct {
    Bucket    string        // Bucket name
    Key       string        // Key name
    Value     []byte        // Value data
    Revision  uint64        // Revision number (for optimistic locking)
    Timestamp time.Time     // Last modified time
    Operation KeyOperation  // Put, Delete, or Purge
}
```

### KeyWatcher Interface

```go
type KeyWatcher interface {
    Updates() <-chan *KVEntry  // nil entry signals initial sync complete
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

Usage with `errors.Is()`:

```go
entry, err := bucket.Get(ctx, "nonexistent")
if errors.Is(err, kvjetstream.ErrKeyNotFound) {
    // Handle missing key
}

_, err = bucket.Create(ctx, "existing-key", data)
if errors.Is(err, kvjetstream.ErrKeyExists) {
    // Key already exists, use Put() or Update() instead
}

_, err = bucket.Update(ctx, "key", newData, staleRevision)
if errors.Is(err, kvjetstream.ErrRevisionMismatch) {
    // Concurrent modification detected, retry with fresh revision
}
```

## Usage Examples

### Basic CRUD Operations

```go
// Put (create or overwrite)
rev, err := bucket.Put(ctx, "user:123", []byte(`{"name":"Alice"}`))

// Get
entry, err := bucket.Get(ctx, "user:123")
fmt.Printf("Value: %s, Revision: %d\n", entry.Value, entry.Revision)

// Delete (soft delete - leaves tombstone)
err = bucket.Delete(ctx, "user:123")

// Purge (hard delete - removes all history)
err = bucket.Purge(ctx, "user:123")
```

### Atomic Create (Fails if Key Exists)

```go
// Create only succeeds if key doesn't exist
rev, err := bucket.Create(ctx, "lock:resource", []byte("owner-123"))
if errors.Is(err, kvjetstream.ErrKeyExists) {
    fmt.Println("Resource is already locked")
}
```

### Optimistic Locking with Update

```go
// Get current value and revision
entry, err := bucket.Get(ctx, "counter")
if err != nil {
    return err
}

// Modify the value
count, _ := strconv.Atoi(string(entry.Value))
count++
newValue := []byte(strconv.Itoa(count))

// Update with revision check (fails if modified by another process)
newRev, err := bucket.Update(ctx, "counter", newValue, entry.Revision)
if errors.Is(err, kvjetstream.ErrRevisionMismatch) {
    // Another process modified the value, retry
    return fmt.Errorf("concurrent modification, please retry")
}
```

### Conditional Delete

```go
// Delete only if revision matches
entry, _ := bucket.Get(ctx, "key")
err := bucket.Delete(ctx, "key", kvjetstream.WithDeleteRevision(entry.Revision))
if errors.Is(err, kvjetstream.ErrRevisionMismatch) {
    // Key was modified since we read it
}
```

### List All Keys

```go
keys, err := bucket.Keys(ctx)
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
// Watch keys matching a pattern (e.g., "user.*" matches "user.123", "user.456")
watcher, err := bucket.Watch(ctx, "user.*")
if err != nil {
    return err
}
defer watcher.Stop()

// Process updates...
```

### Watch Options

```go
// Only receive new updates (skip existing values)
watcher, err := bucket.Watch(ctx, ">", kvjetstream.WithUpdatesOnly())

// Ignore delete operations
watcher, err := bucket.Watch(ctx, ">", kvjetstream.WithIgnoreDeletes())

// Resume from a specific revision
watcher, err := bucket.Watch(ctx, ">", kvjetstream.WithResumeFromRevision(100))
```

### Using Multiple Buckets

```go
type CacheModule struct {
    kvPlugin *kvjetstream.PluginModule
    cache    kvjetstream.KVStoragePort
    sessions kvjetstream.KVStoragePort
}

func (m *CacheModule) Start(ctx context.Context) error {
    m.cache = m.kvPlugin.Bucket("cache")
    m.sessions = m.kvPlugin.Bucket("sessions")

    // Use different buckets for different purposes
    m.cache.Put(ctx, "config:app", configData)
    m.sessions.Put(ctx, "sess:user123", sessionData)

    return nil
}
```

### Multiple Plugin Instances

Register multiple instances with different aliases for different configurations:

```go
// Fast in-memory cache
cache, _ := kvjetstream.New(kvjetstream.Config{
    Buckets: []kvjetstream.BucketConfig{
        {Name: "hot-cache", Storage: kvjetstream.MemoryStorage, TTL: 5 * time.Minute},
    },
})

// Persistent storage
persistent, _ := kvjetstream.New(kvjetstream.Config{
    Buckets: []kvjetstream.BucketConfig{
        {Name: "user-data", Storage: kvjetstream.FileStorage, Replicas: 3},
    },
})

app.RegisterPlugin(cache, "cache")
app.RegisterPlugin(persistent, "storage")

// Consumer module can use both
type DataModule struct {
    cachePlugin   *kvjetstream.PluginModule
    storagePlugin *kvjetstream.PluginModule
}

func (m *DataModule) SetPlugin(alias string, plugin mono.PluginModule) {
    switch alias {
    case "cache":
        m.cachePlugin = plugin.(*kvjetstream.PluginModule)
    case "storage":
        m.storagePlugin = plugin.(*kvjetstream.PluginModule)
    }
}
```

## Plugin Lifecycle

1. **Startup**: Plugin starts FIRST (before middleware and regular modules)
2. **Bucket Creation**: All configured buckets are created/updated in JetStream
3. **Consumer Access**: Consumer modules receive plugin via `SetPlugin()`
4. **Shutdown**: Plugin stops LAST (after all modules and middleware)

## Design Principles

- **Zero Framework Changes**: Uses existing `PluginModule` interface
- **Direct Method Access**: No JSON serialization, no channel overhead
- **Type Safety**: Compile-time type checking via interfaces
- **Optimistic Locking**: Revision-based concurrency control
- **Real-time Updates**: Watch API for change notifications
- **Sentinel Errors**: Programmatic error handling with `errors.Is()`
- **Port/Adapter Pattern**: Clean separation enables future backend implementations (e.g., Redis)

## Comparison with fs-jetstream

| Aspect | kv-jetstream (KV Store) | fs-jetstream (Object Store) |
|--------|-------------------------|------------------------------|
| Data Model | Small key-value pairs | Large binary objects |
| Max Value Size | ~1MB recommended | Unlimited (chunked) |
| Concurrency | Revision-based locking | None |
| Watch | Real-time notifications | Not supported |
| Streaming | Not supported | Reader/Writer support |
| Use Case | Cache, sessions, config | Files, blobs, documents |
