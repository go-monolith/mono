# Key-Value Storage Plugin (kv-jetstream)

This page shows how to keep small values in the `kv-jetstream` plugin: setting it up, configuring buckets, key rules, and the operations a module can call, including optimistic locking and watching for changes. Read it when your modules share caches, sessions, configuration or other small application state. For files and large binary objects, use [fs-jetstream](fs-jetstream.md) instead; [Storage Backends](../guides/storage-backends.md#backend-comparison) compares the two.

The plugin provides fast, distributed key-value storage using NATS JetStream KV Store.

## Overview

The `kv-jetstream` plugin implements bucket-based key-value storage capabilities using JetStream KV Store as the backend. Key features include:

- **Multiple Bucket Support**: Organize keys into separate buckets with independent configurations
- **Flexible Storage**: Choose between disk-based (persistent) and memory-based (fast) storage
- **Revision-Based Locking**: Optimistic locking with revision numbers for concurrent updates
- **Real-Time Notifications**: Watch API for real-time change notifications
- **TTL Support**: Automatically expire keys after a bucket-level duration (`BucketConfig.TTL`)
- **Compression**: Optional S2 compression for space efficiency
- **Distributed**: Shared across all modules in the application

## Quick Start

### 1. Create and Register the Plugin

Import the plugin as `kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"`. The application must enable JetStream:

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
    if err := app.RegisterPlugin(kvStore, "kv"); err != nil {
        panic(err)
    }

    // Register modules that use KV store
    if err := app.Register(&SessionModule{}); err != nil {
        panic(err)
    }

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
    err := m.sessions.Set("sess.abc123", sessionData, 0) // 0 = no expiration
    if err != nil {
        return err
    }
    fmt.Println("Stored session")

    // Store with revision tracking (for optimistic locking)
    rev, err := m.sessions.PutWithRevision("sess.abc123", sessionData, 0)
    if err != nil {
        return err
    }
    fmt.Printf("Stored session with revision: %d\n", rev)

    // Retrieve the session (value only)
    data, err := m.sessions.Get("sess.abc123")
    if err != nil {
        return err
    }
    fmt.Printf("Retrieved: %s\n", string(data))

    // Retrieve with metadata (for revision-based updates)
    entry, err := m.sessions.GetEntry("sess.abc123")
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

The plugin creates or updates every configured bucket in JetStream when it starts, before middleware and modules start. For the full start and stop order, see [Lifecycle Order](../api/plugins.md#lifecycle-order).

## Configuration

### BucketConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Name` | `string` | (required) | Bucket name (required, must be unique) |
| `Description` | `string` | `""` | Optional bucket description |
| `MaxValueSize` | `int32` | `0` (unlimited) | Maximum size per value in bytes (0 = unlimited) |
| `MaxBytes` | `int64` | `0` (unlimited) | Maximum total size of bucket (0 = unlimited) |
| `TTL` | `time.Duration` | `0` (no expiry) | Time-to-live for keys (0 = no expiry). This is the only expiry: the `exp` argument of `Set`, `Create`, `Update` and `PutWithRevision` is ignored, because JetStream KV has no per-key TTL |
| `Replicas` | `int` | `0` (means 1) | Number of replicas (1-5; 0 means 1). The framework's `WithJetStreamDefaultReplicas` does not apply to plugin buckets; values above 1 need a clustered JetStream |
| `Storage` | `StorageType` | `FileStorage` | `FileStorage` (default) or `MemoryStorage` |
| `Compression` | `bool` | `false` | Enable S2 compression |
| `RecreateOnStorageMismatch` | `bool` | `false` | Recreate the bucket when its stream exists with another storage type: its keys are copied to a backup bucket, then **removed from the bucket**; see [Changing a Bucket's Storage Type](#changing-a-buckets-storage-type) |
| `SkipBackupOnRecreate` | `bool` | `false` | Recreate without a backup (**the keys are gone for good**) |
| `RecreateBackupTTL` | `time.Duration` | `0` (7 days) | How long the backup keeps the keys (at least 10 minutes); negative keeps them until you delete the backup |

The minimal configuration is a single bucket with a name:

```go
kvStore, _ := kvjetstream.New(kvjetstream.Config{
    Buckets: []kvjetstream.BucketConfig{
        {Name: "cache"},
    },
})
```

### Changing a Bucket's Storage Type

NATS cannot change the storage type (file or memory) of an existing stream. If a bucket's stream (`KV_<name>`) already exists with a different storage type than its `Storage`, for example because another client created the bucket first with its default file storage, `Start` fails with `ErrStorageMismatch`. The error names the bucket, both storage types and the stream. The bucket and its keys are left as they are. Because JetStream state survives restarts, the application cannot start until the stream is removed (`nats stream rm KV_<name>`, which deletes the bucket's keys).

For a bucket whose contents can be lost (locks, presence records, caches that their owners rebuild), set `RecreateOnStorageMismatch: true`. `Start` then copies the bucket to a backup bucket, deletes it, and creates it again, empty, with the configured storage type:

```go
{Name: "locks", Storage: kvjetstream.MemoryStorage, TTL: time.Minute, RecreateOnStorageMismatch: true}
```

{% hint style="danger" %}
**`RecreateOnStorageMismatch` empties the bucket: the application loses every key in it, with no confirmation.**

- **Without the option:** nothing is ever deleted. `Start` fails with `ErrStorageMismatch` instead.
- **With it:** the bucket is emptied whenever its stored storage type differs from the configured one. This includes changing `Storage` on purpose, in which case the next deploy empties the bucket.
- **The backup:** the old keys are kept in a backup bucket for 7 days by default, but the application doesn't read it, and restoring is manual (see below).
- **No backup:** with `SkipBackupOnRecreate`, or when the backup expires, the keys are gone for good.

Set the option only on buckets whose contents can be lost and are rebuilt by their owners (locks, presence, caches). Never set it on a bucket the application needs to keep reading. To move such a bucket to another storage type, copy its keys to a new bucket yourself and switch the application over.
{% endhint %}

**The backup.** Before deleting, `Start` copies the bucket into a new bucket named `<name>-backup-<UTC time>-<random>` (for example `locks-backup-20261119T101500Z-3f2a`):

- **Storage:** file storage, with the bucket's per-key history and replicas.
- **Retention:** the keys expire after `RecreateBackupTTL` (default 7 days; negative keeps them).
- **Completeness:** the delete happens only once the backup holds every message the bucket held when it was read (messages that expired before they were copied can't be). The backup is then detached, so it copies nothing the recreated bucket receives. Writes made to the old bucket after it was read, for example by instances of an older version still running during a rolling deploy, may or may not be in the backup.
- **Failure:** if the backup can't be made or doesn't finish within 2 minutes (a fixed limit whatever the bucket's size, and within the `Start` context), the partial backup is removed, nothing is deleted, and `Start` fails with `ErrStorageMismatch`. A bucket too large to copy in that time keeps failing `Start`: remove its stream by hand, or set `SkipBackupOnRecreate`. Because the copy runs inside `Start`, a repair can delay startup by up to 2 minutes: allow for it in orchestrator startup deadlines (for example a Kubernetes `startupProbe`, whose `failureThreshold` × `periodSeconds` should exceed it).
- **A message that was not copied:** before the delete the backup's message count is compared with the bucket's (since v0.2.19), when nothing was written to the bucket since it was read. A backup with fewer messages fails `Start` with `ErrStorageMismatch` and nothing is deleted. This catches a message the backup dropped as a duplicate: one that carries the `Nats-Msg-Id` of another message (the plugin's own writes carry none; another writer that sets and reuses one can cause it). Such a bucket cannot be backed up this way: remove its stream by hand or set `SkipBackupOnRecreate`. While other instances keep writing to the bucket the counts cannot be compared, and this check is skipped.
- **When there's none:** an empty bucket is recreated without a backup, and `SkipBackupOnRecreate` turns it off.
- **A backup left attached:** a recreate that was interrupted during the copy (the process was killed) leaves a backup bucket that still copies the bucket's stream. It would copy everything the recreated bucket receives. The next recreate of the bucket detaches such backups (since v0.2.18), once the bucket is recreated: each is logged at Warn with its name, and is kept, since it holds part of what the bucket held and may be incomplete. Only names of the form the plugin gives its backups, with this bucket's stream as their one source, are detached; a bucket of your own that copies this one is left alone. A leftover whose bucket is not recreated again keeps copying until you delete it.
- **Logged:** the Warn line names the backup bucket (`backup_bucket`).
- **Cleanup:** the plugin never removes a backup bucket. Its keys expire, but the empty bucket remains until you delete it (`nats kv del <backup>`).
- **Name length:** a bucket name longer than 211 characters leaves no room for the backup name; `New` rejects it unless `SkipBackupOnRecreate` is set.
- **Restore:** read the keys from the backup with the `nats` CLI (`nats kv ls <backup>`, `nats kv get <backup> <key>`) or a client, and write back the ones you need.

- **Only on a real mismatch:** the stream is inspected only after the bucket could not be created or opened, so a healthy start makes no extra request, and a bucket whose storage type matches is never touched.
- **Only that bucket's own keys are backed up:** the backup copies the stream `KV_<name>`, also for a bucket whose name itself starts with `KV_` (before v0.2.18 the backup of a bucket `KV_x` copied the bucket `x`, and `KV_x` was then deleted without a copy). A backup that copies another stream is never accepted as complete.
- **Only that bucket's stream:** `KV_<name>`, never any other stream. A `KV_<name>` stream that isn't a plain bucket of that name (a mirror, or other subjects) is never deleted: `Start` fails instead.
- **Same `Storage` everywhere:** two applications that share a bucket with different `Storage` values, and both set the option, delete each other's bucket on every start.
- **Concurrent starts:** when several instances start at once, an instance that finds the bucket already repaired by another one, during its copy or right before its delete, drops its backup and opens the repaired bucket. Keys written to the repaired bucket in the moment between that last check and the delete can still be lost: JetStream has no conditional delete.
- **Logged:** each delete is logged at Warn, with both storage types, the number of messages and bytes the stream held, and the backup bucket.
- **A delete lost to another instance is not an error:** a stream another instance already deleted counts as deleted.
- **Only when the storage change was the rejection:** a create that failed for another reason (for example too many replicas for the cluster) fails `Start` with `ErrStorageMismatch` and deletes nothing, since recreating would fail the same way.

### Keys

Keys must match `[-/_=.a-zA-Z0-9]+` (the NATS KV key rules; `:` and spaces are rejected with `nats: invalid key`). `.` separates the tokens that Watch patterns such as `user.*` match.

### StorageType

```go
const (
    FileStorage   StorageType = iota  // Disk-based (persistent, default)
    MemoryStorage                      // Memory-based (faster, not persistent)
)
```

### Plugin Options

```go
// Create with custom logger (myLogger is a *slog.Logger)
kvStore, err := kvjetstream.New(config, kvjetstream.WithLogger(myLogger))
```

Without `WithLogger` the plugin logs to `slog.Default()` as it is when the plugin starts (not when `New` is called), not to the framework logger.

## API Reference

### Plugin Methods

| Method | Description |
|--------|-------------|
| `New(config Config, opts ...Option) (*PluginModule, error)` | Create the plugin with the given bucket configurations |
| `Bucket(name string) KVStoragePort` | Get a bucket by name (returns nil if not found) |
| `Buckets() []string` | List all bucket names |
| `HasBucket(name string) bool` | Check if a bucket exists |

### KVStoragePort Interface

Consumer-facing interface for KV operations. It embeds interfaces from `pkg/storage` and adds `WatchAll`:

| Embedded interface | Methods | Purpose |
|--------------------|---------|---------|
| `storage.Storage` | Get, Set, Delete, Reset, Close | Base key-value operations |
| `storage.StorageWithBucket` | BucketName | Bucket identification |
| `storage.StorageWithWatch` | Watch, WatchWithContext | Real-time change notifications |
| `storage.StorageWithRevision` | Create, Update, Purge, GetEntry, PutWithRevision | Optimistic locking |
| `storage.StorageWithKeys` | Keys, KeysWithContext | Key enumeration |
| `storage.StorageWithStatus` | Status, StatusWithContext | Bucket status information |

Most methods come in a plain and a `...WithContext` variant; see [Context Variants Pattern](../api/storage.md#context-variants-pattern).

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
    GetEntry(key string) (*KVEntry, error)
    GetEntryWithContext(ctx context.Context, key string) (*KVEntry, error)
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
| `DeleteOption` | `storage.DeleteOption` | Delete options (`WithDeleteRevision`); not accepted by any method yet |
| `DeleteOptions` | `storage.DeleteOptions` | Delete options struct; not accepted by any method yet |

### KVEntry (alias of storage.Entry)

Metadata and value returned for stored keys:

```go
type Entry struct { // storage.Entry
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

### Sentinel Errors

The plugin provides sentinel errors for programmatic error handling. They are aliases of the `pkg/storage` errors, so `errors.Is(err, storage.ErrKeyNotFound)` and `errors.Is(err, kvjetstream.ErrKeyNotFound)` are interchangeable:

```go
var (
    ErrKeyNotFound      = storage.ErrKeyNotFound
    ErrKeyExists        = storage.ErrKeyExists
    ErrRevisionMismatch = storage.ErrRevisionMismatch
    ErrBucketNotFound   = storage.ErrBucketNotFound
    ErrStorageMismatch  = errors.New("kv bucket storage type mismatch")
)
```

`ErrBucketNotFound` is declared for backends; the plugin never returns it, and `Bucket(name)` returns nil for an unknown bucket. `ErrStorageMismatch` is returned by `Start` (see [Changing a Bucket's Storage Type](#changing-a-buckets-storage-type)).

### Missing Keys

Missing keys: `Get` returns `nil, nil`; `GetEntry` returns an error wrapping `ErrKeyNotFound`; `Update` returns an error wrapping `ErrRevisionMismatch` (use `Create` to write a key that may not exist); `Delete` and `Purge` of a missing key are not errors.

## Usage Examples

### Basic CRUD Operations

```go
// Set (create or overwrite) - simple storage, no revision returned
err := bucket.Set("user.123", []byte(`{"name":"Alice"}`), 0) // 0 = no expiration
if err != nil {
    return err
}

// PutWithRevision - when you need the revision number for optimistic locking
rev, err := bucket.PutWithRevision("user.123", []byte(`{"name":"Alice"}`), 0)
if err != nil {
    return err
}
fmt.Printf("Stored with revision: %d\n", rev)

// Get - returns value only
data, err := bucket.Get("user.123")
if err != nil {
    return err
}
fmt.Printf("Value: %s\n", string(data))

// GetEntry - returns value with revision metadata
entry, err := bucket.GetEntry("user.123")
if err != nil {
    return err
}
fmt.Printf("Value: %s, Revision: %d\n", string(entry.Value), entry.Revision)
```

To remove a key, use one of:

```go
// Delete (soft delete - leaves tombstone). A key that does not exist is
// left alone: no tombstone is written for it (since v0.2.19).
err = bucket.Delete("user.123")

// Or Purge (hard delete - removes all history)
err = bucket.Purge("user.123")
```

### Atomic Create (Fails if Key Exists)

```go
// Create only succeeds if key doesn't exist
// Signature: Create(key string, val []byte, exp time.Duration) (uint64, error)
rev, err := bucket.Create("lock.resource", []byte("owner-123"), 0)
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
if errors.Is(err, kvjetstream.ErrKeyNotFound) {
    // First use: create the counter instead
    _, err = bucket.Create("counter", []byte("1"), 0)
    return err
}
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

`KeysWithContext(ctx)` returns an error and no keys when `ctx` ends before every key was listed (since v0.2.18; before, it returned the keys listed so far and a nil error, which a caller could take for the whole bucket).

### Watch for Changes

```go
// Watch all keys
watcher, err := bucket.WatchAll(ctx)
if err != nil {
    return err
}
defer watcher.Stop()

for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case entry, ok := <-watcher.Updates():
        if !ok {
            return nil // channel closed: watcher stopped
        }
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
    m.cache.Set("config.app", configData, 0)
    m.sessions.Set("sess.user123", sessionData, 0) // expiry comes from the bucket's TTL

    return nil
}
```

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

## How It Works

The plugin uses a hexagonal (ports & adapters) architecture:

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
storage.Storage (+ StorageWith*) ← Backend contract from pkg/storage
     │
     │ implemented by
     ▼
JetStreamKVBackend (struct)     ← JetStream KV implementation
```

This design allows for future alternative backends (Redis, etc.) without changing consumer code.

## Related Documentation

- [Storage API](../api/storage.md) - Unified storage interface documentation
- [Storage Backends](../guides/storage-backends.md) - Choosing between kv-jetstream and fs-jetstream
- [Plugin System Overview](README.md)
- [File Storage Plugin (fs-jetstream)](fs-jetstream.md)
- [Creating Custom Plugins](creating-plugins.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
