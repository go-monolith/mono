# File Storage Plugin (fs-jetstream)

This page shows how to store files and binary objects with the `fs-jetstream` plugin: setting it up, configuring buckets, and the operations a module can call. Read it when your modules need documents, media files or other large binary objects. For small values, change notifications or optimistic locking, use [kv-jetstream](kv-jetstream.md) instead; [Storage Backends](../guides/storage-backends.md#backend-comparison) compares the two.

The plugin stores objects in NATS JetStream ObjectStore, with support for TTL-based expiration and compression.

## Overview

The `fs-jetstream` plugin implements bucket-based file storage capabilities using JetStream ObjectStore as the backend. Key features include:

- **Multiple Bucket Support**: Organize files into separate buckets with independent configurations
- **Flexible Storage**: Choose between disk-based (persistent) and memory-based (fast) storage
- **TTL Support**: Automatically expire objects after a bucket-level duration (`BucketConfig.TTL`)
- **Compression**: Optional S2 compression for space efficiency
- **Large File Support**: Streaming API for files larger than memory
- **Metadata Headers**: Attach custom metadata to stored objects
- **Object Information**: Query file sizes, modification times, and hashes

## Quick Start

### 1. Create and Register the Plugin

Import the plugin as `fsjetstream "github.com/go-monolith/mono/plugin/fs-jetstream"`. The application must enable JetStream:

```go
package main

import (
    "context"
    "time"

    mono "github.com/go-monolith/mono"
    fsjetstream "github.com/go-monolith/mono/plugin/fs-jetstream"
)

func main() {
    ctx := context.Background()

    // Create application with JetStream enabled
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamStorageDir("./data/jetstream"),
    )
    if err != nil {
        panic(err)
    }

    // Create storage plugin with bucket configurations
    storage, err := fsjetstream.New(fsjetstream.Config{
        Buckets: []fsjetstream.BucketConfig{
            {
                Name:        "documents",
                Description: "Document storage",
                MaxBytes:    500 * 1024 * 1024,  // 500MB limit
                Replicas:    1,
            },
            {
                Name:    "uploads",
                TTL:     24 * time.Hour,  // Auto-expire after 24 hours
                Storage: fsjetstream.MemoryStorage,
            },
        },
    })
    if err != nil {
        panic(err)
    }

    // Register plugin with an alias
    if err := app.RegisterPlugin(storage, "storage"); err != nil {
        panic(err)
    }

    // Register modules that use storage
    if err := app.Register(&DocumentModule{}); err != nil {
        panic(err)
    }

    if err := app.Start(ctx); err != nil {
        panic(err)
    }
}
```

### 2. Create a Consumer Module

```go
type DocumentModule struct {
    storage *fsjetstream.PluginModule
    docs    fsjetstream.FileStoragePort
}

func (m *DocumentModule) Name() string { return "documents" }

// Receive plugin instance
func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storage = plugin.(*fsjetstream.PluginModule)
    }
}

func (m *DocumentModule) Start(ctx context.Context) error {
    // Get bucket from plugin
    m.docs = m.storage.Bucket("documents")
    if m.docs == nil {
        return fmt.Errorf("bucket 'documents' not found")
    }

    // Store a file (Put returns ObjectInfo with metadata)
    info, err := m.docs.Put(ctx, "hello.txt", []byte("Hello, World!"))
    if err != nil {
        return err
    }
    fmt.Printf("Stored: %s (%d bytes)\n", info.Name, info.Size)

    // Or use Set for simple storage (from embedded storage.Storage)
    err = m.docs.Set("simple.txt", []byte("Simple content"), 0) // 0 = no expiration
    if err != nil {
        return err
    }

    // Retrieve the file
    data, err := m.docs.Get("hello.txt")
    if err != nil {
        return err
    }
    fmt.Printf("Retrieved: %s\n", string(data))

    return nil
}

func (m *DocumentModule) Stop(ctx context.Context) error {
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
| `MaxBytes` | `int64` | `0` (unlimited) | Maximum total size of all objects (0 = unlimited) |
| `TTL` | `time.Duration` | `0` (no expiry) | Time-to-live for objects (0 = no expiry). This is the only expiry: the `exp` argument of `Set`/`PutReader` is ignored, because JetStream ObjectStore has no per-object TTL |
| `Replicas` | `int` | `0` (means 1) | Number of replicas for replication (1-5; 0 means 1). The framework's `WithJetStreamDefaultReplicas` does not apply to plugin buckets; values above 1 need a clustered JetStream |
| `Storage` | `StorageType` | `FileStorage` | `FileStorage` (default) or `MemoryStorage` |
| `Compression` | `bool` | `false` | Enable S2 compression |
| `RecreateOnStorageMismatch` | `bool` | `false` | Recreate the bucket when its stream exists with another storage type: its objects are copied to a backup bucket, then **removed from the bucket**; see [Changing a Bucket's Storage Type](#changing-a-buckets-storage-type) |
| `SkipBackupOnRecreate` | `bool` | `false` | Recreate without a backup (**the objects are gone for good**) |
| `RecreateBackupTTL` | `time.Duration` | `0` (7 days) | How long the backup keeps the objects (at least 1 hour, and longer than the copy timeout); negative keeps them until you delete the backup |
| `RecreateBackupTimeout` | `time.Duration` | `0` (10 minutes) | How long the copy into the backup may take |

The minimal configuration is a single bucket with a name:

```go
storage, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {Name: "documents"},
    },
})
```

### Changing a Bucket's Storage Type

NATS cannot change the storage type (file or memory) of an existing stream. A bucket's stream (`OBJ_<name>`) can already exist with a different storage type than its `Storage`, for example because another client created the bucket first with its default file storage. `Start` then fails with `ErrStorageMismatch`:

- **The error:** it names the bucket, both storage types and the stream.
- **The bucket:** its objects are left as they are.
- **Recovery:** JetStream state survives restarts, so the application can't start until the stream is removed (`nats stream rm OBJ_<name>`, which deletes the bucket's objects).

For a bucket whose contents can be lost (caches, files their owners can produce again), set `RecreateOnStorageMismatch: true`. `Start` then copies the bucket to a backup bucket, deletes it, and creates it again, empty, with the configured storage type:

```go
{Name: "thumbnails", Storage: fsjetstream.MemoryStorage, RecreateOnStorageMismatch: true}
```

{% hint style="danger" %}
**`RecreateOnStorageMismatch` empties the bucket: the application loses every object in it, with no confirmation.**

- **Without the option:** nothing is ever deleted. `Start` fails with `ErrStorageMismatch` instead.
- **With it:** the bucket is emptied whenever its stored storage type differs from the configured one. This includes changing `Storage` on purpose, in which case the next deploy empties the bucket.
- **The backup:** the old objects are kept in a backup bucket for 7 days by default, but the application doesn't read it, and restoring is manual (see below).
- **No backup:** with `SkipBackupOnRecreate`, or when the backup expires, the objects are gone for good.

Set the option only on buckets whose contents can be lost. Never set it on a bucket that holds files you need: to move such a bucket to another storage type, copy its objects to a new bucket yourself and switch the application over.
{% endhint %}

**The backup.** Before deleting, `Start` copies the bucket into a new object store bucket named `<name>-backup-<UTC time>-<random>` (for example `thumbnails-backup-20261119T101500Z-3f2a9c01d4e5b6a7`):

- **A bucket of its own:** the backup is a complete object store. Objects are read back with the normal API or the `nats` CLI, with their names, metadata and digests intact. After the copy, each object's metadata is rewritten to name the backup bucket, so deleting or updating an object in the backup never touches the recreated bucket. A link to an object of the same bucket is pointed at the backup too (the object it names is copied there); links to other buckets still point where they did.
- **Storage:** file storage, with the bucket's replicas, placement and compression.
- **Space:** it needs as much storage as the bucket itself.
- **Retention:** the objects expire after `RecreateBackupTTL` (default 7 days; at least 1 hour and longer than the copy timeout; negative keeps them). An object's chunks are dated from the copy and its metadata from slightly later, so in the last minutes of the TTL an object can still be listed while its content is already gone: copy what you need well before the backup expires.
- **Completeness:** the delete happens only once the backup holds every message the bucket held when it was read. Messages that expired before they were copied can't be. The backup is then detached, so it copies nothing the recreated bucket receives. Writes made to the old bucket after it was read, for example by instances of an older version still running during a rolling deploy, may or may not be in the backup.
- **Copy time:** the copy runs inside `Start` and is bounded by `RecreateBackupTimeout` (default 10 minutes) and the `Start` context. Allow for it in orchestrator startup deadlines, for example a Kubernetes `startupProbe`.
- **Failure:** if the backup can't be made or doesn't finish in time, the partial backup is removed, nothing is deleted, and `Start` fails with `ErrStorageMismatch`. A bucket too large to copy in that time keeps failing `Start`: raise `RecreateBackupTimeout`, remove its stream by hand, or set `SkipBackupOnRecreate`.
- **When there's none:** an empty bucket is recreated without a backup, and `SkipBackupOnRecreate` turns it off.
- **Logged:** the Warn line names the backup bucket (`backup_bucket`).
- **Cleanup:** the plugin never removes a backup bucket. Its objects expire, but the empty bucket remains until you delete it (`nats object del <backup>`).
- **Name length:** a bucket name longer than 210 characters leaves no room for the backup name; `New` rejects it unless `SkipBackupOnRecreate` is set.
- **Restore:** list the backup with `nats object ls <backup>`, fetch objects with `nats object get <backup> <name>` (or the `jetstream.ObjectStore` API), and put back the ones you need.

**Other guarantees:**
- **Only on a real mismatch:** the stream is inspected only after the bucket could not be created or opened, so a healthy start makes no extra request. A bucket whose storage type matches is never touched.
- **Only that bucket's stream:** `OBJ_<name>`, never any other stream. A stream that isn't a plain object store of that name (a mirror, or other subjects) is never deleted: `Start` fails instead.
- **Only when the storage change was the rejection:** a create that failed for another reason (for example too many replicas for the cluster) fails `Start` with `ErrStorageMismatch` and deletes nothing.
- **Same `Storage` everywhere:** two applications that share a bucket with different `Storage` values, and both set the option, delete each other's bucket on every start.
- **A backup left attached:** a recreate that was interrupted during the copy (the process was killed) leaves a backup bucket that still copies the bucket's stream, and would copy everything the recreated bucket receives. The next recreate of the bucket detaches such backups (since v0.2.18): each is logged at Warn with its name, and is kept. It may be incomplete, and the last step of a backup, which rewrites the object metadata to name the backup bucket, did not run for it. Only names of the form the plugin gives its backups, with this bucket's stream as their one source, are detached. A leftover whose bucket is not recreated again keeps copying until you delete it.
- **Concurrent starts:** when several instances start at once, an instance that finds the bucket already repaired by another one, during its copy or right before its delete, drops its backup and opens the repaired bucket. Objects written to the repaired bucket in the moment between that last check and the delete can still be lost: JetStream has no conditional delete.

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
storage, err := fsjetstream.New(config, fsjetstream.WithLogger(myLogger))
```

Without `WithLogger` the plugin logs to `slog.Default()` as it is when the plugin starts (not when `New` is called), not to the framework logger. `WithLogger(nil)` is rejected by `New`.

## API Reference

### Plugin Methods

| Method | Description |
|--------|-------------|
| `New(config Config, opts ...Option) (*PluginModule, error)` | Create the plugin with the given bucket configurations |
| `Bucket(name string) FileStoragePort` | Get a bucket by name (returns nil if not found) |
| `Buckets() []string` | List all bucket names |
| `HasBucket(name string) bool` | Check if a bucket exists |

### FileStoragePort Interface

Consumer-facing interface for storage operations. It embeds interfaces from `pkg/storage` and adds `Put`:

| Embedded interface | Methods | Purpose |
|--------------------|---------|---------|
| `storage.Storage` | Get, Set, Delete, Reset, Close | Base key-value operations |
| `storage.StorageWithBucket` | BucketName | Bucket identification |
| `storage.StorageWithList` | List, ListWithContext | Object enumeration with filtering |
| `storage.StorageWithStat` | Stat, StatWithContext | Object metadata retrieval |
| `storage.StorageWithReader` | GetReader, PutReader (+ context) | Streaming for large files |

Most methods come in a plain and a `...WithContext` variant; see [Context Variants Pattern](../api/storage.md#context-variants-pattern).

```go
type FileStoragePort interface {
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

    // From storage.StorageWithList
    List(opts ...ListOption) ([]ObjectInfo, error)
    ListWithContext(ctx context.Context, opts ...ListOption) ([]ObjectInfo, error)

    // From storage.StorageWithStat
    Stat(key string) (*ObjectInfo, error)
    StatWithContext(ctx context.Context, key string) (*ObjectInfo, error)

    // From storage.StorageWithReader (streaming for large files)
    GetReader(key string) (io.ReadCloser, *ObjectInfo, error)
    GetReaderWithContext(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
    PutReader(key string, reader io.Reader, exp time.Duration, opts ...PutOption) (*ObjectInfo, error)
    PutReaderWithContext(ctx context.Context, key string, reader io.Reader, exp time.Duration, opts ...PutOption) (*ObjectInfo, error)

    // FileStoragePort-specific (returns ObjectInfo, unlike base Set)
    Put(ctx context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error)
}
```

{% hint style="info" %}
**Put vs Set**: Use `Put()` when you need the returned `ObjectInfo` metadata; it always takes a context. Use `Set()` for simple storage without metadata return. Both store the data identically.
{% endhint %}

### Type Aliases

The plugin re-exports types from `pkg/storage` for convenience:

| Plugin Type | Source | Description |
|-------------|--------|-------------|
| `ObjectInfo` | `storage.ObjectInfo` | Object metadata (size, hash, modtime) |
| `ListOption` | `storage.ListOption` | List filtering options |
| `ListOptions` | `storage.ListOptions` | List options struct |
| `PutOption` | `storage.PutOption` | Put configuration options |
| `PutOptions` | `storage.PutOptions` | Put options struct |

### ObjectInfo

Metadata returned for stored objects:

```go
type ObjectInfo struct {
    Bucket      string            // Bucket name
    Name        string            // Object key
    Size        int64             // Size in bytes
    Digest      string            // SHA-256 digest as reported by ObjectStore ("SHA-256=<base64>")
    ModTime     time.Time         // Last modified
    Deleted     bool              // Deletion marker
    Headers     map[string]string // Custom headers
    Description string            // Object description
    Chunks      uint32            // Number of chunks
    Revision    uint64            // Always 0 for fs-jetstream
}
```

### Put and List Options

```go
// Set object description
fsjetstream.WithDescription("My document")

// Set custom headers
fsjetstream.WithHeaders(map[string]string{
    "Content-Type": "application/pdf",
    "Author":       "John Doe",
})

// Filter a listing by prefix
fsjetstream.WithPrefix("reports/2024/")
```

### Missing Objects

`Get` returns `nil, nil` for a missing object; `Stat` and `GetReader` return an error wrapping `storage.ErrKeyNotFound` (check with `errors.Is`). `Delete` of a missing object is not an error.

## Usage Examples

### Store and Retrieve Files

```go
// Store from bytes (with ObjectInfo return)
info, err := bucket.Put(ctx, "file.txt", []byte("content"))
if err != nil {
    return err
}
fmt.Printf("Stored: %s (%d bytes)\n", info.Name, info.Size)

// Store from reader (for large files)
// Signature: PutReader(key string, reader io.Reader, exp time.Duration, opts ...PutOption)
file, err := os.Open("large-file.zip")
if err != nil {
    return err
}
defer file.Close()

info, err = bucket.PutReader("large-file.zip", file, 0) // 0 = no expiration
if err != nil {
    return err
}

// Retrieve as bytes
data, err := bucket.Get("file.txt")
if err != nil {
    return err
}

// Retrieve as reader (for large files)
reader, info, err := bucket.GetReader("large-file.zip")
if err != nil {
    return err
}
defer reader.Close()
io.Copy(os.Stdout, reader)
```

### Store with Metadata

```go
info, err := bucket.Put(ctx, "report.pdf", pdfData,
    fsjetstream.WithDescription("Q4 2024 Report"),
    fsjetstream.WithHeaders(map[string]string{
        "Content-Type": "application/pdf",
        "Department":   "Finance",
    }),
)
```

### List and Filter Objects

```go
// List all objects
objects, err := bucket.List()
if err != nil {
    return err
}

// List with prefix filter
objects, err = bucket.List(fsjetstream.WithPrefix("reports/"))
if err != nil {
    return err
}

for _, obj := range objects {
    fmt.Printf("%s: %d bytes\n", obj.Name, obj.Size)
}
```

### Check Object Metadata

```go
info, err := bucket.Stat("file.txt")
if err != nil {
    // Object not found
    return err
}
fmt.Printf("Size: %d, Modified: %s, Hash: %s\n",
    info.Size, info.ModTime, info.Digest)
```

### Delete Objects

```go
err := bucket.Delete("file.txt")
if err != nil {
    return err
}
```

### Using Multiple Buckets

```go
type MediaModule struct {
    storage    *fsjetstream.PluginModule
    uploads    fsjetstream.FileStoragePort
    thumbnails fsjetstream.FileStoragePort
}

func (m *MediaModule) Start(ctx context.Context) error {
    m.uploads = m.storage.Bucket("uploads")
    m.thumbnails = m.storage.Bucket("thumbnails")

    // Use different buckets for different purposes
    // Put returns ObjectInfo, useful for tracking metadata
    m.uploads.Put(ctx, "video.mp4", videoData)
    m.thumbnails.Put(ctx, "video-thumb.jpg", thumbData)

    // Or use Set for simple storage (from embedded storage.Storage)
    m.uploads.Set("video.mp4", videoData, 0) // expiry comes from the bucket's TTL

    return nil
}
```

## Best Practices

✓ **Do**
- Use file storage for large objects (>1MB)
- Set appropriate MaxBytes limits per bucket
- Use TTL for temporary files (uploads, cache)
- Attach meaningful descriptions to important objects
- Use separate buckets for different object types
- Implement cleanup logic for stale objects

✗ **Don't**
- Store small values that would be better in KV store
- Store uncompressible binary data with compression enabled
- Set very large replication counts without reason
- Leave temporary uploads without TTL expiration
- Store sensitive data without encryption (use NATS TLS)

## Uploads That Are Given Up

An object is stored as chunk messages followed by a metadata message. Chunks that no metadata refers to are invisible (`List`, `Delete` and `Reset` do not see them) but count towards `MaxBytes`.

- **A `Put` whose context is cancelled or times out leaves no chunks** (since v0.2.17). The upload is stopped at the next read and what it had written is removed. `Put` then returns the context's error (`context.Canceled`, `context.DeadlineExceeded`). Removing the chunks takes a request of its own, so a `Put` can return shortly after its deadline.
- **An upload whose data was read completely is finished**, also when its context ends in that moment.
- **Uploads and deletes of one name are serialised** within one application instance. Of several concurrent uploads of one name only the last is kept, and a `Delete` waits for an upload of that name (it gives up with its own context). A reader that blocks holds the name until its `Read` returns; calls without a context (`Delete(key)`, `Reset()`) then wait for as long as that takes.
- **A `Delete` that has begun is finished**, also when its context ends meanwhile: it marks the object deleted and then removes the chunks, in two requests. A `Delete` whose context has already ended deletes nothing.
- **Not covered:** uploads of one name from several instances at once, and an instance that dies during an upload. Both can leave chunks behind.
- **Chunks left behind earlier are not removed automatically**: they cannot be told from the chunks of an upload another instance is making. Up to v0.2.16 every cancelled or timed-out `Put` left its chunks, and a bucket with `MaxBytes` could fill up with them until every `Put` failed with `maximum bytes exceeded`. To reclaim the space, compare the bucket's stream (`OBJ_<bucket>`) with its objects while nothing is uploading, or move the objects to a new bucket.

## How It Works

The plugin uses a hexagonal (ports & adapters) architecture:

```
Consumer Module
     │
     │ uses
     ▼
FileStoragePort (interface)     ← Consumer-facing interface
     │
     │ implemented by
     ▼
FileStorageAdapter (struct)     ← Wraps backend, can add logging
     │
     │ wraps
     ▼
storage.Storage (+ StorageWith*) ← Backend contract from pkg/storage
     │
     │ implemented by
     ▼
JetStreamBackend (struct)       ← JetStream ObjectStore implementation
```

This design allows for future alternative backends (S3, Azure, etc.) without changing consumer code.

## Related Documentation

- [Storage API](../api/storage.md) - Unified storage interface documentation
- [Storage Backends](../guides/storage-backends.md) - Choosing between fs-jetstream and kv-jetstream
- [Plugin System Overview](README.md)
- [Key-Value Storage Plugin (kv-jetstream)](kv-jetstream.md)
- [Creating Custom Plugins](creating-plugins.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
