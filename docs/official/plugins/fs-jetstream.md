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

The minimal configuration is a single bucket with a name:

```go
storage, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {Name: "documents"},
    },
})
```

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

Without `WithLogger` the plugin logs to `slog.Default()`, not to the framework logger.

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
