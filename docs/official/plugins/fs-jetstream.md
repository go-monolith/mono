# File Storage Plugin (fs-jetstream)

The File Storage Plugin provides persistent file and object storage using NATS JetStream ObjectStore. It's ideal for storing documents, media files, and large binary objects with support for TTL-based expiration and compression.

## Overview

The `fs-jetstream` plugin implements bucket-based file storage capabilities using JetStream ObjectStore as the backend. Key features include:

- **Multiple Bucket Support**: Organize files into separate buckets with independent configurations
- **Flexible Storage**: Choose between disk-based (persistent) and memory-based (fast) storage
- **TTL Support**: Automatically expire objects after a specified duration
- **Compression**: Optional S2 compression for space efficiency
- **Large File Support**: Streaming API for files larger than memory
- **Metadata Headers**: Attach custom metadata to stored objects
- **Object Information**: Query file sizes, modification times, and hashes

## Architecture

The plugin uses a hexagonal (ports & adapters) architecture with clean separation:

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
FileStorageBackend (interface)  ← Internal backend interface
     │
     │ implemented by
     ▼
JetStreamBackend (struct)       ← JetStream ObjectStore implementation
```

This design allows for future alternative backends (S3, Azure, etc.) without changing consumer code.

## Interface Hierarchy

The `FileStoragePort` interface embeds multiple interfaces from the `pkg/storage` package, providing a unified storage abstraction:

```
FileStoragePort
     │
     ├── storage.Storage (base)
     │   └── Get, Set, Delete, Reset, Close (+ context variants)
     │
     ├── storage.StorageWithBucket
     │   └── BucketName()
     │
     ├── storage.StorageWithList
     │   └── List, ListWithContext
     │
     ├── storage.StorageWithStat
     │   └── Stat, StatWithContext
     │
     ├── storage.StorageWithReader
     │   └── GetReader, PutReader (+ context variants)
     │
     └── Put (FileStoragePort-specific, returns ObjectInfo)
```

### Embedded Interfaces

| Interface | Methods | Purpose |
|-----------|---------|---------|
| `storage.Storage` | Get, Set, Delete, Reset, Close | Base key-value operations |
| `storage.StorageWithBucket` | BucketName | Bucket identification |
| `storage.StorageWithList` | List, ListWithContext | Object enumeration with filtering |
| `storage.StorageWithStat` | Stat, StatWithContext | Object metadata retrieval |
| `storage.StorageWithReader` | GetReader, PutReader (+ context) | Streaming for large files |

### Type Aliases

The plugin re-exports types from `pkg/storage` for convenience:

| Plugin Type | Source | Description |
|-------------|--------|-------------|
| `ObjectInfo` | `storage.ObjectInfo` | Object metadata (size, hash, modtime) |
| `ListOption` | `storage.ListOption` | List filtering options |
| `ListOptions` | `storage.ListOptions` | List options struct |
| `PutOption` | `storage.PutOption` | Put configuration options |
| `PutOptions` | `storage.PutOptions` | Put options struct |

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
data, err := bucket.Get("file.txt")

// Context-aware usage (explicit timeout/cancellation)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
data, err := bucket.GetWithContext(ctx, "large-file.zip")
```

{% hint style="info" %}
**Note**: The `Put()` method always requires a context parameter because it returns `ObjectInfo` metadata. Use `Set()` for simple storage without metadata return.
{% endhint %}

## Installation

Import the plugin:

```go
import "github.com/go-monolith/mono/plugin/fs-jetstream"
```

## Signatures

### New

```go
func New(config Config, opts ...Option) (*PluginModule, error)
```

Creates a new file storage plugin with the given bucket configurations.

### Bucket Methods

```go
func (p *PluginModule) Bucket(name string) FileStoragePort
func (p *PluginModule) Buckets() []string
func (p *PluginModule) HasBucket(name string) bool
```

Access configured storage buckets.

## Quick Start

### 1. Create and Register the Plugin

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
        mono.WithJetStreamDomain("default"),
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
    app.RegisterPlugin(storage, "storage")

    // Register modules that use storage
    app.Register(&DocumentModule{})

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

## Configuration

### BucketConfig

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Bucket name (required, must be unique) |
| `Description` | `string` | Optional bucket description |
| `MaxBytes` | `int64` | Maximum total size of all objects (0 = unlimited) |
| `TTL` | `time.Duration` | Time-to-live for objects (0 = no expiry) |
| `Replicas` | `int` | Number of replicas for replication (1-5, default 1) |
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
storage, err := fsjetstream.New(config, fsjetstream.WithLogger(myLogger))
```

## Default Config

```go
// Minimal configuration (single bucket)
storage, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {
            Name:     "documents",
            Storage:  fsjetstream.FileStorage,  // Persistent by default
            Replicas: 1,
        },
    },
})
```

## API Reference

### Module Methods

| Method | Description |
|--------|-------------|
| `Bucket(name string) FileStoragePort` | Get a bucket by name (returns nil if not found) |
| `Buckets() []string` | List all bucket names |
| `HasBucket(name string) bool` | Check if a bucket exists |

### FileStoragePort Interface

Consumer-facing interface for storage operations. The interface embeds `storage.Storage` for basic operations and additional interfaces for file-specific capabilities:

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
**Put vs Set**: Use `Put()` when you need the returned `ObjectInfo` metadata. Use `Set()` for simple storage without metadata return. Both store the data identically.
{% endhint %}

### ObjectInfo

Metadata returned for stored objects:

```go
type ObjectInfo struct {
    Bucket      string            // Bucket name
    Name        string            // Object key
    Size        int64             // Size in bytes
    Digest      string            // SHA-256 hash
    ModTime     time.Time         // Last modified
    Deleted     bool              // Deletion marker
    Headers     map[string]string // Custom headers
    Description string            // Object description
    Chunks      uint32            // Number of chunks
}
```

### Put Options

```go
// Set object description
fsjetstream.WithDescription("My document")

// Set custom headers
fsjetstream.WithHeaders(map[string]string{
    "Content-Type": "application/pdf",
    "Author":       "John Doe",
})
```

### List Options

```go
// Filter by prefix
fsjetstream.WithPrefix("reports/2024/")
```

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
    m.uploads.Set("video.mp4", videoData, 24*time.Hour) // expires in 24h

    return nil
}
```

## Plugin Lifecycle

1. **Startup**: Plugin starts FIRST (before middleware and regular modules)
2. **Bucket Creation**: All configured buckets are created/updated in JetStream
3. **Consumer Access**: Consumer modules receive plugin via `SetPlugin()`
4. **Shutdown**: Plugin stops LAST (after all modules and middleware)

## Performance Characteristics

- **Storage Overhead**: ~100 bytes per object metadata
- **Write Latency**: 1-10ms depending on object size and replication
- **Read Latency**: Sub-millisecond for small objects, limited by network for large files
- **Throughput**: Up to 10MB/s per connection
- **Replication**: 1-5 replicas supported for high availability

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

## Comparison with Other Storage Options

| Aspect | fs-jetstream | kv-jetstream | Regular Files |
|--------|------------|------------|---------------|
| Data Model | Objects (binary) | Key-value | Raw files |
| Max Size | Unlimited | ~1MB | Unlimited |
| Persistence | Yes (configurable) | Yes | Yes |
| Replication | Yes (1-5) | Yes (1-5) | No |
| Distributed | Yes | Yes | No |
| Watch/Notify | No | Yes | No |
| Use Case | Documents, media | Config, sessions | Local storage |

## Related Documentation

- [Storage API](../api/storage.md) - Unified storage interface documentation
- [Plugin System Overview](README.md)
- [Key-Value Storage Plugin (kv-jetstream)](kv-jetstream.md)
- [Creating Custom Plugins](creating-plugins.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For key-value storage needs, see [kv-jetstream](kv-jetstream.md). For creating your own plugins, see [Creating Plugins](creating-plugins.md).
