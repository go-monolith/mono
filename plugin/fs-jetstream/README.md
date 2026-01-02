# fs-jetstream

A file storage plugin for mono-framework using NATS JetStream ObjectStore as the backend.

## Overview

The `fs-jetstream` plugin provides bucket-based file storage capabilities using JetStream ObjectStore. It implements the `PluginModule` interface, allowing consumer modules to store and retrieve files with features like:

- Multiple bucket support with independent configurations
- File and memory storage options
- TTL-based automatic object expiration
- S2 compression support
- Custom metadata headers
- Streaming support for large files

## Architecture

The plugin follows a hexagonal architecture with clean port/adapter separation:

```
Consumer Module
     │
     │ uses
     ▼
FileStoragePort (interface)     ← Consumer-facing interface
     │
     │ implemented by
     ▼
FileStorageAdapter (struct)     ← Wraps backend, can add logging/metrics
     │
     │ wraps
     ▼
FileStorageBackend (interface)  ← Internal backend interface
     │
     │ implemented by
     ▼
JetStreamBackend (struct)       ← JetStream ObjectStore implementation
```

## Installation

Import the plugin in your application:

```go
import "github.com/go-monolith/mono/v1/plugin/fs-jetstream"
```

## Quick Start

### 1. Create and Register the Plugin

```go
package main

import (
    "context"
    "time"

    mono "github.com/go-monolith/mono/v1"
    fsjetstream "github.com/go-monolith/mono/v1/plugin/fs-jetstream"
)

func main() {
    ctx := context.Background()

    // Create the framework with JetStream enabled
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamDomain("default"),
    )
    if err != nil {
        panic(err)
    }

    // Create the storage plugin with bucket configurations
    storage, err := fsjetstream.New(fsjetstream.Config{
        Buckets: []fsjetstream.BucketConfig{
            {
                Name:        "documents",
                Description: "Document storage",
                MaxBytes:    500 * 1024 * 1024, // 500MB limit
                Replicas:    1,
            },
            {
                Name:    "uploads",
                TTL:     24 * time.Hour,        // Auto-expire after 24 hours
                Storage: fsjetstream.MemoryStorage,
            },
        },
    })
    if err != nil {
        panic(err)
    }

    // Register the plugin with an alias
    app.RegisterPlugin(storage, "storage")

    // Register consumer modules
    app.Register(&DocumentModule{})

    if err := app.Start(ctx); err != nil {
        panic(err)
    }
}
```

### 2. Create a Consumer Module

```go
type DocumentModule struct {
    storagePlugin *fsjetstream.PluginModule
    documents     fsjetstream.FileStoragePort
}

func (m *DocumentModule) Name() string { return "documents" }

// Receive plugin instance
func (m *DocumentModule) SetPlugin(alias string, plugin mono.PluginModule) {
    if alias == "storage" {
        m.storagePlugin = plugin.(*fsjetstream.PluginModule)
    }
}

func (m *DocumentModule) Start(ctx context.Context) error {
    // Get bucket from plugin
    m.documents = m.storagePlugin.Bucket("documents")
    if m.documents == nil {
        return fmt.Errorf("bucket 'documents' not found")
    }

    // Store a file
    info, err := m.documents.Put(ctx, "hello.txt", []byte("Hello, World!"))
    if err != nil {
        return err
    }
    fmt.Printf("Stored: %s (%d bytes)\n", info.Name, info.Size)

    // Retrieve the file
    data, err := m.documents.Get(ctx, "hello.txt")
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
| `MaxBytes` | `int64` | Maximum total size of all objects (0 = unlimited) |
| `TTL` | `time.Duration` | Time-to-live for objects (0 = no expiry) |
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
storage, err := fsjetstream.New(config, fsjetstream.WithLogger(myLogger))
```

## API Reference

### Module Methods

| Method | Description |
|--------|-------------|
| `Bucket(name string) FileStoragePort` | Get a bucket by name (returns nil if not found) |
| `Buckets() []string` | List all bucket names |
| `HasBucket(name string) bool` | Check if a bucket exists |

### FileStoragePort Interface

The consumer-facing interface for storage operations:

```go
type FileStoragePort interface {
    BucketName() string
    Put(ctx context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error)
    PutReader(ctx context.Context, key string, reader io.Reader, opts ...PutOption) (*ObjectInfo, error)
    Get(ctx context.Context, key string) ([]byte, error)
    GetReader(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, opts ...ListOption) ([]ObjectInfo, error)
    Stat(ctx context.Context, key string) (*ObjectInfo, error)
}
```

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
// Store from bytes
info, err := bucket.Put(ctx, "file.txt", []byte("content"))

// Store from reader (for large files)
file, _ := os.Open("large-file.zip")
defer file.Close()
info, err := bucket.PutReader(ctx, "large-file.zip", file)

// Retrieve as bytes
data, err := bucket.Get(ctx, "file.txt")

// Retrieve as reader (for large files)
reader, info, err := bucket.GetReader(ctx, "large-file.zip")
if err == nil {
    defer reader.Close()
    io.Copy(os.Stdout, reader)
}
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
objects, err := bucket.List(ctx)

// List with prefix filter
objects, err := bucket.List(ctx, fsjetstream.WithPrefix("reports/"))

for _, obj := range objects {
    fmt.Printf("%s: %d bytes\n", obj.Name, obj.Size)
}
```

### Check Object Metadata

```go
info, err := bucket.Stat(ctx, "file.txt")
if err != nil {
    // Object not found
}
fmt.Printf("Size: %d, Modified: %s\n", info.Size, info.ModTime)
```

### Delete Objects

```go
err := bucket.Delete(ctx, "file.txt")
```

### Using Multiple Buckets

```go
type MediaModule struct {
    storagePlugin *fsjetstream.PluginModule
    uploads       fsjetstream.FileStoragePort
    thumbnails    fsjetstream.FileStoragePort
}

func (m *MediaModule) Start(ctx context.Context) error {
    m.uploads = m.storagePlugin.Bucket("uploads")
    m.thumbnails = m.storagePlugin.Bucket("thumbnails")

    // Use different buckets for different purposes
    m.uploads.Put(ctx, "video.mp4", videoData)
    m.thumbnails.Put(ctx, "video-thumb.jpg", thumbData)

    return nil
}
```

### Multiple Plugin Instances

Register multiple instances with different aliases for different configurations:

```go
// Primary storage with replication
primary, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {Name: "documents", Replicas: 3},
    },
})

// Backup storage with file persistence
backup, _ := fsjetstream.New(fsjetstream.Config{
    Buckets: []fsjetstream.BucketConfig{
        {Name: "documents-backup", Storage: fsjetstream.FileStorage},
    },
})

app.RegisterPlugin(primary, "primary-storage")
app.RegisterPlugin(backup, "backup-storage")

// Consumer module can use both
type BackupModule struct {
    primary *fsjetstream.PluginModule
    backup  *fsjetstream.PluginModule
}

func (m *BackupModule) SetPlugin(alias string, plugin mono.PluginModule) {
    switch alias {
    case "primary-storage":
        m.primary = plugin.(*fsjetstream.PluginModule)
    case "backup-storage":
        m.backup = plugin.(*fsjetstream.PluginModule)
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
- **Port/Adapter Pattern**: Clean separation enables future backend implementations
- **Multi-Instance Support**: Multiple plugins with different configs via aliases
