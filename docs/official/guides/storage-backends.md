# Storage Backends Guide

This guide explains the unified storage interface pattern used by storage plugins and how to work with different storage backends effectively.

## Overview

The Monolith Framework provides a unified storage abstraction through the `pkg/storage` package. This allows you to write code that works with different storage backends (file storage, key-value storage) using a common interface pattern.

### GoFiber Storage Compatibility

The `mono.Storage` interface is **fully compatible** with the [GoFiber Storage](https://github.com/gofiber/storage) interface. This means you can use any of the 40+ storage implementations from the GoFiber ecosystem directly within this framework.

```go
// mono.Storage interface is identical to fiber.Storage
type Storage interface {
    Get(key string) ([]byte, error)
    GetWithContext(ctx context.Context, key string) ([]byte, error)
    Set(key string, val []byte, exp time.Duration) error
    SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error
    Delete(key string) error
    DeleteWithContext(ctx context.Context, key string) error
    Reset() error
    ResetWithContext(ctx context.Context) error
    Close() error
}
```

**Available GoFiber Storage Backends:**

| Category | Backends |
|----------|----------|
| **SQL Databases** | MySQL, PostgreSQL, SQLite3, MSSQL |
| **NoSQL** | MongoDB, Couchbase, DynamoDB |
| **Key-Value Stores** | Redis, Valkey, Memcache, BadgerDB, Bbolt, Pebble |
| **Cloud Storage** | S3, Azure Blob, Cloudflare KV |
| **Embedded** | Ristretto, memory, rueidis |

**Example: Using Redis from GoFiber:**

```go
import (
    "github.com/gofiber/storage/redis/v3"
    "github.com/go-monolith/mono/pkg/storage"
)

// Create a Redis storage from gofiber
redisStore := redis.New(redis.Config{
    Host:     "localhost",
    Port:     6379,
    Database: 0,
})

// Use it anywhere mono.Storage is expected
var s storage.Storage = redisStore

// Works seamlessly
s.Set("key", []byte("value"), time.Hour)
```

For the complete list of available backends and their configuration options, visit the [GoFiber Storage repository](https://github.com/gofiber/storage).

### Key Benefits

The key benefits of this design:
- **Consistent API**: Same methods across different storage backends
- **Capability Detection**: Safely check what features a backend supports
- **Future-Proof**: New backends can be added without changing consumer code
- **Type Safety**: Compile-time interface checks prevent runtime errors

## Interface Hierarchy

The storage package defines a base `Storage` interface and several extension interfaces:

```
Storage (base)
     │
     ├── StorageWithBucket      ← Bucket identification
     ├── StorageWithList        ← Object enumeration
     ├── StorageWithStat        ← Metadata retrieval
     ├── StorageWithReader      ← Streaming I/O for large files
     ├── StorageWithWatch       ← Real-time notifications
     ├── StorageWithRevision    ← Optimistic locking
     ├── StorageWithKeys        ← Key enumeration
     └── StorageWithStatus      ← Storage status info
```

### Base Storage Interface

All storage backends implement the base `Storage` interface:

```go
type Storage interface {
    Get(key string) ([]byte, error)
    GetWithContext(ctx context.Context, key string) ([]byte, error)
    Set(key string, val []byte, exp time.Duration) error
    SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error
    Delete(key string) error
    DeleteWithContext(ctx context.Context, key string) error
    Reset() error
    ResetWithContext(ctx context.Context) error
    Close() error
}
```

### Extended Interfaces

| Interface | Purpose | Methods |
|-----------|---------|---------|
| `StorageWithBucket` | Bucket identification | `BucketName()` |
| `StorageWithList` | Object enumeration with filtering | `List()`, `ListWithContext()` |
| `StorageWithStat` | Metadata retrieval without data | `Stat()`, `StatWithContext()` |
| `StorageWithReader` | Streaming for large files | `GetReader()`, `PutReader()` |
| `StorageWithWatch` | Real-time change notifications | `Watch()`, `WatchWithContext()` |
| `StorageWithRevision` | Optimistic locking via revisions | `Create()`, `Update()`, `GetEntry()`, `PutWithRevision()`, `Purge()` |
| `StorageWithKeys` | Key enumeration | `Keys()`, `KeysWithContext()` |
| `StorageWithStatus` | Storage status information | `Status()`, `StatusWithContext()` |

## Capability Detection

### The If-Ok Pattern

Use type assertions to safely check if a storage backend supports specific capabilities:

```go
import "github.com/go-monolith/mono/pkg/storage"

func processStorage(s storage.Storage) error {
    // Check if storage supports listing
    if lister, ok := s.(storage.StorageWithList); ok {
        objects, err := lister.List()
        if err != nil {
            return err
        }
        fmt.Printf("Found %d objects\n", len(objects))
    }

    // Check if storage supports watching
    if watcher, ok := s.(storage.StorageWithWatch); ok {
        w, err := watcher.Watch("user.*")
        if err != nil {
            return err
        }
        defer w.Stop()
        // Handle updates...
    }

    // Check if storage supports revision-based locking
    if revStore, ok := s.(storage.StorageWithRevision); ok {
        entry, err := revStore.GetEntry("my-key")
        if err != nil {
            return err
        }
        fmt.Printf("Key revision: %d\n", entry.Revision)
    }

    return nil
}
```

### Checking Multiple Capabilities

When you need multiple capabilities, chain the checks:

```go
func requireAdvancedStorage(s storage.Storage) error {
    // Check for bucket awareness
    bucket, hasBucket := s.(storage.StorageWithBucket)
    if !hasBucket {
        return fmt.Errorf("storage does not support bucket identification")
    }

    // Check for status reporting
    statusProvider, hasStatus := s.(storage.StorageWithStatus)
    if !hasStatus {
        return fmt.Errorf("storage does not support status reporting")
    }

    // Use the capabilities
    fmt.Printf("Working with bucket: %s\n", bucket.BucketName())

    status, err := statusProvider.Status()
    if err != nil {
        return err
    }
    fmt.Printf("Bucket has %d values using %d bytes\n", status.Values, status.Bytes)

    return nil
}
```

## Backend Comparison

### Implemented Interfaces by Backend

| Interface | fs-jetstream | kv-jetstream |
|-----------|:------------:|:------------:|
| `Storage` (base) | ✅ | ✅ |
| `StorageWithBucket` | ✅ | ✅ |
| `StorageWithList` | ✅ | ❌ |
| `StorageWithStat` | ✅ | ❌ |
| `StorageWithReader` | ✅ | ❌ |
| `StorageWithWatch` | ❌ | ✅ |
| `StorageWithRevision` | ❌ | ✅ |
| `StorageWithKeys` | ❌ | ✅ |
| `StorageWithStatus` | ❌ | ✅ |

### Feature Comparison

| Feature | fs-jetstream | kv-jetstream |
|---------|-------------|--------------|
| **Data Model** | Binary objects (files) | Key-value pairs |
| **Max Value Size** | Unlimited (streaming) | ~1MB recommended |
| **Streaming Support** | Yes (GetReader/PutReader) | No |
| **Object Listing** | Yes (with prefix filter) | No (use Keys instead) |
| **Watch Changes** | No | Yes (pattern-based) |
| **Optimistic Locking** | No | Yes (revision-based) |
| **Metadata Retrieval** | Yes (Stat) | Yes (GetEntry) |
| **TTL Support** | Yes | Yes |
| **Compression** | Yes (S2) | Yes (S2) |

### When to Use Each Backend

**Use fs-jetstream when:**
- Storing large files (documents, images, videos)
- Need streaming read/write for memory efficiency
- Files may exceed 1MB
- Need to list objects by prefix
- File metadata (size, hash, modtime) is important

**Use kv-jetstream when:**
- Storing small configuration values
- Need real-time change notifications (Watch)
- Require optimistic locking for concurrent updates
- Building cache or session storage
- Values are typically JSON or small binary data

## Practical Examples

### Generic Storage Handler

Write code that works with any storage backend:

```go
import (
    "context"
    "fmt"
    "github.com/go-monolith/mono/pkg/storage"
)

// StoreWithFallback stores data and optionally tracks revision
func StoreWithFallback(ctx context.Context, s storage.Storage, key string, data []byte) error {
    // Try revision-aware storage first
    if revStore, ok := s.(storage.StorageWithRevision); ok {
        rev, err := revStore.PutWithRevisionWithContext(ctx, key, data, 0)
        if err != nil {
            return err
        }
        fmt.Printf("Stored with revision: %d\n", rev)
        return nil
    }

    // Fall back to basic storage
    return s.SetWithContext(ctx, key, data, 0)
}
```

### Detecting Large File Support

```go
func StoreFile(ctx context.Context, s storage.Storage, key string, r io.Reader) error {
    // Check if streaming is supported
    if streamStore, ok := s.(storage.StorageWithReader); ok {
        _, err := streamStore.PutReaderWithContext(ctx, key, r, 0)
        return err
    }

    // Fall back: read all data into memory (not ideal for large files)
    data, err := io.ReadAll(r)
    if err != nil {
        return err
    }
    return s.SetWithContext(ctx, key, data, 0)
}
```

### Watching for Configuration Changes

```go
func WatchConfig(ctx context.Context, s storage.Storage, handler func(key string, value []byte)) error {
    // Check if watching is supported
    watcher, ok := s.(storage.StorageWithWatch)
    if !ok {
        return fmt.Errorf("storage does not support watching")
    }

    w, err := watcher.WatchWithContext(ctx, "config.*")
    if err != nil {
        return err
    }
    defer w.Stop()

    for entry := range w.Updates() {
        if entry == nil {
            continue // Initial sync marker
        }
        handler(entry.Key, entry.Value)
    }

    return nil
}
```

### Optimistic Locking Pattern

```go
func IncrementCounter(ctx context.Context, s storage.Storage, key string) error {
    revStore, ok := s.(storage.StorageWithRevision)
    if !ok {
        return fmt.Errorf("storage does not support revision-based updates")
    }

    for retries := 0; retries < 3; retries++ {
        // Get current value with revision
        entry, err := revStore.GetEntryWithContext(ctx, key)
        if err != nil {
            return err
        }

        // Increment
        count, _ := strconv.Atoi(string(entry.Value))
        count++
        newValue := []byte(strconv.Itoa(count))

        // Update with revision check
        _, err = revStore.UpdateWithContext(ctx, key, newValue, 0, entry.Revision)
        if err == nil {
            return nil // Success
        }

        if !errors.Is(err, storage.ErrRevisionMismatch) {
            return err // Real error
        }
        // Retry on revision mismatch
    }

    return fmt.Errorf("failed after 3 retries")
}
```

## Sentinel Errors

The storage package provides sentinel errors for programmatic error handling:

```go
import "github.com/go-monolith/mono/pkg/storage"

// Check for specific errors
if errors.Is(err, storage.ErrKeyNotFound) {
    // Key doesn't exist
}

if errors.Is(err, storage.ErrKeyExists) {
    // Key already exists (Create failed)
}

if errors.Is(err, storage.ErrRevisionMismatch) {
    // Concurrent modification detected
}

if errors.Is(err, storage.ErrBucketNotFound) {
    // Bucket doesn't exist
}
```

## Best Practices

### Capability Checking

✅ **Do**
- Always use the if-ok pattern for capability detection
- Handle the case when a capability is not available
- Use compile-time interface checks in your modules:
  ```go
  var _ storage.StorageWithWatch = (*MyStore)(nil)
  ```

❌ **Don't**
- Assume all storage backends support all interfaces
- Panic when a capability is missing
- Use unsafe type assertions without the `ok` check

### Storage Selection

✅ **Do**
- Choose fs-jetstream for large binary objects
- Choose kv-jetstream for small values with watch/revision needs
- Use separate buckets for different data types
- Consider the capability requirements before choosing a backend

❌ **Don't**
- Store large files in kv-jetstream (>1MB)
- Use fs-jetstream when you need real-time notifications
- Mix different data models in a single bucket

### Error Handling

✅ **Do**
- Check for sentinel errors using `errors.Is()`
- Retry on `ErrRevisionMismatch` for optimistic locking
- Handle `ErrKeyNotFound` gracefully for optional data

❌ **Don't**
- Compare errors using `==` (use `errors.Is()`)
- Ignore revision mismatches in concurrent scenarios
- Silently swallow storage errors

## Related Documentation

- [Storage API Reference](../api/storage.md) - Complete API documentation
- [File Storage Plugin](../plugins/fs-jetstream.md) - fs-jetstream plugin guide
- [Key-Value Storage Plugin](../plugins/kv-jetstream.md) - kv-jetstream plugin guide
- [Creating Custom Plugins](../plugins/creating-plugins.md) - Build your own storage plugin
