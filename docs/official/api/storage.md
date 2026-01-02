# Storage Interface

The `pkg/storage` package provides a unified storage interface for communicating with different storage backends. This abstraction enables consistent storage operations across different implementations (JetStream KV, JetStream ObjectStore, and future backends like Redis or S3).

## Overview

The storage package defines a composable interface hierarchy where implementations only need to implement the interfaces they support. This design enables:

- **Backend Agnostic Code**: Write storage code once, swap backends without changes
- **Capability Detection**: Check for extended capabilities using type assertions
- **Consistent Patterns**: Same API patterns across KV store, file storage, and future implementations
- **GoFiber Compatibility**: The base `Storage` interface is fully compatible with [GoFiber Storage](https://github.com/gofiber/storage), allowing you to use any of its 40+ storage implementations (Redis, PostgreSQL, MongoDB, S3, DynamoDB, etc.) within this framework

## Interface Hierarchy

Storage implementations compose interfaces based on their capabilities:

```
Storage (base)
     │
     ├── StorageWithBucket      - Bucket name awareness
     │
     ├── StorageWithList        - Object enumeration (fs-jetstream)
     │
     ├── StorageWithStat        - Metadata retrieval (fs-jetstream)
     │
     ├── StorageWithReader      - Streaming read/write (fs-jetstream)
     │
     ├── StorageWithWatch       - Real-time notifications (kv-jetstream)
     │
     ├── StorageWithRevision    - Optimistic locking (kv-jetstream)
     │
     ├── StorageWithKeys        - Key enumeration (kv-jetstream)
     │
     └── StorageWithStatus      - Status information (kv-jetstream)
```

## Base Interface

### Storage

The base interface for all storage backends. All implementations must provide these methods.

```go
type Storage interface {
    // Get retrieves the value for a key.
    // Returns nil, nil when key does not exist.
    Get(key string) ([]byte, error)
    GetWithContext(ctx context.Context, key string) ([]byte, error)

    // Set stores a value with optional expiration.
    // exp=0 means no expiration.
    Set(key string, val []byte, exp time.Duration) error
    SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error

    // Delete removes a key.
    // Returns no error if key doesn't exist.
    Delete(key string) error
    DeleteWithContext(ctx context.Context, key string) error

    // Reset deletes all keys in the storage.
    Reset() error
    ResetWithContext(ctx context.Context) error

    // Close closes the storage and releases resources.
    Close() error
}
```

## Extended Interfaces

### StorageWithBucket

Provides bucket name awareness for bucket-based storage backends.

```go
type StorageWithBucket interface {
    BucketName() string
}
```

### StorageWithList

Provides object enumeration for storage backends that support listing.

```go
type StorageWithList interface {
    List(opts ...ListOption) ([]ObjectInfo, error)
    ListWithContext(ctx context.Context, opts ...ListOption) ([]ObjectInfo, error)
}
```

**Used by**: [File Storage Plugin (fs-jetstream)](../plugins/fs-jetstream.md)

### StorageWithStat

Provides metadata retrieval without fetching data.

```go
type StorageWithStat interface {
    Stat(key string) (*ObjectInfo, error)
    StatWithContext(ctx context.Context, key string) (*ObjectInfo, error)
}
```

**Used by**: [File Storage Plugin (fs-jetstream)](../plugins/fs-jetstream.md)

### StorageWithReader

Provides streaming read/write for large objects.

```go
type StorageWithReader interface {
    GetReader(key string) (io.ReadCloser, *ObjectInfo, error)
    GetReaderWithContext(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
    PutReader(key string, reader io.Reader, exp time.Duration, opts ...PutOption) (*ObjectInfo, error)
    PutReaderWithContext(ctx context.Context, key string, reader io.Reader, exp time.Duration, opts ...PutOption) (*ObjectInfo, error)
}
```

**Used by**: [File Storage Plugin (fs-jetstream)](../plugins/fs-jetstream.md)

### StorageWithWatch

Provides real-time change notifications.

```go
type StorageWithWatch interface {
    Watch(pattern string, opts ...WatchOption) (KeyWatcher, error)
    WatchWithContext(ctx context.Context, pattern string, opts ...WatchOption) (KeyWatcher, error)
}
```

**Used by**: [Key-Value Storage Plugin (kv-jetstream)](../plugins/kv-jetstream.md)

### StorageWithRevision

Provides optimistic locking via revisions for safe concurrent updates.

```go
type StorageWithRevision interface {
    // Create stores only if key doesn't exist
    Create(key string, val []byte, exp time.Duration) (uint64, error)
    CreateWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error)

    // Update stores only if revision matches
    Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error)
    UpdateWithContext(ctx context.Context, key string, val []byte, exp time.Duration, revision uint64) (uint64, error)

    // Purge hard-deletes a key and all history
    Purge(key string) error
    PurgeWithContext(ctx context.Context, key string) error

    // GetEntry retrieves value with revision metadata
    GetEntry(key string) (*Entry, error)
    GetEntryWithContext(ctx context.Context, key string) (*Entry, error)

    // PutWithRevision stores and returns revision number
    PutWithRevision(key string, val []byte, exp time.Duration) (uint64, error)
    PutWithRevisionWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error)
}
```

**Used by**: [Key-Value Storage Plugin (kv-jetstream)](../plugins/kv-jetstream.md)

### StorageWithKeys

Provides key enumeration.

```go
type StorageWithKeys interface {
    Keys() ([]string, error)
    KeysWithContext(ctx context.Context) ([]string, error)
}
```

**Used by**: [Key-Value Storage Plugin (kv-jetstream)](../plugins/kv-jetstream.md)

### StorageWithStatus

Provides storage status information.

```go
type StorageWithStatus interface {
    Status() (*BucketStatus, error)
    StatusWithContext(ctx context.Context) (*BucketStatus, error)
}
```

**Used by**: [Key-Value Storage Plugin (kv-jetstream)](../plugins/kv-jetstream.md)

## Data Types

### ObjectInfo

Metadata for stored objects (used by file storage).

```go
type ObjectInfo struct {
    Bucket      string            // Bucket name
    Name        string            // Object key/name
    Size        int64             // Size in bytes
    Digest      string            // Content hash (SHA-256)
    ModTime     time.Time         // Last modification time
    Deleted     bool              // Deletion marker
    Headers     map[string]string // Custom metadata headers
    Description string            // Optional description
    Chunks      uint32            // Number of chunks
    Revision    uint64            // Sequence number
}
```

### Entry

Metadata for stored key-value pairs (used by KV storage).

```go
type Entry struct {
    Bucket    string        // Bucket name
    Key       string        // Key name
    Value     []byte        // Stored value (nil for delete markers)
    Revision  uint64        // Sequence number for optimistic locking
    Timestamp time.Time     // When this revision was created
    Operation KeyOperation  // Put, Delete, or Purge
}
```

### BucketStatus

Status information about a storage bucket.

```go
type BucketStatus struct {
    Bucket       string         // Bucket name
    Values       uint64         // Number of entries
    TTL          time.Duration  // Configured TTL
    BackingStore string         // Storage identifier
    Bytes        uint64         // Total data size
}
```

### KeyWatcher

Real-time notification handle for watching changes.

```go
type KeyWatcher interface {
    // Updates returns a channel that receives key-value updates.
    // A nil entry signals initial sync complete.
    Updates() <-chan *Entry

    // Stop stops the watcher and releases resources.
    Stop() error
}
```

### KeyOperation

Represents the type of operation performed on a key.

```go
const (
    KeyOperationPut    KeyOperation = iota // Put/update operation
    KeyOperationDelete                     // Soft delete
    KeyOperationPurge                      // Hard purge
)
```

## Sentinel Errors

The storage package defines standard errors for programmatic error handling:

```go
var (
    ErrKeyNotFound      = errors.New("key not found")
    ErrKeyExists        = errors.New("key already exists")
    ErrRevisionMismatch = errors.New("revision mismatch")
    ErrBucketNotFound   = errors.New("bucket not found")
)
```

**Usage:**
```go
entry, err := bucket.GetEntry("key")
if errors.Is(err, storage.ErrKeyNotFound) {
    // Handle key not found
}
```

## Functional Options

### ListOption

Options for listing objects.

```go
// Filter by prefix
storage.WithListPrefix("reports/2024/")
```

### PutOption

Options for storing objects.

```go
// Set description
storage.WithPutDescription("Q4 Report")

// Set custom headers
storage.WithPutHeaders(map[string]string{
    "Content-Type": "application/pdf",
})
```

### WatchOption

Options for watching changes.

```go
// Only receive future updates (skip current values)
storage.WithWatchUpdatesOnly()

// Filter out delete markers
storage.WithWatchIgnoreDeletes()

// Receive only metadata without values
storage.WithWatchMetaOnly()

// Resume from a specific revision
storage.WithWatchResumeFromRevision(100)
```

### DeleteOption

Options for delete operations.

```go
// Conditional delete (only if revision matches)
storage.WithDeleteRevision(expectedRevision)
```

## Context Variants Pattern

Most methods have two variants:
- **Non-context**: `Get(key)` - Uses `context.Background()` internally
- **Context-aware**: `GetWithContext(ctx, key)` - Accepts explicit context

Use context-aware variants when you need timeout control, cancellation, or request tracing.

```go
// Simple usage
data, err := bucket.Get("key")

// With timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
data, err := bucket.GetWithContext(ctx, "key")
```

## Capability Detection

Check for extended capabilities using type assertions:

```go
// Check if storage supports watching
if watcher, ok := storage.(storage.StorageWithWatch); ok {
    w, err := watcher.Watch("user.*")
    if err != nil {
        return err
    }
    defer w.Stop()

    for entry := range w.Updates() {
        fmt.Printf("Key %s changed\n", entry.Key)
    }
}

// Check if storage supports revisions
if revStore, ok := storage.(storage.StorageWithRevision); ok {
    entry, err := revStore.GetEntry("key")
    if err != nil {
        return err
    }
    // Use entry.Revision for optimistic locking
}
```

## Plugin Implementations

The following plugins implement subsets of the storage interface:

| Plugin | Base Interfaces | Extended Interfaces |
|--------|-----------------|---------------------|
| [kv-jetstream](../plugins/kv-jetstream.md) | Storage, StorageWithBucket | StorageWithWatch, StorageWithRevision, StorageWithKeys, StorageWithStatus |
| [fs-jetstream](../plugins/fs-jetstream.md) | Storage, StorageWithBucket | StorageWithList, StorageWithStat, StorageWithReader |

## Related Documentation

- [Key-Value Storage Plugin (kv-jetstream)](../plugins/kv-jetstream.md)
- [File Storage Plugin (fs-jetstream)](../plugins/fs-jetstream.md)
- [Creating Custom Plugins](../plugins/creating-plugins.md)

---

For plugin-specific documentation, see the individual plugin pages. For creating custom storage backends, implement the relevant interfaces from this package.
