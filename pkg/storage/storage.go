// Package storage provides a unified storage interface for communicating with
// different database/key-value providers.
//
// The base Storage interface provides simple key-value operations with optional
// expiration. Extended interfaces provide additional capabilities like listing,
// watching, streaming, and revision-based locking.
//
// # Interface Hierarchy
//
// Storage implementations compose interfaces based on their capabilities:
//
//   - Storage: Base interface for all storage backends (Get, Set, Delete, Reset, Close)
//   - StorageWithBucket: Provides bucket name awareness
//   - StorageWithList: Provides listing capabilities for object storage
//   - StorageWithStat: Provides metadata retrieval without fetching data
//   - StorageWithReader: Provides streaming read/write for large objects
//   - StorageWithWatch: Provides real-time change notifications
//   - StorageWithRevision: Provides optimistic locking via revisions
//   - StorageWithKeys: Provides key enumeration
//   - StorageWithStatus: Provides storage status information
//
// # Usage
//
// Check for extended capabilities using type assertions:
//
//	if watcher, ok := storage.(StorageWithWatch); ok {
//	    w, err := watcher.WatchWithContext(ctx, "user.*")
//	    // ...
//	}
package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// =============================================================================
// Base Interfaces
// =============================================================================

// StorageWithConn is a generic interface for accessing underlying storage connections.
// Implementations should return a connection to the storage using the proper driver.
type StorageWithConn[T any] interface {
	// Conn returns a connection to the storage.
	Conn() T
}

// Storage interface for communicating with different database/key-value
// providers. Visit https://github.com/gofiber/storage for more info.
type Storage interface {
	// GetWithContext gets the value for the given key with a context.
	// `nil, nil` is returned when the key does not exist
	GetWithContext(ctx context.Context, key string) ([]byte, error)

	// Get gets the value for the given key.
	// `nil, nil` is returned when the key does not exist
	Get(key string) ([]byte, error)

	// SetWithContext stores the given value for the given key
	// with an expiration value, 0 means no expiration.
	SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error

	// Set stores the given value for the given key along
	// with an expiration value, 0 means no expiration.
	// Empty key or value will be ignored without an error.
	Set(key string, val []byte, exp time.Duration) error

	// DeleteWithContext deletes the value for the given key with a context.
	// It returns no error if the storage does not contain the key,
	DeleteWithContext(ctx context.Context, key string) error

	// Delete deletes the value for the given key.
	// It returns no error if the storage does not contain the key,
	Delete(key string) error

	// ResetWithContext resets the storage and deletes all keys with a context.
	ResetWithContext(ctx context.Context) error

	// Reset resets the storage and delete all keys.
	Reset() error

	// Close closes the storage and will stop any running garbage
	// collectors and open connections.
	Close() error
}

// =============================================================================
// Extended Interfaces
// =============================================================================

// StorageWithBucket provides bucket name awareness for storage implementations.
// Used by bucket-based storage backends to identify their bucket.
type StorageWithBucket interface {
	// BucketName returns the name of the bucket this storage operates on.
	BucketName() string
}

// StorageWithList provides listing capabilities for storage backends.
// Typically used by object storage backends that support listing objects.
type StorageWithList interface {
	// ListWithContext returns all objects matching the optional filter options.
	ListWithContext(ctx context.Context, opts ...ListOption) ([]ObjectInfo, error)

	// List returns all objects matching the optional filter options.
	List(opts ...ListOption) ([]ObjectInfo, error)
}

// StorageWithStat provides metadata retrieval without fetching data.
// Useful for checking object existence and metadata without data transfer.
type StorageWithStat interface {
	// StatWithContext returns metadata for a key without retrieving the value.
	StatWithContext(ctx context.Context, key string) (*ObjectInfo, error)

	// Stat returns metadata for a key without retrieving the value.
	Stat(key string) (*ObjectInfo, error)
}

// StorageWithReader provides streaming read/write for large objects.
// Enables efficient handling of large files without loading into memory.
type StorageWithReader interface {
	// GetReaderWithContext retrieves an object as a reader (for large files).
	GetReaderWithContext(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)

	// GetReader retrieves an object as a reader (for large files).
	GetReader(key string) (io.ReadCloser, *ObjectInfo, error)

	// PutReaderWithContext stores an object from a reader.
	PutReaderWithContext(ctx context.Context, key string, reader io.Reader, exp time.Duration, opts ...PutOption) (*ObjectInfo, error)

	// PutReader stores an object from a reader.
	PutReader(key string, reader io.Reader, exp time.Duration, opts ...PutOption) (*ObjectInfo, error)
}

// StorageWithWatch provides real-time change notifications.
// Enables reactive patterns for cache invalidation and event-driven architectures.
type StorageWithWatch interface {
	// WatchWithContext creates a watcher for changes to keys matching the pattern.
	// Pattern supports wildcards: ">" for all keys, "user.*" for user keys.
	WatchWithContext(ctx context.Context, pattern string, opts ...WatchOption) (KeyWatcher, error)

	// Watch creates a watcher for changes to keys matching the pattern.
	Watch(pattern string, opts ...WatchOption) (KeyWatcher, error)
}

// StorageWithRevision provides optimistic locking via revisions.
// Enables safe concurrent updates without distributed locks.
type StorageWithRevision interface {
	// CreateWithContext stores a value only if the key does not exist.
	// Returns revision number on success, ErrKeyExists if key exists.
	CreateWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error)

	// Create stores a value only if the key does not exist.
	Create(key string, val []byte, exp time.Duration) (uint64, error)

	// UpdateWithContext stores a value only if the current revision matches.
	// Returns new revision on success, ErrRevisionMismatch if revision doesn't match.
	UpdateWithContext(ctx context.Context, key string, val []byte, exp time.Duration, revision uint64) (uint64, error)

	// Update stores a value only if the current revision matches.
	Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error)

	// PurgeWithContext hard-deletes a key and all its history.
	PurgeWithContext(ctx context.Context, key string) error

	// Purge hard-deletes a key and all its history.
	Purge(key string) error

	// GetEntryWithContext retrieves the value with revision metadata.
	// Use this when you need the revision for subsequent Update calls.
	GetEntryWithContext(ctx context.Context, key string) (*Entry, error)

	// GetEntry retrieves the value with revision metadata.
	GetEntry(key string) (*Entry, error)

	// PutWithRevisionWithContext stores a value and returns the new revision.
	// Use this when you need to track revisions for optimistic locking.
	PutWithRevisionWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error)

	// PutWithRevision stores a value and returns the new revision.
	PutWithRevision(key string, val []byte, exp time.Duration) (uint64, error)
}

// StorageWithKeys provides key enumeration.
// Useful for iterating over all keys in a storage bucket.
type StorageWithKeys interface {
	// KeysWithContext returns all keys in the storage.
	KeysWithContext(ctx context.Context) ([]string, error)

	// Keys returns all keys in the storage.
	Keys() ([]string, error)
}

// StorageWithStatus provides storage status information.
// Useful for monitoring and capacity planning.
type StorageWithStatus interface {
	// StatusWithContext returns status information about the storage.
	StatusWithContext(ctx context.Context) (*BucketStatus, error)

	// Status returns status information about the storage.
	Status() (*BucketStatus, error)
}

// =============================================================================
// Sentinel Errors
// =============================================================================

var (
	// ErrKeyNotFound is returned when a requested key does not exist.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyExists is returned when attempting to create a key that already exists.
	ErrKeyExists = errors.New("key already exists")

	// ErrRevisionMismatch is returned when an update's expected revision doesn't match.
	ErrRevisionMismatch = errors.New("revision mismatch")

	// ErrBucketNotFound is returned when a requested bucket does not exist.
	ErrBucketNotFound = errors.New("bucket not found")
)

// =============================================================================
// Data Types
// =============================================================================

// ObjectInfo contains metadata about a stored object.
// Used by object storage backends (like file storage).
type ObjectInfo struct {
	// Bucket is the bucket name containing the object.
	Bucket string

	// Name is the object key/name.
	Name string

	// Size is the object size in bytes.
	Size int64

	// Digest is the hash of content (typically SHA-256).
	Digest string

	// ModTime is the last modification time.
	ModTime time.Time

	// Deleted indicates if this is a deletion marker.
	Deleted bool

	// Headers contains custom metadata headers.
	Headers map[string]string

	// Description is an optional object description.
	Description string

	// Chunks is the number of chunks (for chunked storage).
	Chunks uint32

	// Revision is the sequence number for this object.
	Revision uint64
}

// Entry contains value and metadata for a stored key-value pair.
// Used by key-value storage backends.
type Entry struct {
	// Bucket is the bucket name containing the entry.
	Bucket string

	// Key is the key name.
	Key string

	// Value is the stored value (nil for delete markers).
	Value []byte

	// Revision is the sequence number for this key.
	// Used for optimistic locking with Update().
	Revision uint64

	// Timestamp is when this revision was created.
	Timestamp time.Time

	// Operation indicates the type of operation (Put, Delete, Purge).
	Operation KeyOperation
}

// BucketStatus contains status information about a storage bucket.
type BucketStatus struct {
	// Bucket is the bucket name.
	Bucket string

	// Values is the number of key-value pairs or objects.
	Values uint64

	// TTL is the configured time-to-live.
	TTL time.Duration

	// BackingStore is the underlying storage identifier.
	BackingStore string

	// Bytes is the total size of all data.
	Bytes uint64
}

// KeyOperation represents the type of operation performed on a key.
type KeyOperation int

const (
	// KeyOperationPut indicates a put/update operation.
	KeyOperationPut KeyOperation = iota
	// KeyOperationDelete indicates a soft delete operation.
	KeyOperationDelete
	// KeyOperationPurge indicates a hard purge operation.
	KeyOperationPurge
)

// KeyWatcher provides real-time notifications of key changes.
// Must be stopped when no longer needed to release resources.
type KeyWatcher interface {
	// Updates returns a channel that receives key-value updates.
	// A nil entry signals that all initial values have been sent
	// (when not using WithUpdatesOnly option).
	// The channel is closed when the watcher is stopped or encounters an error.
	Updates() <-chan *Entry

	// Stop stops the watcher and releases resources.
	// The Updates channel will be closed after Stop is called.
	Stop() error
}

// =============================================================================
// Functional Options - List
// =============================================================================

// ListOptions contains options for List operations.
type ListOptions struct {
	// Prefix filters objects by name prefix.
	Prefix string
}

// ListOption is a functional option for List operations.
type ListOption func(*ListOptions)

// WithListPrefix sets the prefix filter for listing objects.
func WithListPrefix(prefix string) ListOption {
	return func(o *ListOptions) {
		o.Prefix = prefix
	}
}

// ApplyListOptions applies ListOption functions to ListOptions.
func ApplyListOptions(opts ...ListOption) *ListOptions {
	options := &ListOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// =============================================================================
// Functional Options - Put
// =============================================================================

// PutOptions contains options for Put operations.
type PutOptions struct {
	// Description is an optional description for the object.
	Description string

	// Headers contains custom metadata headers.
	Headers map[string]string
}

// PutOption is a functional option for Put operations.
type PutOption func(*PutOptions)

// WithPutDescription sets the object description.
func WithPutDescription(description string) PutOption {
	return func(o *PutOptions) {
		o.Description = description
	}
}

// WithPutHeaders sets custom metadata headers.
func WithPutHeaders(headers map[string]string) PutOption {
	return func(o *PutOptions) {
		o.Headers = headers
	}
}

// ApplyPutOptions applies PutOption functions to PutOptions.
func ApplyPutOptions(opts ...PutOption) *PutOptions {
	options := &PutOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// =============================================================================
// Functional Options - Watch
// =============================================================================

// WatchOptions contains options for Watch operations.
type WatchOptions struct {
	// UpdatesOnly skips initial values and only receives future updates.
	UpdatesOnly bool

	// IgnoreDeletes filters out delete markers from updates.
	IgnoreDeletes bool

	// MetaOnly retrieves only entry metadata, not the value.
	MetaOnly bool

	// ResumeFromRevision resumes watching from a specific revision.
	ResumeFromRevision uint64
}

// WatchOption is a functional option for Watch operations.
type WatchOption func(*WatchOptions)

// WithWatchUpdatesOnly receives only future updates, skipping initial values.
func WithWatchUpdatesOnly() WatchOption {
	return func(o *WatchOptions) {
		o.UpdatesOnly = true
	}
}

// WithWatchIgnoreDeletes filters out delete markers from watch updates.
func WithWatchIgnoreDeletes() WatchOption {
	return func(o *WatchOptions) {
		o.IgnoreDeletes = true
	}
}

// WithWatchMetaOnly retrieves only entry metadata without values.
func WithWatchMetaOnly() WatchOption {
	return func(o *WatchOptions) {
		o.MetaOnly = true
	}
}

// WithWatchResumeFromRevision resumes watching from a specific revision.
func WithWatchResumeFromRevision(revision uint64) WatchOption {
	return func(o *WatchOptions) {
		o.ResumeFromRevision = revision
	}
}

// ApplyWatchOptions applies WatchOption functions to WatchOptions.
func ApplyWatchOptions(opts ...WatchOption) *WatchOptions {
	options := &WatchOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// =============================================================================
// Functional Options - Delete
// =============================================================================

// DeleteOptions contains options for Delete operations.
type DeleteOptions struct {
	// Revision specifies the expected revision for conditional delete.
	// If non-zero, delete only succeeds if current revision matches.
	Revision uint64
}

// DeleteOption is a functional option for Delete operations.
type DeleteOption func(*DeleteOptions)

// WithDeleteRevision sets the expected revision for conditional delete.
func WithDeleteRevision(revision uint64) DeleteOption {
	return func(o *DeleteOptions) {
		o.Revision = revision
	}
}

// ApplyDeleteOptions applies DeleteOption functions to DeleteOptions.
func ApplyDeleteOptions(opts ...DeleteOption) *DeleteOptions {
	options := &DeleteOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
