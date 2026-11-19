// Package fsjetstream provides a file storage plugin using JetStream ObjectStore.
//
// This plugin implements the PluginModule interface and provides bucket-based
// file storage capabilities using NATS JetStream ObjectStore as the backend.
//
// # Usage
//
// Create a plugin with configured buckets:
//
//	storage, err := fsjetstream.New(fsjetstream.Config{
//	    Buckets: []fsjetstream.BucketConfig{
//	        {Name: "documents", Description: "Document storage"},
//	        {Name: "uploads", TTL: 24 * time.Hour},
//	    },
//	})
//
//	app.RegisterPlugin(storage, "storage")
//
// Consumer modules access buckets via the plugin:
//
//	func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
//	    if alias == "storage" {
//	        m.storagePlugin = plugin.(*fsjetstream.PluginModule)
//	    }
//	}
//
//	func (m *MyModule) Start(ctx context.Context) error {
//	    m.documents = m.storagePlugin.Bucket("documents")
//	    return nil
//	}
package fsjetstream

import (
	"bytes"
	"fmt"
	"time"

	"github.com/go-monolith/mono/pkg/storage"
)

// objectNotFoundError creates an error for object not found.
func objectNotFoundError(key string) error {
	return fmt.Errorf("object not found: %s", key)
}

// newBytesReader creates a new bytes.Reader from a byte slice.
func newBytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}

// StorageType specifies the type of storage backend for a bucket.
type StorageType int

const (
	// FileStorage stores data on disk (default).
	FileStorage StorageType = iota
	// MemoryStorage stores data in memory (faster, not persistent).
	MemoryStorage
)

// Config defines the module configuration.
type Config struct {
	// Buckets defines the buckets to create on startup.
	// At least one bucket must be configured.
	Buckets []BucketConfig
}

// BucketConfig defines configuration for a single ObjectStore bucket.
type BucketConfig struct {
	// Name is the bucket name (required, must be unique).
	Name string

	// Description is an optional description for the bucket.
	Description string

	// MaxBytes is the maximum total size of all objects (0 = unlimited).
	MaxBytes int64

	// TTL is the time-to-live for objects (0 = no expiry).
	TTL time.Duration

	// Replicas is the number of replicas (1-5, default 1).
	Replicas int

	// Storage specifies file or memory storage.
	Storage StorageType

	// Compression enables S2 compression.
	Compression bool

	// RecreateOnStorageMismatch lets Start delete and recreate the bucket when
	// its stream (OBJ_<Name>) already exists with a different storage type
	// than Storage. NATS cannot change the storage type of an existing
	// stream, so without this option Start fails with ErrStorageMismatch
	// until the stream is removed by hand.
	//
	// Recreating DELETES EVERY OBJECT IN THE BUCKET, including when Storage
	// is changed on purpose: the next Start empties the bucket. Before the
	// delete, the objects are copied to a backup bucket
	// <Name>-backup-<UTC time>-<random> (file storage, kept for
	// RecreateBackupTTL), unless SkipBackupOnRecreate is set or the bucket is
	// empty; if the backup fails or does not finish within
	// RecreateBackupTimeout, nothing is deleted and Start fails. The backup
	// needs as much space as the bucket, and its objects' metadata is
	// rewritten to name it, so it is an object store of its own. The
	// application reads only the new, empty bucket: restoring from the
	// backup is manual. Set it only for a
	// bucket whose contents can be lost, such as caches or files their owners
	// can produce again.
	//
	// A matching or missing stream is never touched; only this bucket's own
	// stream is ever deleted, and the delete is logged at Warn with both
	// storage types, the number of messages and bytes it held and the backup
	// bucket. A stream that is not a plain object store of this name (a
	// mirror, or other subjects) is never deleted: Start fails instead.
	//
	// When several instances start at the same time, an instance that finds
	// the bucket repaired by another one while it copies (or right before
	// its delete) drops its backup and opens the repaired bucket. Objects
	// written to the repaired bucket between that last check and the delete
	// can still be lost (JetStream has no conditional delete). Two
	// applications that share a bucket with different Storage values and
	// both set this option delete each other's bucket on every start: give
	// every application the same Storage.
	RecreateOnStorageMismatch bool

	// SkipBackupOnRecreate skips the backup bucket that
	// RecreateOnStorageMismatch makes before it deletes the bucket. The
	// objects are then gone for good.
	SkipBackupOnRecreate bool

	// RecreateBackupTTL is how long the objects of the backup bucket made by
	// RecreateOnStorageMismatch are kept: 0 means DefaultRecreateBackupTTL
	// (7 days), a negative value keeps them until the backup bucket is
	// deleted by hand, and a positive value must be at least
	// MinRecreateBackupTTL (1 hour) and longer than the copy may take. The
	// empty backup bucket itself is not removed.
	RecreateBackupTTL time.Duration

	// RecreateBackupTimeout bounds the copy of the bucket into its backup
	// (0: DefaultRecreateBackupTimeout, 10 minutes). Object buckets can be
	// large: raise it for a bucket that cannot be copied in that time, and
	// allow for it in orchestrator startup deadlines, since the copy runs
	// inside Start.
	RecreateBackupTimeout time.Duration
}

// ObjectInfo is an alias to storage.ObjectInfo.
// Contains metadata about a stored object.
type ObjectInfo = storage.ObjectInfo

// PutOptions is an alias to storage.PutOptions.
// Contains options for Put operations.
type PutOptions = storage.PutOptions

// PutOption is an alias to storage.PutOption.
// Functional option for Put operations.
type PutOption = storage.PutOption

// WithDescription sets the object description.
func WithDescription(description string) PutOption {
	return storage.WithPutDescription(description)
}

// WithHeaders sets custom metadata headers.
func WithHeaders(headers map[string]string) PutOption {
	return storage.WithPutHeaders(headers)
}

// ListOptions is an alias to storage.ListOptions.
// Contains options for List operations.
type ListOptions = storage.ListOptions

// ListOption is an alias to storage.ListOption.
// Functional option for List operations.
type ListOption = storage.ListOption

// WithPrefix sets the prefix filter for listing objects.
func WithPrefix(prefix string) ListOption {
	return storage.WithListPrefix(prefix)
}
