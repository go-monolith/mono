// Package kvjetstream provides a key-value storage plugin using JetStream KV Store.
//
// This plugin implements the PluginModule interface and provides bucket-based
// key-value storage capabilities using NATS JetStream KeyValue Store as the backend.
//
// # Usage
//
// Create a plugin with configured buckets:
//
//	kv, err := kvjetstream.New(kvjetstream.Config{
//	    Buckets: []kvjetstream.BucketConfig{
//	        {Name: "sessions", Description: "User sessions", TTL: 24 * time.Hour},
//	        {Name: "cache", Description: "Application cache"},
//	    },
//	})
//
//	app.RegisterPlugin(kv, "kv")
//
// Consumer modules access buckets via the plugin:
//
//	func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
//	    if alias == "kv" {
//	        m.kvPlugin = plugin.(*kvjetstream.PluginModule)
//	    }
//	}
//
//	func (m *MyModule) Start(ctx context.Context) error {
//	    m.sessions = m.kvPlugin.Bucket("sessions")
//	    return nil
//	}
package kvjetstream

import (
	"time"

	"github.com/go-monolith/mono/pkg/storage"
)

// ModuleName is the name of the kv-jetstream plugin module.
const ModuleName = "kv-jetstream"

// StorageType specifies the type of storage backend for a bucket.
type StorageType int

const (
	// FileStorage stores data on disk (default).
	FileStorage StorageType = iota
	// MemoryStorage stores data in memory (faster, not persistent).
	MemoryStorage
)

// KeyOperation is an alias to storage.KeyOperation.
// Represents the type of operation performed on a key.
type KeyOperation = storage.KeyOperation

// KeyOperation constants (aliases to storage constants).
const (
	// KeyOperationPut indicates a put/update operation.
	KeyOperationPut = storage.KeyOperationPut
	// KeyOperationDelete indicates a soft delete operation.
	KeyOperationDelete = storage.KeyOperationDelete
	// KeyOperationPurge indicates a hard purge operation.
	KeyOperationPurge = storage.KeyOperationPurge
)

// Config defines the module configuration.
type Config struct {
	// Buckets defines the buckets to create on startup.
	// At least one bucket must be configured.
	Buckets []BucketConfig
}

// BucketConfig defines configuration for a single KV bucket.
type BucketConfig struct {
	// Name is the bucket name (required, must be unique).
	Name string

	// Description is an optional description for the bucket.
	Description string

	// MaxValueSize is the maximum size of a value in bytes (0 = unlimited).
	MaxValueSize int32

	// TTL is the time-to-live for keys (0 = no expiry).
	TTL time.Duration

	// MaxBytes is the maximum total size of all keys/values (0 = unlimited).
	MaxBytes int64

	// Replicas is the number of replicas (1-5, default 1).
	Replicas int

	// Storage specifies file or memory storage.
	Storage StorageType

	// Compression enables S2 compression.
	Compression bool

	// RecreateOnStorageMismatch lets Start delete and recreate the bucket when
	// its stream (KV_<Name>) already exists with a different storage type
	// than Storage. NATS cannot change the storage type of an existing
	// stream, so without this option Start fails with ErrStorageMismatch
	// until the stream is removed by hand.
	//
	// Recreating DELETES EVERY KEY IN THE BUCKET, with no backup, including
	// when Storage is changed on purpose: the next Start wipes the bucket.
	// Set it only for a bucket whose contents can be lost, such as locks,
	// presence records or caches that their owners rebuild, never for data
	// that must be kept. A matching or missing stream is never
	// touched; only this bucket's own stream is ever deleted, and the delete
	// is logged at Warn with both storage types and the number of messages
	// and bytes it held. A stream that is not a plain KV bucket of this name
	// (a mirror, or other subjects) is never deleted: Start fails instead.
	//
	// When several instances start at the same time, keys written in the
	// first moments after one instance recreated the bucket can be lost to
	// another instance's repair (JetStream has no conditional delete). Two
	// applications that share a bucket with different Storage values and
	// both set this option delete each other's bucket on every start: give
	// every application the same Storage.
	RecreateOnStorageMismatch bool
}

// KVEntry is an alias to storage.Entry.
// Contains metadata and value for a stored key-value pair.
type KVEntry = storage.Entry

// BucketStatus is an alias to storage.BucketStatus.
// Contains status information about a KV bucket.
type BucketStatus = storage.BucketStatus
