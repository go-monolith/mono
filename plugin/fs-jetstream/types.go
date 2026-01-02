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
