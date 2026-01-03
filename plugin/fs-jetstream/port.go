package fsjetstream

import (
	"context"

	"github.com/go-monolith/mono/pkg/storage"
)

// FileStoragePort is the port interface exposed to consumer modules.
// It defines operations that consumers can perform on a storage bucket.
//
// This interface embeds storage.Storage and extended interfaces for full
// storage capabilities, plus file-specific operations like Put with metadata.
//
// # Interface Hierarchy
//
//   - storage.Storage: Base operations (Get, Set, Delete, Reset, Close)
//   - storage.StorageWithBucket: Bucket name awareness
//   - storage.StorageWithList: Listing capabilities
//   - storage.StorageWithStat: Metadata retrieval
//   - storage.StorageWithReader: Streaming read/write
//
// # Usage
//
// Consumer modules access buckets via the plugin:
//
//	type DocumentModule struct {
//	    storagePlugin *fsjetstream.PluginModule
//	    documents     fsjetstream.FileStoragePort
//	}
//
//	func (m *DocumentModule) Start(ctx context.Context) error {
//	    m.documents = m.storagePlugin.Bucket("documents")
//	    if m.documents == nil {
//	        return fmt.Errorf("bucket 'documents' not found")
//	    }
//
//	    // Store an object (with metadata return)
//	    info, err := m.documents.Put(ctx, "file.txt", []byte("Hello"))
//	    if err != nil {
//	        return err
//	    }
//	    fmt.Printf("Stored: %s, Size: %d\n", info.Name, info.Size)
//
//	    // Retrieve an object (nil, nil if not found)
//	    data, err := m.documents.Get("file.txt")
//	    if err != nil {
//	        return err
//	    }
//	    if data == nil {
//	        fmt.Println("Object not found")
//	    }
//
//	    return nil
//	}
//
// # Passing to Functions Expecting storage.Storage
//
// FileStoragePort satisfies storage.Storage, allowing it to be passed to
// generic storage functions:
//
//	func processStorage(s storage.Storage) {
//	    data, _ := s.Get("key")
//	    s.Set("key", []byte("value"), 0)
//	}
//
//	bucket := plugin.Bucket("documents")
//	processStorage(bucket) // Works!
type FileStoragePort interface {
	// Embed base storage interface for basic operations.
	// Provides: Get, GetWithContext, Set, SetWithContext, Delete, DeleteWithContext,
	// Reset, ResetWithContext, Close
	storage.Storage

	// Embed extended interfaces for file-specific capabilities.
	storage.StorageWithBucket // BucketName()
	storage.StorageWithList   // List, ListWithContext
	storage.StorageWithStat   // Stat, StatWithContext
	storage.StorageWithReader // GetReader, GetReaderWithContext, PutReader, PutReaderWithContext

	// Put stores an object from bytes and returns metadata.
	// This is the preferred method over Set() when you need ObjectInfo.
	// Returns ObjectInfo containing metadata about the stored object.
	Put(ctx context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error)
}
