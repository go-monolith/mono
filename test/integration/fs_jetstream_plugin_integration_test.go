//go:build integration
// +build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	fsjetstream "github.com/go-monolith/mono/plugin/fs-jetstream"
)

// testConsumerModule implements mono.UsePluginModule to consume file storage plugin
type testConsumerModule struct {
	name          string
	storagePlugin *fsjetstream.PluginModule
	documents     fsjetstream.FileStoragePort
	uploads       fsjetstream.FileStoragePort
}

func (m *testConsumerModule) Name() string {
	return m.name
}

func (m *testConsumerModule) SetPlugin(alias string, plugin mono.PluginModule) {
	if alias == "storage" {
		m.storagePlugin = plugin.(*fsjetstream.PluginModule)
	}
}

func (m *testConsumerModule) Start(ctx context.Context) error {
	m.documents = m.storagePlugin.Bucket("documents")
	m.uploads = m.storagePlugin.Bucket("uploads")
	return nil
}

func (m *testConsumerModule) Stop(_ context.Context) error {
	return nil
}

func TestIntegration_FsJetstreamPlugin_BasicOperations(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Create file storage plugin with buckets
	storage, err := fsjetstream.New(fsjetstream.Config{
		Buckets: []fsjetstream.BucketConfig{
			{
				Name:        "documents",
				Description: "Document storage",
				Storage:     fsjetstream.MemoryStorage,
			},
			{
				Name:        "uploads",
				Description: "Temporary uploads",
				TTL:         1 * time.Hour,
				Storage:     fsjetstream.MemoryStorage,
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create storage plugin: %v", err)
	}

	// Register plugin
	if err := fw.RegisterPlugin(storage, "storage"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Register consumer module
	consumer := &testConsumerModule{name: "consumer"}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Verify buckets are available
	if consumer.documents == nil {
		t.Fatal("expected documents bucket to be available")
	}
	if consumer.uploads == nil {
		t.Fatal("expected uploads bucket to be available")
	}

	// Test Put and Get
	testData := []byte("Hello, World!")
	info, err := consumer.documents.Put(ctx, "test.txt", testData)
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	if info.Name != "test.txt" {
		t.Errorf("expected name 'test.txt', got '%s'", info.Name)
	}
	if info.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size)
	}

	// Get the object
	data, err := consumer.documents.GetWithContext(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}
	if string(data) != "Hello, World!" {
		t.Errorf("expected 'Hello, World!', got '%s'", string(data))
	}

	// Test Stat
	statInfo, err := consumer.documents.StatWithContext(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Failed to stat object: %v", err)
	}
	if statInfo.Name != "test.txt" {
		t.Errorf("expected name 'test.txt', got '%s'", statInfo.Name)
	}

	// Test List
	objects, err := consumer.documents.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("Failed to list objects: %v", err)
	}
	if len(objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(objects))
	}

	// Test Delete
	if err := consumer.documents.DeleteWithContext(ctx, "test.txt"); err != nil {
		t.Fatalf("Failed to delete object: %v", err)
	}

	// Verify deleted (Get returns nil, nil for missing keys)
	data, err = consumer.documents.GetWithContext(ctx, "test.txt")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data != nil {
		t.Error("expected nil data for deleted object")
	}
}

func TestIntegration_FsJetstreamPlugin_PutWithOptions(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	storage, err := fsjetstream.New(fsjetstream.Config{
		Buckets: []fsjetstream.BucketConfig{
			{Name: "documents", Storage: fsjetstream.MemoryStorage},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create storage plugin: %v", err)
	}

	if err := fw.RegisterPlugin(storage, "storage"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	consumer := &testConsumerModule{name: "consumer"}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Put with description and headers
	info, err := consumer.documents.Put(ctx, "doc.txt", []byte("content"),
		fsjetstream.WithDescription("Test document"),
		fsjetstream.WithHeaders(map[string]string{
			"Content-Type": "text/plain",
		}),
	)
	if err != nil {
		t.Fatalf("Failed to put object with options: %v", err)
	}

	if info.Description != "Test document" {
		t.Errorf("expected description 'Test document', got '%s'", info.Description)
	}
}

func TestIntegration_FsJetstreamPlugin_ListWithPrefix(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	storage, err := fsjetstream.New(fsjetstream.Config{
		Buckets: []fsjetstream.BucketConfig{
			{Name: "documents", Storage: fsjetstream.MemoryStorage},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create storage plugin: %v", err)
	}

	if err := fw.RegisterPlugin(storage, "storage"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	consumer := &testConsumerModule{name: "consumer"}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create multiple objects
	consumer.documents.Put(ctx, "docs/file1.txt", []byte("1"))
	consumer.documents.Put(ctx, "docs/file2.txt", []byte("2"))
	consumer.documents.Put(ctx, "images/logo.png", []byte("3"))

	// List with prefix
	docsOnly, err := consumer.documents.ListWithContext(ctx, fsjetstream.WithPrefix("docs/"))
	if err != nil {
		t.Fatalf("Failed to list with prefix: %v", err)
	}

	if len(docsOnly) != 2 {
		t.Errorf("expected 2 docs, got %d", len(docsOnly))
	}

	// List all
	all, err := consumer.documents.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("Failed to list all: %v", err)
	}

	if len(all) != 3 {
		t.Errorf("expected 3 total objects, got %d", len(all))
	}
}

func TestIntegration_FsJetstreamPlugin_MultipleBuckets(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	storage, err := fsjetstream.New(fsjetstream.Config{
		Buckets: []fsjetstream.BucketConfig{
			{Name: "bucket1", Storage: fsjetstream.MemoryStorage},
			{Name: "bucket2", Storage: fsjetstream.MemoryStorage},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create storage plugin: %v", err)
	}

	if err := fw.RegisterPlugin(storage, "storage"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Verify both buckets exist
	bucket1 := storage.Bucket("bucket1")
	bucket2 := storage.Bucket("bucket2")

	if bucket1 == nil {
		t.Fatal("expected bucket1 to exist")
	}
	if bucket2 == nil {
		t.Fatal("expected bucket2 to exist")
	}

	// Verify buckets are independent
	bucket1.Put(ctx, "file.txt", []byte("bucket1"))
	bucket2.Put(ctx, "file.txt", []byte("bucket2"))

	data1, _ := bucket1.GetWithContext(ctx, "file.txt")
	data2, _ := bucket2.GetWithContext(ctx, "file.txt")

	if string(data1) != "bucket1" {
		t.Errorf("expected 'bucket1', got '%s'", string(data1))
	}
	if string(data2) != "bucket2" {
		t.Errorf("expected 'bucket2', got '%s'", string(data2))
	}
}

func TestIntegration_FsJetstreamPlugin_BucketInfo(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	storage, err := fsjetstream.New(fsjetstream.Config{
		Buckets: []fsjetstream.BucketConfig{
			{Name: "documents", Storage: fsjetstream.MemoryStorage},
			{Name: "uploads", Storage: fsjetstream.MemoryStorage},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create storage plugin: %v", err)
	}

	if err := fw.RegisterPlugin(storage, "storage"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Test Buckets()
	buckets := storage.Buckets()
	if len(buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(buckets))
	}

	// Test HasBucket()
	if !storage.HasBucket("documents") {
		t.Error("expected HasBucket('documents') to be true")
	}
	if !storage.HasBucket("uploads") {
		t.Error("expected HasBucket('uploads') to be true")
	}
	if storage.HasBucket("nonexistent") {
		t.Error("expected HasBucket('nonexistent') to be false")
	}

	// Test BucketName()
	documents := storage.Bucket("documents")
	if documents.BucketName() != "documents" {
		t.Errorf("expected 'documents', got '%s'", documents.BucketName())
	}
}
