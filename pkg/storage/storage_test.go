package storage_test

import (
	"errors"
	"testing"

	"github.com/go-monolith/mono/v1/pkg/storage"
)

// =============================================================================
// Sentinel Errors Tests
// =============================================================================

func TestSentinelErrors(t *testing.T) {
	// Test that sentinel errors are properly defined
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrKeyNotFound", storage.ErrKeyNotFound, "key not found"},
		{"ErrKeyExists", storage.ErrKeyExists, "key already exists"},
		{"ErrRevisionMismatch", storage.ErrRevisionMismatch, "revision mismatch"},
		{"ErrBucketNotFound", storage.ErrBucketNotFound, "bucket not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Errorf("expected %s to be non-nil", tt.name)
			}
			if tt.err.Error() != tt.msg {
				t.Errorf("expected error message %q, got %q", tt.msg, tt.err.Error())
			}
		})
	}
}

func TestSentinelErrors_ErrorsIs(t *testing.T) {
	// Test that errors.Is works correctly with wrapped errors
	wrapped := errors.New("wrapped: " + storage.ErrKeyNotFound.Error())
	if errors.Is(wrapped, storage.ErrKeyNotFound) {
		t.Log("Note: Standard wrapped errors don't match with errors.Is - this is expected")
	}

	// Test direct comparison
	if !errors.Is(storage.ErrKeyNotFound, storage.ErrKeyNotFound) {
		t.Error("expected ErrKeyNotFound to match itself")
	}
}

// =============================================================================
// List Options Tests
// =============================================================================

func TestWithListPrefix(t *testing.T) {
	opt := storage.WithListPrefix("test-prefix")
	options := &storage.ListOptions{}
	opt(options)

	if options.Prefix != "test-prefix" {
		t.Errorf("expected prefix 'test-prefix', got %q", options.Prefix)
	}
}

func TestApplyListOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []storage.ListOption
		expected *storage.ListOptions
	}{
		{
			name:     "no options",
			opts:     nil,
			expected: &storage.ListOptions{},
		},
		{
			name:     "empty options",
			opts:     []storage.ListOption{},
			expected: &storage.ListOptions{},
		},
		{
			name:     "with prefix",
			opts:     []storage.ListOption{storage.WithListPrefix("prefix")},
			expected: &storage.ListOptions{Prefix: "prefix"},
		},
		{
			name: "multiple options - last wins",
			opts: []storage.ListOption{
				storage.WithListPrefix("first"),
				storage.WithListPrefix("second"),
			},
			expected: &storage.ListOptions{Prefix: "second"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := storage.ApplyListOptions(tt.opts...)
			if result.Prefix != tt.expected.Prefix {
				t.Errorf("expected Prefix %q, got %q", tt.expected.Prefix, result.Prefix)
			}
		})
	}
}

// =============================================================================
// Put Options Tests
// =============================================================================

func TestWithPutDescription(t *testing.T) {
	opt := storage.WithPutDescription("test description")
	options := &storage.PutOptions{}
	opt(options)

	if options.Description != "test description" {
		t.Errorf("expected description 'test description', got %q", options.Description)
	}
}

func TestWithPutHeaders(t *testing.T) {
	headers := map[string]string{
		"Content-Type": "application/json",
		"X-Custom":     "value",
	}
	opt := storage.WithPutHeaders(headers)
	options := &storage.PutOptions{}
	opt(options)

	if len(options.Headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(options.Headers))
	}
	if options.Headers["Content-Type"] != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %q", options.Headers["Content-Type"])
	}
	if options.Headers["X-Custom"] != "value" {
		t.Errorf("expected X-Custom 'value', got %q", options.Headers["X-Custom"])
	}
}

func TestApplyPutOptions(t *testing.T) {
	tests := []struct {
		name           string
		opts           []storage.PutOption
		expDescription string
		expHeadersLen  int
	}{
		{
			name:           "no options",
			opts:           nil,
			expDescription: "",
			expHeadersLen:  0,
		},
		{
			name:           "with description",
			opts:           []storage.PutOption{storage.WithPutDescription("desc")},
			expDescription: "desc",
			expHeadersLen:  0,
		},
		{
			name:           "with headers",
			opts:           []storage.PutOption{storage.WithPutHeaders(map[string]string{"key": "value"})},
			expDescription: "",
			expHeadersLen:  1,
		},
		{
			name: "with both",
			opts: []storage.PutOption{
				storage.WithPutDescription("my desc"),
				storage.WithPutHeaders(map[string]string{"a": "1", "b": "2"}),
			},
			expDescription: "my desc",
			expHeadersLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := storage.ApplyPutOptions(tt.opts...)
			if result.Description != tt.expDescription {
				t.Errorf("expected Description %q, got %q", tt.expDescription, result.Description)
			}
			if len(result.Headers) != tt.expHeadersLen {
				t.Errorf("expected %d headers, got %d", tt.expHeadersLen, len(result.Headers))
			}
		})
	}
}

// =============================================================================
// Watch Options Tests
// =============================================================================

func TestWithWatchUpdatesOnly(t *testing.T) {
	opt := storage.WithWatchUpdatesOnly()
	options := &storage.WatchOptions{}
	opt(options)

	if !options.UpdatesOnly {
		t.Error("expected UpdatesOnly to be true")
	}
}

func TestWithWatchIgnoreDeletes(t *testing.T) {
	opt := storage.WithWatchIgnoreDeletes()
	options := &storage.WatchOptions{}
	opt(options)

	if !options.IgnoreDeletes {
		t.Error("expected IgnoreDeletes to be true")
	}
}

func TestWithWatchMetaOnly(t *testing.T) {
	opt := storage.WithWatchMetaOnly()
	options := &storage.WatchOptions{}
	opt(options)

	if !options.MetaOnly {
		t.Error("expected MetaOnly to be true")
	}
}

func TestWithWatchResumeFromRevision(t *testing.T) {
	opt := storage.WithWatchResumeFromRevision(42)
	options := &storage.WatchOptions{}
	opt(options)

	if options.ResumeFromRevision != 42 {
		t.Errorf("expected ResumeFromRevision 42, got %d", options.ResumeFromRevision)
	}
}

func TestApplyWatchOptions(t *testing.T) {
	tests := []struct {
		name             string
		opts             []storage.WatchOption
		expUpdatesOnly   bool
		expIgnoreDeletes bool
		expMetaOnly      bool
		expResumeFromRev uint64
	}{
		{
			name:             "no options",
			opts:             nil,
			expUpdatesOnly:   false,
			expIgnoreDeletes: false,
			expMetaOnly:      false,
			expResumeFromRev: 0,
		},
		{
			name:           "updates only",
			opts:           []storage.WatchOption{storage.WithWatchUpdatesOnly()},
			expUpdatesOnly: true,
		},
		{
			name:             "ignore deletes",
			opts:             []storage.WatchOption{storage.WithWatchIgnoreDeletes()},
			expIgnoreDeletes: true,
		},
		{
			name:        "meta only",
			opts:        []storage.WatchOption{storage.WithWatchMetaOnly()},
			expMetaOnly: true,
		},
		{
			name:             "resume from revision",
			opts:             []storage.WatchOption{storage.WithWatchResumeFromRevision(100)},
			expResumeFromRev: 100,
		},
		{
			name: "all options combined",
			opts: []storage.WatchOption{
				storage.WithWatchUpdatesOnly(),
				storage.WithWatchIgnoreDeletes(),
				storage.WithWatchMetaOnly(),
				storage.WithWatchResumeFromRevision(999),
			},
			expUpdatesOnly:   true,
			expIgnoreDeletes: true,
			expMetaOnly:      true,
			expResumeFromRev: 999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := storage.ApplyWatchOptions(tt.opts...)
			if result.UpdatesOnly != tt.expUpdatesOnly {
				t.Errorf("expected UpdatesOnly %v, got %v", tt.expUpdatesOnly, result.UpdatesOnly)
			}
			if result.IgnoreDeletes != tt.expIgnoreDeletes {
				t.Errorf("expected IgnoreDeletes %v, got %v", tt.expIgnoreDeletes, result.IgnoreDeletes)
			}
			if result.MetaOnly != tt.expMetaOnly {
				t.Errorf("expected MetaOnly %v, got %v", tt.expMetaOnly, result.MetaOnly)
			}
			if result.ResumeFromRevision != tt.expResumeFromRev {
				t.Errorf("expected ResumeFromRevision %d, got %d", tt.expResumeFromRev, result.ResumeFromRevision)
			}
		})
	}
}

// =============================================================================
// Delete Options Tests
// =============================================================================

func TestWithDeleteRevision(t *testing.T) {
	opt := storage.WithDeleteRevision(123)
	options := &storage.DeleteOptions{}
	opt(options)

	if options.Revision != 123 {
		t.Errorf("expected Revision 123, got %d", options.Revision)
	}
}

func TestApplyDeleteOptions(t *testing.T) {
	tests := []struct {
		name        string
		opts        []storage.DeleteOption
		expRevision uint64
	}{
		{
			name:        "no options",
			opts:        nil,
			expRevision: 0,
		},
		{
			name:        "empty options",
			opts:        []storage.DeleteOption{},
			expRevision: 0,
		},
		{
			name:        "with revision",
			opts:        []storage.DeleteOption{storage.WithDeleteRevision(456)},
			expRevision: 456,
		},
		{
			name: "multiple revisions - last wins",
			opts: []storage.DeleteOption{
				storage.WithDeleteRevision(1),
				storage.WithDeleteRevision(2),
			},
			expRevision: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := storage.ApplyDeleteOptions(tt.opts...)
			if result.Revision != tt.expRevision {
				t.Errorf("expected Revision %d, got %d", tt.expRevision, result.Revision)
			}
		})
	}
}

// =============================================================================
// KeyOperation Tests
// =============================================================================

func TestKeyOperation_Constants(t *testing.T) {
	// Verify the constants are defined correctly
	if storage.KeyOperationPut != 0 {
		t.Errorf("expected KeyOperationPut to be 0, got %d", storage.KeyOperationPut)
	}
	if storage.KeyOperationDelete != 1 {
		t.Errorf("expected KeyOperationDelete to be 1, got %d", storage.KeyOperationDelete)
	}
	if storage.KeyOperationPurge != 2 {
		t.Errorf("expected KeyOperationPurge to be 2, got %d", storage.KeyOperationPurge)
	}
}

// =============================================================================
// Option Composition Tests
// =============================================================================

func TestOptionComposition_List(t *testing.T) {
	// Test that options can be stored and applied in different orders
	opts := []storage.ListOption{
		storage.WithListPrefix("first"),
	}

	// Add more options
	opts = append(opts, storage.WithListPrefix("second"))

	result := storage.ApplyListOptions(opts...)
	if result.Prefix != "second" {
		t.Errorf("expected last prefix 'second', got %q", result.Prefix)
	}
}

func TestOptionComposition_Put(t *testing.T) {
	// Test composing options from multiple sources
	baseOpts := []storage.PutOption{
		storage.WithPutDescription("base"),
	}

	additionalOpts := []storage.PutOption{
		storage.WithPutHeaders(map[string]string{"X-Header": "value"}),
	}

	allOpts := append(baseOpts, additionalOpts...)
	result := storage.ApplyPutOptions(allOpts...)

	if result.Description != "base" {
		t.Errorf("expected description 'base', got %q", result.Description)
	}
	if len(result.Headers) != 1 {
		t.Errorf("expected 1 header, got %d", len(result.Headers))
	}
}

func TestOptionComposition_Watch(t *testing.T) {
	// Test that watch options can be applied incrementally
	opts := []storage.WatchOption{}

	opts = append(opts, storage.WithWatchUpdatesOnly())
	opts = append(opts, storage.WithWatchMetaOnly())

	result := storage.ApplyWatchOptions(opts...)

	if !result.UpdatesOnly {
		t.Error("expected UpdatesOnly to be true")
	}
	if !result.MetaOnly {
		t.Error("expected MetaOnly to be true")
	}
	// Unset options should remain default
	if result.IgnoreDeletes {
		t.Error("expected IgnoreDeletes to be false")
	}
}
