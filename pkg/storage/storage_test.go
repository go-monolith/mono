package storage

import (
	"errors"
	"testing"
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
		{"ErrKeyNotFound", ErrKeyNotFound, "key not found"},
		{"ErrKeyExists", ErrKeyExists, "key already exists"},
		{"ErrRevisionMismatch", ErrRevisionMismatch, "revision mismatch"},
		{"ErrBucketNotFound", ErrBucketNotFound, "bucket not found"},
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
	wrapped := errors.New("wrapped: " + ErrKeyNotFound.Error())
	if errors.Is(wrapped, ErrKeyNotFound) {
		t.Log("Note: Standard wrapped errors don't match with errors.Is - this is expected")
	}

	// Test direct comparison
	if !errors.Is(ErrKeyNotFound, ErrKeyNotFound) {
		t.Error("expected ErrKeyNotFound to match itself")
	}
}

// =============================================================================
// List Options Tests
// =============================================================================

func TestWithListPrefix(t *testing.T) {
	opt := WithListPrefix("test-prefix")
	options := &ListOptions{}
	opt(options)

	if options.Prefix != "test-prefix" {
		t.Errorf("expected prefix 'test-prefix', got %q", options.Prefix)
	}
}

func TestApplyListOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []ListOption
		expected *ListOptions
	}{
		{
			name:     "no options",
			opts:     nil,
			expected: &ListOptions{},
		},
		{
			name:     "empty options",
			opts:     []ListOption{},
			expected: &ListOptions{},
		},
		{
			name:     "with prefix",
			opts:     []ListOption{WithListPrefix("prefix")},
			expected: &ListOptions{Prefix: "prefix"},
		},
		{
			name: "multiple options - last wins",
			opts: []ListOption{
				WithListPrefix("first"),
				WithListPrefix("second"),
			},
			expected: &ListOptions{Prefix: "second"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyListOptions(tt.opts...)
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
	opt := WithPutDescription("test description")
	options := &PutOptions{}
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
	opt := WithPutHeaders(headers)
	options := &PutOptions{}
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
		opts           []PutOption
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
			opts:           []PutOption{WithPutDescription("desc")},
			expDescription: "desc",
			expHeadersLen:  0,
		},
		{
			name:           "with headers",
			opts:           []PutOption{WithPutHeaders(map[string]string{"key": "value"})},
			expDescription: "",
			expHeadersLen:  1,
		},
		{
			name: "with both",
			opts: []PutOption{
				WithPutDescription("my desc"),
				WithPutHeaders(map[string]string{"a": "1", "b": "2"}),
			},
			expDescription: "my desc",
			expHeadersLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyPutOptions(tt.opts...)
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
	opt := WithWatchUpdatesOnly()
	options := &WatchOptions{}
	opt(options)

	if !options.UpdatesOnly {
		t.Error("expected UpdatesOnly to be true")
	}
}

func TestWithWatchIgnoreDeletes(t *testing.T) {
	opt := WithWatchIgnoreDeletes()
	options := &WatchOptions{}
	opt(options)

	if !options.IgnoreDeletes {
		t.Error("expected IgnoreDeletes to be true")
	}
}

func TestWithWatchMetaOnly(t *testing.T) {
	opt := WithWatchMetaOnly()
	options := &WatchOptions{}
	opt(options)

	if !options.MetaOnly {
		t.Error("expected MetaOnly to be true")
	}
}

func TestWithWatchResumeFromRevision(t *testing.T) {
	opt := WithWatchResumeFromRevision(42)
	options := &WatchOptions{}
	opt(options)

	if options.ResumeFromRevision != 42 {
		t.Errorf("expected ResumeFromRevision 42, got %d", options.ResumeFromRevision)
	}
}

func TestApplyWatchOptions(t *testing.T) {
	tests := []struct {
		name             string
		opts             []WatchOption
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
			opts:           []WatchOption{WithWatchUpdatesOnly()},
			expUpdatesOnly: true,
		},
		{
			name:             "ignore deletes",
			opts:             []WatchOption{WithWatchIgnoreDeletes()},
			expIgnoreDeletes: true,
		},
		{
			name:        "meta only",
			opts:        []WatchOption{WithWatchMetaOnly()},
			expMetaOnly: true,
		},
		{
			name:             "resume from revision",
			opts:             []WatchOption{WithWatchResumeFromRevision(100)},
			expResumeFromRev: 100,
		},
		{
			name: "all options combined",
			opts: []WatchOption{
				WithWatchUpdatesOnly(),
				WithWatchIgnoreDeletes(),
				WithWatchMetaOnly(),
				WithWatchResumeFromRevision(999),
			},
			expUpdatesOnly:   true,
			expIgnoreDeletes: true,
			expMetaOnly:      true,
			expResumeFromRev: 999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyWatchOptions(tt.opts...)
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
	opt := WithDeleteRevision(123)
	options := &DeleteOptions{}
	opt(options)

	if options.Revision != 123 {
		t.Errorf("expected Revision 123, got %d", options.Revision)
	}
}

func TestApplyDeleteOptions(t *testing.T) {
	tests := []struct {
		name        string
		opts        []DeleteOption
		expRevision uint64
	}{
		{
			name:        "no options",
			opts:        nil,
			expRevision: 0,
		},
		{
			name:        "empty options",
			opts:        []DeleteOption{},
			expRevision: 0,
		},
		{
			name:        "with revision",
			opts:        []DeleteOption{WithDeleteRevision(456)},
			expRevision: 456,
		},
		{
			name: "multiple revisions - last wins",
			opts: []DeleteOption{
				WithDeleteRevision(1),
				WithDeleteRevision(2),
			},
			expRevision: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyDeleteOptions(tt.opts...)
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
	if KeyOperationPut != 0 {
		t.Errorf("expected KeyOperationPut to be 0, got %d", KeyOperationPut)
	}
	if KeyOperationDelete != 1 {
		t.Errorf("expected KeyOperationDelete to be 1, got %d", KeyOperationDelete)
	}
	if KeyOperationPurge != 2 {
		t.Errorf("expected KeyOperationPurge to be 2, got %d", KeyOperationPurge)
	}
}

// =============================================================================
// Option Composition Tests
// =============================================================================

func TestOptionComposition_List(t *testing.T) {
	// Test that options can be stored and applied in different orders
	opts := []ListOption{
		WithListPrefix("first"),
	}

	// Add more options
	opts = append(opts, WithListPrefix("second"))

	result := ApplyListOptions(opts...)
	if result.Prefix != "second" {
		t.Errorf("expected last prefix 'second', got %q", result.Prefix)
	}
}

func TestOptionComposition_Put(t *testing.T) {
	// Test composing options from multiple sources
	baseOpts := []PutOption{
		WithPutDescription("base"),
	}

	additionalOpts := []PutOption{
		WithPutHeaders(map[string]string{"X-Header": "value"}),
	}

	allOpts := append(baseOpts, additionalOpts...)
	result := ApplyPutOptions(allOpts...)

	if result.Description != "base" {
		t.Errorf("expected description 'base', got %q", result.Description)
	}
	if len(result.Headers) != 1 {
		t.Errorf("expected 1 header, got %d", len(result.Headers))
	}
}

func TestOptionComposition_Watch(t *testing.T) {
	// Test that watch options can be applied incrementally
	opts := []WatchOption{}

	opts = append(opts, WithWatchUpdatesOnly())
	opts = append(opts, WithWatchMetaOnly())

	result := ApplyWatchOptions(opts...)

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
