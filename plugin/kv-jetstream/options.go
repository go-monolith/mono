package kvjetstream

import (
	"log/slog"

	"github.com/go-monolith/mono/pkg/storage"
)

// Option is a functional option for configuring the Module.
type Option func(*PluginModule) error

// WithLogger sets a custom logger for the module.
func WithLogger(logger *slog.Logger) Option {
	return func(m *PluginModule) error {
		m.logger = logger
		return nil
	}
}

// =============================================================================
// Delete Options
// =============================================================================

// DeleteOptions is an alias to storage.DeleteOptions.
// Contains options for Delete operations.
type DeleteOptions = storage.DeleteOptions

// DeleteOption is an alias to storage.DeleteOption.
// Functional option for Delete operations.
type DeleteOption = storage.DeleteOption

// WithDeleteRevision sets the expected revision for conditional delete.
// The delete will fail with ErrRevisionMismatch if the current revision doesn't match.
func WithDeleteRevision(revision uint64) DeleteOption {
	return storage.WithDeleteRevision(revision)
}

// =============================================================================
// Watch Options
// =============================================================================

// WatchOptions is an alias to storage.WatchOptions.
// Contains options for Watch operations.
type WatchOptions = storage.WatchOptions

// WatchOption is an alias to storage.WatchOption.
// Functional option for Watch operations.
type WatchOption = storage.WatchOption

// WithUpdatesOnly receives only future updates, skipping initial values.
// Without this option, Watch first sends current values for all matching keys,
// then sends a nil entry as a sentinel, then sends live updates.
func WithUpdatesOnly() WatchOption {
	return storage.WithWatchUpdatesOnly()
}

// WithIgnoreDeletes filters out delete markers from watch updates.
// Use this when you only care about active keys, not deletions.
func WithIgnoreDeletes() WatchOption {
	return storage.WithWatchIgnoreDeletes()
}

// WithMetaOnly retrieves only entry metadata without values.
// Use this for efficient tracking when you don't need the actual data.
func WithMetaOnly() WatchOption {
	return storage.WithWatchMetaOnly()
}

// WithResumeFromRevision resumes watching from a specific revision.
// Use this to continue watching after a disconnect or restart.
func WithResumeFromRevision(revision uint64) WatchOption {
	return storage.WithWatchResumeFromRevision(revision)
}
