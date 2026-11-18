// Package output flushes and closes the writers that the built-in middleware
// (access log, audit) write to, without closing the process's standard
// streams.
package output

import (
	"fmt"
	"io"
	"os"
)

// syncer is implemented by writers that can flush buffered data.
type syncer interface{ Sync() error }

// FlushAndClose flushes w and closes it, at Stop of a middleware that owns it.
//
//   - os.Stdout and os.Stderr are flushed only when they are regular files and
//     are never closed: the rest of the process keeps writing to them.
//   - Any other *os.File is flushed only when it is a regular file (fsync on a
//     pipe, terminal or socket fails with EINVAL and has nothing to flush),
//     then closed.
//   - Other writers are flushed when they implement Sync() error and closed
//     when they implement io.Closer.
//
// what names the writer in errors ("access log", "audit log").
func FlushAndClose(w io.Writer, what string) error {
	if f, ok := w.(*os.File); ok {
		if isRegular(f) {
			if err := f.Sync(); err != nil {
				return fmt.Errorf("failed to sync %s: %w", what, err)
			}
		}
		if IsStdStream(f) {
			return nil
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close %s: %w", what, err)
		}
		return nil
	}
	if s, ok := w.(syncer); ok {
		if err := s.Sync(); err != nil {
			return fmt.Errorf("failed to sync %s: %w", what, err)
		}
	}
	if c, ok := w.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return fmt.Errorf("failed to close %s: %w", what, err)
		}
	}
	return nil
}

// IsStdStream reports whether f is os.Stdout or os.Stderr.
func IsStdStream(f *os.File) bool {
	return f == os.Stdout || f == os.Stderr
}

func isRegular(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode().IsRegular()
}
