package output

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFlushAndCloseRegularFile(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := FlushAndClose(f, "log"); err != nil {
		t.Fatalf("FlushAndClose: %v", err)
	}
	if _, err := f.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after close = %v, want os.ErrClosed", err)
	}
}

func TestFlushAndClosePipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	// fsync on a pipe fails with EINVAL; it must not fail Stop.
	if err := FlushAndClose(w, "log"); err != nil {
		t.Fatalf("FlushAndClose on a pipe: %v", err)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after close = %v, want os.ErrClosed", err)
	}
}

func TestFlushAndCloseKeepsStdStreams(t *testing.T) {
	for name, f := range map[string]**os.File{"stdout": &os.Stdout, "stderr": &os.Stderr} {
		t.Run(name, func(t *testing.T) {
			orig := *f
			t.Cleanup(func() { *f = orig })

			// The standard stream redirected to a file, then to a pipe.
			file, err := os.Create(filepath.Join(t.TempDir(), name))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close(); _ = w.Close() }()

			for _, target := range []*os.File{file, w} {
				*f = target
				if !IsStdStream(target) {
					t.Fatal("not recognized as a standard stream")
				}
				if err := FlushAndClose(target, "log"); err != nil {
					t.Fatalf("FlushAndClose(%s): %v", target.Name(), err)
				}
				if _, err := target.Write([]byte("still open\n")); err != nil {
					t.Fatalf("write after FlushAndClose: %v", err)
				}
			}
		})
	}
}

type syncCloser struct {
	bytes.Buffer
	synced, closed bool
	syncErr        error
}

func (s *syncCloser) Sync() error  { s.synced = true; return s.syncErr }
func (s *syncCloser) Close() error { s.closed = true; return nil }

func TestFlushAndCloseOtherWriters(t *testing.T) {
	w := &syncCloser{}
	if err := FlushAndClose(w, "log"); err != nil || !w.synced || !w.closed {
		t.Fatalf("err=%v synced=%v closed=%v", err, w.synced, w.closed)
	}
	failing := &syncCloser{syncErr: errors.New("disk full")}
	if err := FlushAndClose(failing, "audit log"); err == nil || !strings.Contains(err.Error(), "failed to sync audit log: disk full") {
		t.Fatalf("err = %v", err)
	}
	if err := FlushAndClose(&bytes.Buffer{}, "log"); err != nil {
		t.Fatalf("plain writer: %v", err)
	}
}
