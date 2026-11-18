package accesslog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestStopKeepsStdStreams: an access log written to os.Stdout or os.Stderr
// does not close the stream at Stop, and a pipe (which cannot be synced) does
// not fail Stop (issue #55).
func TestStopKeepsStdStreams(t *testing.T) {
	for name, std := range map[string]**os.File{"stdout": &os.Stdout, "stderr": &os.Stderr} {
		t.Run(name, func(t *testing.T) {
			orig := *std
			t.Cleanup(func() { *std = orig })
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
			go func() { _, _ = r.Read(make([]byte, 1<<16)) }()

			for _, target := range []*os.File{file, w} {
				*std = target
				m, err := New(WithOutput(*std))
				if err != nil {
					t.Fatal(err)
				}
				if err := m.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := m.Stop(context.Background()); err != nil {
					t.Fatalf("Stop with %s as %s: %v", target.Name(), name, err)
				}
				if _, err := target.Write([]byte("after Stop\n")); err != nil {
					t.Fatalf("%s closed by Stop: %v", name, err)
				}
			}
		})
	}
}

// TestStopClosesOwnFile: a file of its own is still flushed and closed.
func TestStopClosesOwnFile(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "access.log"))
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(WithOutput(f))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := f.Write([]byte("x")); err == nil {
		t.Fatal("the access log file is still open after Stop")
	}
}
