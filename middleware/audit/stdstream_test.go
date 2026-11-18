package audit

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestVerifyChainFrom: a log written with WithHashChaining(lastHash)
// verifies from that hash, not from an empty prev_hash (issue #55).
func TestVerifyChainFrom(t *testing.T) {
	first := NewHashChain("")
	var log1 []Entry
	for i := 0; i < 3; i++ {
		log1 = append(log1, first.AddEntry(Entry{Timestamp: time.Now().UTC(), EventType: EventModuleStarted, ModuleName: "m1"}))
	}
	last := log1[len(log1)-1].EntryHash

	second := NewHashChain(last)
	var log2 []Entry
	for i := 0; i < 2; i++ {
		log2 = append(log2, second.AddEntry(Entry{Timestamp: time.Now().UTC(), EventType: EventModuleStopped, ModuleName: "m2"}))
	}

	if err := VerifyChainFrom(log2, last); err != nil {
		t.Fatalf("VerifyChainFrom(continued log, last hash) = %v", err)
	}
	if err := VerifyChainFrom(log1, ""); err != nil {
		t.Fatalf("VerifyChainFrom(new log, \"\") = %v", err)
	}
	if err := VerifyChain(log2); err == nil || !strings.Contains(err.Error(), "hash chain broken at entry 0") {
		t.Fatalf("VerifyChain(continued log) = %v, want broken at entry 0", err)
	}
	if err := VerifyChainFrom(log2, log1[0].EntryHash); err == nil {
		t.Fatal("VerifyChainFrom with a wrong start hash passed")
	}
	if err := VerifyChainFrom(nil, last); err != nil {
		t.Fatalf("empty log: %v", err)
	}
}

// TestStopKeepsStdStreams: an audit log written to os.Stdout or os.Stderr
// does not close the stream at Stop, whether it is a file or a pipe
// (issue #55).
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
				if err := m.RegisterServices(newMockServiceContainer()); err != nil {
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
