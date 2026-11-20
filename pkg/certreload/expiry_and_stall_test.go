package certreload

import (
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestReloader_WarnsAboutAnExpiredCertificate: a certificate is checked when
// it is loaded. One that is never renewed was served past its expiry without
// a word; it is now warned about, at most once per ExpiryWarnInterval.
func TestReloader_WarnsAboutAnExpiredCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	// A short-lived certificate, so that the clock can pass its expiry
	// while the test CA (valid for a day) is still good.
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 10, usages: both,
		notBefore: time.Now().Add(-time.Hour), notAfter: time.Now().Add(time.Hour)})
	logger := &testLogger{}
	r, err := New(Config{CertFile: certFile, KeyFile: keyFile, Roots: ca.pool(), Usages: both, Name: "cluster TLS", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	clock := r.checked
	r.now = func() time.Time { return clock }
	expiredWarnings := func() int {
		logger.mu.Lock()
		defer logger.mu.Unlock()
		n := 0
		for _, w := range logger.warns {
			if strings.Contains(w, "has expired and its files were not renewed") {
				n++
			}
		}
		return n
	}

	// Valid: checks say nothing.
	clock = clock.Add(DefaultCheckInterval)
	r.Certificate()
	if expiredWarnings() != 0 {
		t.Fatal("warned about a valid certificate")
	}
	// Past its expiry, the files unchanged: one warning, then one per interval.
	clock = r.NotAfter().Add(time.Minute)
	for i := 0; i < 5; i++ {
		clock = clock.Add(DefaultCheckInterval)
		if r.Certificate() == nil {
			t.Fatal("no certificate returned")
		}
	}
	if got := expiredWarnings(); got != 1 {
		t.Fatalf("%d warnings within the interval, want 1", got)
	}
	clock = clock.Add(ExpiryWarnInterval)
	r.Certificate()
	if got := expiredWarnings(); got != 2 {
		t.Fatalf("%d warnings after the interval, want 2", got)
	}
	// Renewed: the warning stops.
	ca.issue(t, dir, leaf{serial: 11, usages: both, notBefore: time.Now().Add(-time.Hour), notAfter: time.Now().Add(20 * time.Hour)})
	clock = clock.Add(ExpiryWarnInterval)
	if s := r.Certificate().Leaf.SerialNumber.Int64(); s != 11 {
		t.Fatalf("serial = %d, want the renewed certificate", s)
	}
	clock = clock.Add(ExpiryWarnInterval)
	r.Certificate()
	if got := expiredWarnings(); got != 2 {
		t.Fatalf("%d warnings after the renewal, want still 2", got)
	}
}

// TestReloader_StalledFileSystem: a read of the files that does not return
// (a network file system that hangs) used to hold the reloader's lock, and
// with it every handshake. Only the caller that makes the check waits now.
func TestReloader_StalledFileSystem(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a named pipe")
	}
	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 10, usages: both})
	r, err := New(Config{CertFile: certFile, KeyFile: keyFile, Roots: ca.pool(), Usages: both, CheckInterval: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	// A named pipe in place of the certificate file: reading it blocks
	// until somebody writes.
	if err := os.Remove(certFile); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(certFile, 0o600); err != nil {
		t.Skipf("cannot make a named pipe: %v", err)
	}

	stalled := make(chan int64, 1)
	go func() { stalled <- r.Certificate().Leaf.SerialNumber.Int64() }()
	time.Sleep(100 * time.Millisecond) // the check is reading the pipe

	for i := 0; i < 20; i++ {
		got := make(chan int64, 1)
		go func() { got <- r.Certificate().Leaf.SerialNumber.Int64() }()
		select {
		case s := <-got:
			if s != 10 {
				t.Fatalf("serial = %d, want the current certificate", s)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a handshake waited behind the stalled read of the certificate file")
		}
	}

	// The file system answers again.
	w, err := os.OpenFile(certFile, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(certPEM); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	select {
	case s := <-stalled:
		if s != 10 {
			t.Fatalf("serial = %d after the read returned", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the check did not return once the file could be read")
	}
}
