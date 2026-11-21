package accesslog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// TestRestartAfterRollback: the rollback of a failed application Start leaves
// the log file open, and the module logs again after the next Start. Before,
// the shutdown flag stayed set and the file was closed, so nothing was
// logged, without an error.
func TestRestartAfterRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(WithOutput(f))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	call := func(service string) {
		t.Helper()
		h := m.wrapRequestReplyHandler(func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }, "orders", service)
		if _, err := h(ctx, &types.Msg{}); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	call("first")
	if err := m.RollbackStart(ctx); err != nil {
		t.Fatalf("RollbackStart: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start after a rollback: %v", err)
	}
	call("second")
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"service=first", "service=second"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("log lacks %q:\n%s", want, data)
		}
	}

	// Stop closed the file: the module cannot be started again, and says so.
	if err := m.Start(ctx); !errors.Is(err, ErrOutputClosed) {
		t.Fatalf("Start after Stop = %v, want ErrOutputClosed", err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// TestRestartAfterStop_WriterNotClosed: a writer Stop does not close keeps
// the module restartable after Stop as well.
func TestRestartAfterStop_WriterNotClosed(t *testing.T) {
	buf := &syncWriter{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	h := m.wrapRequestReplyHandler(func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }, "orders", "place")
	if _, err := h(ctx, &types.Msg{}); err != nil {
		t.Fatal(err)
	}
	if lines := buf.Lines(); len(lines) != 1 {
		t.Fatalf("%d lines logged after the restart, want 1", len(lines))
	}
}

// TestChannelProxiesOfTwoModulesWithOneServiceName: two modules may each
// have a channel service of the same name. Kept by the service name alone,
// the second proxy replaced the first, whose goroutines Stop then never
// ended.
func TestChannelProxiesOfTwoModulesWithOneServiceName(t *testing.T) {
	m, err := New(WithOutput(&syncWriter{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, module := range []string{"orders", "billing"} {
		m.wrapChannelService(make(chan *types.Msg, 1), make(chan *types.Msg, 1), module, "events")
	}
	// The same service wrapped again (a module that registers twice): the
	// proxy it replaces is stopped at once.
	m.channelProxiesMu.Lock()
	first := m.channelProxies["orders/events"]
	m.channelProxiesMu.Unlock()
	m.wrapChannelService(make(chan *types.Msg, 1), make(chan *types.Msg, 1), "orders", "events")
	if first == nil || first.ctx.Err() == nil {
		t.Fatal("the replaced proxy was not stopped")
	}
	// It is kept for Stop, which waits for its goroutines with the others.
	m.channelProxiesMu.Lock()
	replaced := len(m.replacedProxies)
	m.channelProxiesMu.Unlock()
	if replaced != 1 {
		t.Fatalf("%d replaced proxies kept, want 1", replaced)
	}

	m.channelProxiesMu.Lock()
	proxies := make([]*channelProxy, 0, len(m.channelProxies))
	for _, p := range m.channelProxies {
		proxies = append(proxies, p)
	}
	m.channelProxiesMu.Unlock()
	if len(proxies) != 2 {
		t.Fatalf("%d proxies kept for two modules, want 2", len(proxies))
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() {
		first.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("the goroutines of the replaced proxy were still running after Stop")
	}
	m.channelProxiesMu.Lock()
	replaced = len(m.replacedProxies)
	m.channelProxiesMu.Unlock()
	if replaced != 0 {
		t.Errorf("%d replaced proxies kept after Stop", replaced)
	}
	for _, p := range proxies {
		if p.ctx.Err() == nil {
			t.Errorf("the proxy of %s/%s was not stopped", p.moduleName, p.serviceName)
		}
	}
}

// TestChannelProxyForwardsANilMessage: a nil message on a channel service
// has nothing to log. The proxies used to dereference it in a goroutine
// nothing recovers; they pass it on, and the channel router drops it.
func TestChannelProxyForwardsANilMessage(t *testing.T) {
	buf := &syncWriter{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatal(err)
	}
	in, out := make(chan *types.Msg, 2), make(chan *types.Msg, 2)
	proxyIn, proxyOut := m.wrapChannelService(in, out, "orders", "jobs")
	defer func() { _ = m.Stop(context.Background()) }()

	// Towards the provider.
	proxyIn <- nil
	proxyIn <- &types.Msg{Data: []byte("request")}
	// From the provider.
	out <- nil
	out <- &types.Msg{Data: []byte("response")}

	for name, ch := range map[string]chan *types.Msg{"provider": in, "consumer": proxyOut} {
		for i, wantNil := range []bool{true, false} {
			select {
			case msg := <-ch:
				if (msg == nil) != wantNil {
					t.Fatalf("%s: message %d = %v", name, i, msg)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: message %d was not forwarded", name, i)
			}
		}
	}
	if lines := buf.Lines(); len(lines) != 2 {
		t.Fatalf("%d lines logged, want one per real message", len(lines))
	}
}
