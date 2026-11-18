//go:build integration
// +build integration

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/middleware/audit"
)

// auditConsumer depends on the audit middleware and saves entries through
// its audit-trail channel service.
type auditConsumer struct {
	name    string
	adapter audit.AuditAdapterPort
	err     error
}

func (m *auditConsumer) Name() string                { return m.name }
func (m *auditConsumer) Dependencies() []string      { return []string{"audit"} }
func (m *auditConsumer) Start(context.Context) error { return m.err }
func (m *auditConsumer) Stop(context.Context) error  { return nil }
func (m *auditConsumer) SetDependencyServiceContainer(dep string, c mono.ServiceContainer) {
	if dep == "audit" {
		m.adapter, m.err = audit.NewAuditAdapter(c, m.name)
	}
}

// TestAuditTrailServiceReachable: a module depending on the audit
// middleware gets its container and saves custom entries through the
// audit-trail channel service; with two consumers each gets the
// confirmations of its own requests (issue #55).
func TestAuditTrailServiceReachable(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "audit.log")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	am, err := audit.New(audit.WithOutput(f))
	if err != nil {
		t.Fatal(err)
	}
	app, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}), mono.WithNATSPort(freeTCPPort(t)))
	if err != nil {
		t.Fatal(err)
	}
	a, b := &auditConsumer{name: "consumer-a"}, &auditConsumer{name: "consumer-b"}
	for _, m := range []mono.Module{am, a, b} {
		if err := app.Register(m); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if app.Services("audit") == nil {
		t.Error(`app.Services("audit") = nil after Start`)
	}

	const perConsumer = 20
	var wg sync.WaitGroup
	for _, c := range []*auditConsumer{a, b} {
		wg.Add(1)
		go func(c *auditConsumer) {
			defer wg.Done()
			for i := 0; i < perConsumer; i++ {
				n, err := c.adapter.SaveAuditTrail(ctx, []audit.Entry{{ModuleName: c.name, Details: map[string]any{"i": i}}})
				if err != nil || n != 1 {
					t.Errorf("%s SaveAuditTrail #%d = %d, %v", c.name, i, n, err)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := map[string]int{}
	rf, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rf.Close() }()
	sc := bufio.NewScanner(rf)
	for sc.Scan() {
		var e audit.Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("decode %q: %v", sc.Text(), err)
		}
		if e.EventType == audit.EventCustomAuditTrail {
			got[e.ModuleName]++
		}
	}
	want := map[string]int{"consumer-a": perConsumer, "consumer-b": perConsumer}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("custom entries = %v, want %v", got, want)
	}
}
