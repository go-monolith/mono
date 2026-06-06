package container

import (
	"context"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// newBoundCronContainer returns a container bound to a module with an EventBus
// set, ready for cron registration.
func newBoundCronContainer(t *testing.T, moduleName string) *serviceContainer {
	t.Helper()
	c := NewServiceContainer(&mockLogger{}).(*serviceContainer)
	if err := c.BindModule(&mockModule{name: moduleName}); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}
	c.SetEventBus(&mockEventBusWithJetStream{jetStream: &mockJetStream{}})
	return c
}

func noopCronHandler(_ context.Context, _ *types.Msg) error { return nil }

func TestRegisterCronService_Valid(t *testing.T) {
	c := newBoundCronContainer(t, "reports")

	cfg := types.CronServiceConfig{
		Schedule: "@daily",
		Payload:  []byte(`{"job":"rollup"}`),
	}
	if err := c.RegisterCronService("nightly-rollup", cfg, noopCronHandler); err != nil {
		t.Fatalf("RegisterCronService failed: %v", err)
	}

	if !c.Has("nightly-rollup") {
		t.Fatal("service should be registered")
	}

	var entry *types.ServiceEntry
	for _, e := range c.Entries() {
		if e.Name == "nightly-rollup" {
			entry = e
		}
	}
	if entry == nil {
		t.Fatal("service entry not found")
	}
	if entry.Type != types.ServiceTypeCron {
		t.Errorf("expected ServiceTypeCron, got %v", entry.Type)
	}
	if entry.CronConfig == nil || entry.CronHandler == nil {
		t.Fatal("CronConfig and CronHandler must be populated")
	}
	if want := "services.reports.nightly-rollup"; entry.Subject != want {
		t.Errorf("target subject = %q, want %q", entry.Subject, want)
	}
	if want := "_framework.cron.reports.nightly-rollup.schedule"; entry.ScheduleSubject != want {
		t.Errorf("schedule subject = %q, want %q", entry.ScheduleSubject, want)
	}
}

func TestRegisterCronService_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  types.CronServiceConfig
		handler types.CronHandler
		wantErr bool
	}{
		{
			name:    "nil handler",
			config:  types.CronServiceConfig{Schedule: "@daily"},
			handler: nil,
			wantErr: true,
		},
		{
			name:    "empty schedule",
			config:  types.CronServiceConfig{Payload: []byte("x")},
			handler: noopCronHandler,
			wantErr: true,
		},
		{
			name:    "payload and source mutually exclusive",
			config:  types.CronServiceConfig{Schedule: "@daily", Payload: []byte("x"), SourceSubject: "events.metrics"},
			handler: noopCronHandler,
			wantErr: true,
		},
		{
			name:    "invalid timezone",
			config:  types.CronServiceConfig{Schedule: "@daily", TimeZone: "Mars/Phobos"},
			handler: noopCronHandler,
			wantErr: true,
		},
		{
			name:    "ttl below minimum",
			config:  types.CronServiceConfig{Schedule: "@daily", TTL: 500 * time.Millisecond},
			handler: noopCronHandler,
			wantErr: true,
		},
		{
			name:    "valid source only",
			config:  types.CronServiceConfig{Schedule: "@every 1m", SourceSubject: "events.metrics"},
			handler: noopCronHandler,
			wantErr: false,
		},
		{
			name:    "valid with timezone and ttl",
			config:  types.CronServiceConfig{Schedule: "0 0 * * *", TimeZone: "UTC", TTL: time.Minute, Payload: []byte("x")},
			handler: noopCronHandler,
			wantErr: false,
		},
		{
			name:    "valid deprecated",
			config:  types.CronServiceConfig{Schedule: "@daily", Payload: []byte("x"), Deprecated: true},
			handler: noopCronHandler,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newBoundCronContainer(t, "mod")
			err := c.RegisterCronService("job", tt.config, tt.handler)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRegisterCronService_RequiresEventBus(t *testing.T) {
	c := NewServiceContainer(&mockLogger{}).(*serviceContainer)
	if err := c.BindModule(&mockModule{name: "mod"}); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}
	// No SetEventBus call.

	err := c.RegisterCronService("job", types.CronServiceConfig{Schedule: "@daily"}, noopCronHandler)
	if err == nil {
		t.Fatal("expected error when EventBus is not set")
	}
}

func TestRegisterCronService_DuplicateName(t *testing.T) {
	c := newBoundCronContainer(t, "mod")
	cfg := types.CronServiceConfig{Schedule: "@daily", Payload: []byte("x")}
	if err := c.RegisterCronService("job", cfg, noopCronHandler); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}
	if err := c.RegisterCronService("job", cfg, noopCronHandler); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
}

func TestRegisterCronService_SourceEqualsServiceSubject(t *testing.T) {
	c := newBoundCronContainer(t, "mod")
	cfg := types.CronServiceConfig{
		Schedule:      "@daily",
		SourceSubject: "services.mod.job", // identical to the computed service subject
	}
	if err := c.RegisterCronService("job", cfg, noopCronHandler); err == nil {
		t.Fatal("expected error when SourceSubject equals the service subject")
	}
}

func TestFormatServiceType_Cron(t *testing.T) {
	if got := types.FormatServiceType(types.ServiceTypeCron); got != "cron" {
		t.Errorf("FormatServiceType(ServiceTypeCron) = %q, want %q", got, "cron")
	}
}

func TestCronSubjectHelpers(t *testing.T) {
	if got := types.CronStreamName("mod", "job"); got != "MONO_CRON_mod_job" {
		t.Errorf("CronStreamName = %q", got)
	}
	if got := types.CronScheduleSubject("mod", "job"); got != "_framework.cron.mod.job.schedule" {
		t.Errorf("CronScheduleSubject = %q", got)
	}
	if got := types.CronControlSubject("mod", "job"); got != "_framework.cron.mod.job.control" {
		t.Errorf("CronControlSubject = %q", got)
	}
	if got := types.CronInternalSubjectsWildcard("mod", "job"); got != "_framework.cron.mod.job.>" {
		t.Errorf("CronInternalSubjectsWildcard = %q", got)
	}
}
