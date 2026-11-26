//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	fsjetstream "github.com/go-monolith/mono/plugin/fs-jetstream"
	kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
)

// jsEventStreamConsumerModule registers an event stream consumer.
type jsEventStreamConsumerModule struct{}

func (m *jsEventStreamConsumerModule) Name() string                { return "billing" }
func (m *jsEventStreamConsumerModule) Start(context.Context) error { return nil }
func (m *jsEventStreamConsumerModule) Stop(context.Context) error  { return nil }

func (m *jsEventStreamConsumerModule) RegisterEventConsumers(r mono.EventRegistry) error {
	return r.RegisterEventStreamConsumer(mono.BaseEventDefinition{
		ModuleName: "order", Name: "Placed", Subject: "events.order.v1.placed", Version: "v1",
	}, mono.StreamConsumerConfig{Stream: mono.StreamConfig{Name: "ORDER_EVENTS_NO_JS"}},
		func(context.Context, []*mono.Msg) error { return nil }, m)
}

// TestJetStreamRequired_FailFastWithoutJetStream checks that a module
// registering a stream consumer service, cron service or event stream
// consumer, or a plugin that needs
// JetStream, fails Start at once with ErrJetStreamNotEnabled on an application
// without JetStream, instead of waiting for the JetStream ready timeout.
func TestJetStreamRequired_FailFastWithoutJetStream(t *testing.T) {
	cases := map[string]func(t *testing.T, fw mono.MonoApplication){
		"stream consumer service": func(t *testing.T, fw mono.MonoApplication) {
			if err := fw.Register(newStreamConsumerModule("orders", mono.StreamConsumerConfig{
				Stream: mono.StreamConfig{Name: "ORDERS_NO_JS"},
			}, 1)); err != nil {
				t.Fatalf("Register failed: %v", err)
			}
		},
		"cron service": func(t *testing.T, fw mono.MonoApplication) {
			if err := fw.Register(newCronTestModule("reports", "rollup", mono.CronServiceConfig{
				Schedule: "@every 1s",
			}, 1)); err != nil {
				t.Fatalf("Register failed: %v", err)
			}
		},
		"event stream consumer": func(t *testing.T, fw mono.MonoApplication) {
			if err := fw.Register(&jsEventStreamConsumerModule{}); err != nil {
				t.Fatalf("Register failed: %v", err)
			}
		},
		"kv-jetstream plugin": func(t *testing.T, fw mono.MonoApplication) {
			kv, err := kvjetstream.New(kvjetstream.Config{
				Buckets: []kvjetstream.BucketConfig{{Name: "sessions"}},
			})
			if err != nil {
				t.Fatalf("kvjetstream.New failed: %v", err)
			}
			if err := fw.RegisterPlugin(kv, "kv"); err != nil {
				t.Fatalf("RegisterPlugin failed: %v", err)
			}
		},
		"fs-jetstream plugin": func(t *testing.T, fw mono.MonoApplication) {
			fs, err := fsjetstream.New(fsjetstream.Config{
				Buckets: []fsjetstream.BucketConfig{{Name: "documents"}},
			})
			if err != nil {
				t.Fatalf("fsjetstream.New failed: %v", err)
			}
			if err := fw.RegisterPlugin(fs, "fs"); err != nil {
				t.Fatalf("RegisterPlugin failed: %v", err)
			}
		},
	}
	for name, register := range cases {
		t.Run(name, func(t *testing.T) {
			fw, err := mono.NewMonoApplication(
				mono.WithCustomLogger(&noOpsLogger{}),
				mono.WithNATSDontListen(),
				mono.WithNATSInProcessConn(),
				// JetStream intentionally NOT enabled.
			)
			if err != nil {
				t.Fatalf("NewMonoApplication failed: %v", err)
			}
			defer fw.Stop(context.Background())
			register(t, fw)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			began := time.Now()
			err = fw.Start(ctx)
			elapsed := time.Since(began)
			if !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
				t.Fatalf("Start err = %v, want ErrJetStreamNotEnabled", err)
			}
			if !errors.Is(err, monoerrors.ErrModuleStartFailed) {
				t.Errorf("Start err = %v, want it to wrap ErrModuleStartFailed", err)
			}
			if elapsed > 5*time.Second {
				t.Errorf("Start took %s, want it to fail fast", elapsed)
			}
		})
	}
}
