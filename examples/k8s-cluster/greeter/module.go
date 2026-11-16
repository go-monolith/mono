// Package greeter is the single module of the k8s-cluster example. It shows
// the three ways the framework's services behave in a NATS cluster:
//
//   - hello (request-reply): every instance subscribes in the same queue
//     group, so a request is answered by exactly one instance;
//   - visits (stream consumer): a JetStream stream replicated to the servers
//     of the cluster (WithJetStreamDefaultReplicas), consumed through one
//     shared durable consumer, so each visit is handled once (at least once
//     across restarts), by whichever instance fetches it;
//   - tick (cron): a server-side schedule in a replicated stream, so exactly
//     one occurrence fires per tick for the whole cluster, not one per
//     instance.
//
// The module needs nothing cluster-specific: the framework waits for clustered
// JetStream before it creates the streams, and replicates them.
package greeter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/examples/k8s-cluster/greeter/contract"
)

// visitsStreamName is the name of the replicated visits stream.
const visitsStreamName = "GREETER_VISITS"

// Module greets callers and records every greeting in a replicated stream.
type Module struct {
	instance string
	logger   *slog.Logger
}

// Compile-time interface checks.
var (
	_ mono.Module                = (*Module)(nil)
	_ mono.ServiceProviderModule = (*Module)(nil)
)

// NewModule creates the greeter module. instance names this replica in
// replies and logs (the pod name).
func NewModule(instance string, logger *slog.Logger) *Module {
	return &Module{instance: instance, logger: logger}
}

// Name identifies the module.
func (m *Module) Name() string { return contract.ModuleName }

// RegisterServices registers the hello, visits and tick services. The streams
// leave Replicas at 0, so they get the framework default
// (WithJetStreamDefaultReplicas).
func (m *Module) RegisterServices(c mono.ServiceContainer) error {
	if err := contract.Hello.Register(c, m.hello); err != nil {
		return fmt.Errorf("register hello: %w", err)
	}

	visits := mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{
			Name:     visitsStreamName,
			Subjects: []string{contract.Visits.ToBase().Subject()},
			Storage:  mono.FileStorage,
			MaxMsgs:  10000,
		},
		Consumer: mono.ConsumerConfig{AckPolicy: mono.AckExplicitPolicy},
		Fetch:    mono.FetchConfig{BatchSize: 10, Timeout: 5 * time.Second},
	}
	if err := contract.Visits.Register(c, visits, m.onVisits); err != nil {
		return fmt.Errorf("register visits: %w", err)
	}

	if err := c.RegisterCronService("tick", mono.CronServiceConfig{
		Schedule: "@every 15s",
		Payload:  []byte(`{"event":"tick"}`),
	}, m.onTick); err != nil {
		return fmt.Errorf("register tick: %w", err)
	}
	return nil
}

// Start has nothing to prepare.
func (m *Module) Start(_ context.Context) error { return nil }

// Stop has nothing to release: the framework drains the module's services.
func (m *Module) Stop(_ context.Context) error { return nil }

func (m *Module) hello(_ context.Context, req contract.HelloRequest, _ *mono.Msg) (contract.HelloResponse, error) {
	name := req.Name
	if name == "" {
		name = "world"
	}
	return contract.HelloResponse{
		Message:  fmt.Sprintf("Hello, %s!", name),
		Instance: m.instance,
	}, nil
}

func (m *Module) onVisits(_ context.Context, visits []contract.Visit, msgs []*mono.Msg) error {
	for i, v := range visits {
		m.logger.Info("visit recorded",
			"handled_by", m.instance, "name", v.Name, "greeted_by", v.GreetedBy, "at", v.At.Format(time.RFC3339))
		if err := msgs[i].Ack(); err != nil {
			return fmt.Errorf("ack visit: %w", err)
		}
	}
	return nil
}

func (m *Module) onTick(_ context.Context, msg *mono.Msg) error {
	m.logger.Info("cron tick (one per occurrence for the whole cluster)",
		"handled_by", m.instance, "payload", string(msg.Data))
	return nil
}
