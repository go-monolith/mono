//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// clusterReadyModule registers a stream consumer service ("jobs") and a cron
// service ("tick") and counts what they handle. Every node of a test cluster
// registers the same module, as the replicas of one application do.
type clusterReadyModule struct {
	node      string
	ticks     *atomic.Int64
	jobs      *jobLog
	startWait time.Duration // extra time Start takes
	withCron  bool
	stream    string
}

// jobLog records the payloads handled across all nodes.
type jobLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *jobLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, s)
}

func (l *jobLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.seen)
}

func (m *clusterReadyModule) Name() string { return "jobs" }

func (m *clusterReadyModule) Start(ctx context.Context) error {
	if m.startWait > 0 {
		select {
		case <-time.After(m.startWait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *clusterReadyModule) Stop(context.Context) error { return nil }

func (m *clusterReadyModule) RegisterServices(c mono.ServiceContainer) error {
	stream := m.stream
	if stream == "" {
		stream = "JOBS"
	}
	if err := c.RegisterStreamConsumerService("work", mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{Name: stream, Subjects: []string{"services.jobs.work"}},
		Fetch:  mono.FetchConfig{BatchSize: 10, Timeout: 500 * time.Millisecond},
	}, func(_ context.Context, msgs []*mono.Msg) error {
		for _, msg := range msgs {
			m.jobs.add(string(msg.Data))
			if err := msg.Ack(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if !m.withCron {
		return nil
	}
	return c.RegisterCronService("tick", mono.CronServiceConfig{Schedule: "@every 1s"}, func(context.Context, *mono.Msg) error {
		m.ticks.Add(1)
		return nil
	})
}

// clusterSpec is one node of a test cluster.
type clusterSpec struct {
	name        string
	clientPort  int
	clusterPort int
}

// newClusterSpecs returns n nodes on free ports and the route list every node
// uses (all nodes, its own included).
func newClusterSpecs(t *testing.T, n int) ([]clusterSpec, []string) {
	t.Helper()
	specs := make([]clusterSpec, n)
	routes := make([]string, n)
	for i := range specs {
		specs[i] = clusterSpec{name: fmt.Sprintf("node-%d", i), clientPort: freePort(t), clusterPort: freePort(t)}
		routes[i] = fmt.Sprintf("nats://127.0.0.1:%d", specs[i].clusterPort)
	}
	return specs, routes
}

// newClusterApp creates the application of one node. NewMonoApplication
// starts the embedded server, which begins forming the cluster; Start is left
// to the test. The app is stopped at the end of the test.
func newClusterApp(t *testing.T, spec clusterSpec, routes []string, storeDir string, m *clusterReadyModule, extra ...mono.MonoFrameworkOption) mono.MonoApplication {
	t.Helper()
	opts := append([]mono.MonoFrameworkOption{
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(spec.clientPort),
		mono.WithNATSServerName(spec.name),
		mono.WithNATSClustering("ready-test", "127.0.0.1", spec.clusterPort, routes),
		mono.WithNATSClusterAuth("route", "integration-secret"),
		mono.WithJetStreamStorageDir(filepath.Join(storeDir, spec.name)),
		mono.WithLogLevel(mono.LogLevelWarn),
	}, extra...)
	app, err := mono.NewMonoApplication(opts...)
	if err != nil {
		t.Fatalf("%s: NewMonoApplication: %v", spec.name, err)
	}
	m.node = spec.name
	if err := app.Register(m); err != nil {
		t.Fatalf("%s: Register: %v", spec.name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = app.Stop(ctx)
	})
	return app
}

// clusterJS connects a client to a node and returns its JetStream context.
func clusterJS(t *testing.T, port int) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

// startAll starts the apps concurrently, as the pods of a StatefulSet with
// parallel pod management do, and returns each Start's error.
func startAll(ctx context.Context, apps []mono.MonoApplication) []error {
	errs := make([]error, len(apps))
	var wg sync.WaitGroup
	for i, app := range apps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = app.Start(ctx)
		}()
	}
	wg.Wait()
	return errs
}

// TestJetStreamCluster_ColdStartTogether starts three nodes at once with
// replicated framework streams: every Start succeeds (no node fails because
// the cluster has no leader yet), the streams have three replicas, each job is
// handled once, and the cron schedule fires once per occurrence.
func TestJetStreamCluster_ColdStartTogether(t *testing.T) {
	specs, routes := newClusterSpecs(t, 3)
	storeDir := t.TempDir()
	ticks := &atomic.Int64{}
	jobs := &jobLog{}
	apps := make([]mono.MonoApplication, len(specs))
	for i, spec := range specs {
		apps[i] = newClusterApp(t, spec, routes, storeDir,
			&clusterReadyModule{ticks: ticks, jobs: jobs, withCron: true},
			mono.WithJetStreamDefaultReplicas(3))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i, err := range startAll(ctx, apps) {
		if err != nil {
			t.Fatalf("%s: Start: %v", specs[i].name, err)
		}
	}

	js := clusterJS(t, specs[0].clientPort)
	for _, name := range []string{"JOBS", "MONO_CRON_jobs_tick"} {
		info := streamInfo(ctx, t, js, name)
		if info.Config.Replicas != 3 {
			t.Errorf("stream %s has %d replicas, want 3", name, info.Config.Replicas)
		}
	}

	for i := 1; i <= 9; i++ {
		if _, err := js.Publish(ctx, "services.jobs.work", fmt.Appendf(nil, "job-%d", i)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 10*time.Second, func() bool { return len(jobs.all()) >= 9 })
	time.Sleep(time.Second) // catch duplicates
	got := jobs.all()
	slices.Sort(got)
	if len(got) != 9 || len(slices.Compact(slices.Clone(got))) != 9 {
		t.Errorf("jobs handled = %v, want each of 9 once", got)
	}

	before := ticks.Load()
	time.Sleep(4 * time.Second)
	if fired := ticks.Load() - before; fired < 2 || fired > 6 {
		t.Errorf("cron fired %d times in 4s across 3 nodes, want about 4 (once per occurrence)", fired)
	}
}

// TestJetStreamCluster_StartWaitsForPeer starts one node of a two-node cluster
// alone: its Start waits (JetStream has no quorum) instead of failing, and
// both nodes start once the second one comes up.
func TestJetStreamCluster_StartWaitsForPeer(t *testing.T) {
	specs, routes := newClusterSpecs(t, 2)
	storeDir := t.TempDir()
	jobs := &jobLog{}
	first := newClusterApp(t, specs[0], routes, storeDir, &clusterReadyModule{ticks: &atomic.Int64{}, jobs: jobs},
		mono.WithJetStreamDefaultReplicas(2))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	firstErr := make(chan error, 1)
	start := time.Now()
	go func() { firstErr <- first.Start(ctx) }()

	select {
	case err := <-firstErr:
		t.Fatalf("the first node's Start returned before its peer existed: %v", err)
	case <-time.After(3 * time.Second):
	}

	second := newClusterApp(t, specs[1], routes, storeDir, &clusterReadyModule{ticks: &atomic.Int64{}, jobs: jobs},
		mono.WithJetStreamDefaultReplicas(2))
	if err := second.Start(ctx); err != nil {
		t.Fatalf("second node: Start: %v", err)
	}
	if err := <-firstErr; err != nil {
		t.Fatalf("first node: Start: %v", err)
	}
	if waited := time.Since(start); waited < 3*time.Second {
		t.Errorf("first node started after %s, want it to have waited for its peer", waited)
	}

	js := clusterJS(t, specs[1].clientPort)
	if _, err := js.Publish(ctx, "services.jobs.work", []byte("after-start")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return len(jobs.all()) == 1 })
}

// TestJetStreamCluster_ReadyTimeout checks that a node whose peers never come
// fails its Start after the configured timeout, naming the option.
func TestJetStreamCluster_ReadyTimeout(t *testing.T) {
	specs, routes := newClusterSpecs(t, 2)
	app := newClusterApp(t, specs[0], routes, t.TempDir(), &clusterReadyModule{ticks: &atomic.Int64{}, jobs: &jobLog{}},
		mono.WithJetStreamReadyTimeout(2*time.Second))

	start := time.Now()
	err := app.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "WithJetStreamReadyTimeout") {
		t.Fatalf("Start error = %v, want a JetStream ready timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("Start took %s, want about the 2s ready timeout", elapsed)
	}
}

// TestJetStreamCluster_ReplicatedCronSurvivesLeaderLoss stops the node that
// leads a replicated cron stream: the schedule keeps firing on the others.
func TestJetStreamCluster_ReplicatedCronSurvivesLeaderLoss(t *testing.T) {
	specs, routes := newClusterSpecs(t, 3)
	storeDir := t.TempDir()
	ticks := &atomic.Int64{}
	apps := make([]mono.MonoApplication, len(specs))
	for i, spec := range specs {
		apps[i] = newClusterApp(t, spec, routes, storeDir,
			&clusterReadyModule{ticks: ticks, jobs: &jobLog{}, withCron: true},
			mono.WithJetStreamDefaultReplicas(3))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i, err := range startAll(ctx, apps) {
		if err != nil {
			t.Fatalf("%s: Start: %v", specs[i].name, err)
		}
	}

	js := clusterJS(t, specs[0].clientPort)
	leader := streamInfo(ctx, t, js, "MONO_CRON_jobs_tick").Cluster.Leader
	idx := slices.IndexFunc(specs, func(s clusterSpec) bool { return s.name == leader })
	if idx < 0 {
		t.Fatalf("cron stream leader %q is not a test node", leader)
	}
	stopCtx, stopCancel := context.WithTimeout(ctx, 20*time.Second)
	if err := apps[idx].Stop(stopCtx); err != nil {
		t.Fatalf("stop %s: %v", leader, err)
	}
	stopCancel()

	// A new leader is elected among the other two, then the schedule fires.
	before := ticks.Load()
	waitFor(t, 20*time.Second, func() bool { return ticks.Load()-before >= 3 })
}

// TestStreamConsumer_ConvertsNonDurableConsumer starts an application against
// the state an earlier framework version left: the stream consumer service's
// consumer exists with a Name only (non-durable to the server) and has handled
// part of the stream. A module takes longer to start than the non-durable
// inactivity threshold, which since nats-server v2.14.10 runs from the server
// start. The consumer must be kept, converted to a durable in place, and
// deliver only what it had not handled.
func TestStreamConsumer_ConvertsNonDurableConsumer(t *testing.T) {
	storeDir := t.TempDir()
	port := freePort(t)
	jobs := &jobLog{}
	run := func(m *clusterReadyModule) mono.MonoApplication {
		app, err := mono.NewMonoApplication(
			mono.WithNATSHost("127.0.0.1"),
			mono.WithNATSPort(port),
			mono.WithJetStreamStorageDir(storeDir),
			mono.WithLogLevel(mono.LogLevelWarn),
		)
		if err != nil {
			t.Fatalf("NewMonoApplication: %v", err)
		}
		if m != nil {
			if err := app.Register(m); err != nil {
				t.Fatalf("Register: %v", err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := app.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return app
	}
	stop := func(app mono.MonoApplication) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}

	// Earlier version's state: stream, Name-only consumer, 1-5 handled.
	app := run(nil)
	js := clusterJS(t, port)
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "LEGACYJOBS", Subjects: []string{"services.jobs.work"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if _, err := js.CreateOrUpdateConsumer(ctx, "LEGACYJOBS", jetstream.ConsumerConfig{
		Name: "jobs-work", AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "services.jobs.work",
	}); err != nil {
		t.Fatalf("create legacy consumer: %v", err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := js.Publish(ctx, "services.jobs.work", fmt.Appendf(nil, "job-%d", i)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	legacy, err := js.Consumer(ctx, "LEGACYJOBS", "jobs-work")
	if err != nil {
		t.Fatalf("legacy consumer: %v", err)
	}
	batch, err := legacy.Fetch(5, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	for msg := range batch.Messages() {
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	stop(app)

	// Upgrade: a module whose Start outlasts the 5s threshold.
	app = run(&clusterReadyModule{ticks: &atomic.Int64{}, jobs: jobs, startWait: 7 * time.Second, stream: "LEGACYJOBS"})
	js = clusterJS(t, port)
	waitFor(t, 10*time.Second, func() bool { return len(jobs.all()) >= 5 })
	time.Sleep(time.Second)
	got := jobs.all()
	slices.Sort(got)
	want := []string{"job-10", "job-6", "job-7", "job-8", "job-9"}
	if !slices.Equal(got, want) {
		t.Errorf("handled %v, want only the unhandled %v", got, want)
	}
	info, err := js.Consumer(ctx, "LEGACYJOBS", "jobs-work")
	if err != nil {
		t.Fatalf("consumer after upgrade: %v", err)
	}
	if cfg := info.CachedInfo().Config; cfg.Durable != "jobs-work" || cfg.InactiveThreshold != 0 {
		t.Errorf("consumer Durable=%q InactiveThreshold=%s, want durable without threshold", cfg.Durable, cfg.InactiveThreshold)
	}
	stop(app)

	// Idle longer than the old threshold, then restart: nothing is replayed.
	time.Sleep(6 * time.Second)
	app = run(&clusterReadyModule{ticks: &atomic.Int64{}, jobs: jobs, stream: "LEGACYJOBS"})
	defer stop(app)
	time.Sleep(2 * time.Second)
	if n := len(jobs.all()); n != 5 {
		t.Errorf("after an idle restart %d jobs were handled in total, want 5 (no replay)", n)
	}
}

func streamInfo(ctx context.Context, t *testing.T, js jetstream.JetStream, name string) *jetstream.StreamInfo {
	t.Helper()
	var info *jetstream.StreamInfo
	waitFor(t, 15*time.Second, func() bool {
		s, err := js.Stream(ctx, name)
		if err != nil {
			return false
		}
		info, err = s.Info(ctx)
		return err == nil && info.Cluster != nil && info.Cluster.Leader != ""
	})
	return info
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestJetStreamCluster_HealthReportsQuorum checks FrameworkHealth.JetStream on
// the surviving node of a two-node cluster: available while both run, not
// available once its peer stops (no quorum), available again when the peer
// comes back. Healthy stays true throughout: JetStream is reported, not
// counted.
func TestJetStreamCluster_HealthReportsQuorum(t *testing.T) {
	specs, routes := newClusterSpecs(t, 2)
	storeDir := t.TempDir()
	newNode := func(i int) mono.MonoApplication {
		return newClusterApp(t, specs[i], routes, storeDir, &clusterReadyModule{ticks: &atomic.Int64{}, jobs: &jobLog{}})
	}
	apps := []mono.MonoApplication{newNode(0), newNode(1)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for i, err := range startAll(ctx, apps) {
		if err != nil {
			t.Fatalf("%s: Start: %v", specs[i].name, err)
		}
	}

	survivor := apps[0]
	available := func(want bool) func() bool {
		return func() bool {
			h := survivor.Health(ctx)
			if !h.Healthy {
				t.Fatalf("Healthy must not depend on JetStream quorum: %+v", h)
			}
			return h.JetStream != nil && h.JetStream.Clustered && h.JetStream.Available == want
		}
	}
	waitFor(t, 30*time.Second, available(true))
	if h := survivor.Health(ctx).JetStream; h.ClusterSize != 2 || h.MetaLeader == "" {
		t.Errorf("JetStream health with both nodes = %+v", h)
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, 20*time.Second)
	if err := apps[1].Stop(stopCtx); err != nil {
		t.Fatalf("stop peer: %v", err)
	}
	stopCancel()
	waitFor(t, 60*time.Second, available(false))
	if msg := survivor.Health(ctx).JetStream.Message; !strings.Contains(msg, "no meta leader") {
		t.Errorf("message without quorum = %q", msg)
	}

	back := newNode(1)
	if err := back.Start(ctx); err != nil {
		t.Fatalf("restart peer: %v", err)
	}
	waitFor(t, 60*time.Second, available(true))
}
