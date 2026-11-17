//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
)

type sysAcctPayload struct {
	N int `json:"n"`
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// sysAcctApp runs one application with a stream consumer service on store,
// listening on port, and records the payloads it handles.
type sysAcctApp struct {
	app      mono.MonoApplication
	mu       sync.Mutex
	received []int
}

func startSysAcctApp(t *testing.T, store string, port int, extra ...mono.MonoFrameworkOption) *sysAcctApp {
	t.Helper()
	def := helper.StreamConsumerService[sysAcctPayload]("ledger", "entries")
	a := &sysAcctApp{}
	module := &testModule{name: "ledger"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return def.Register(container, mono.StreamConsumerConfig{
			Stream:   mono.StreamConfig{Name: "LEDGER_ENTRIES", Subjects: []string{def.ToBase().Subject()}},
			Consumer: mono.ConsumerConfig{AckPolicy: mono.AckExplicitPolicy},
			Fetch:    mono.FetchConfig{BatchSize: 10, Timeout: 200 * time.Millisecond},
		}, func(_ context.Context, payloads []sysAcctPayload, msgs []*mono.Msg) error {
			a.mu.Lock()
			defer a.mu.Unlock()
			for i, p := range payloads {
				a.received = append(a.received, p.N)
				if err := msgs[i].Ack(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	opts := append([]mono.MonoFrameworkOption{
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(store),
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(port),
		mono.WithNATSInProcessConn(),
	}, extra...)
	app, err := mono.NewMonoApplication(opts...)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	if err := app.Register(module); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	a.app = app
	return a
}

func (a *sysAcctApp) publish(t *testing.T, n int) {
	t.Helper()
	def := helper.StreamConsumerService[sysAcctPayload]("ledger", "entries")
	if _, err := def.Publish(context.Background(), a.app.Services("ledger"), sysAcctPayload{N: n}); err != nil {
		t.Fatalf("publish %d: %v", n, err)
	}
}

func (a *sysAcctApp) waitReceived(t *testing.T, want int) []int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		n := len(a.received)
		a.mu.Unlock()
		if n >= want {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // a replay would arrive meanwhile
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int(nil), a.received...)
}

// TestSystemAccount_ExistingStoreAndClients covers issue #39: enabling
// WithNATSSystemAccount on an existing store keeps the streams, the consumer
// positions and every client without credentials working (they stay in the
// global account), and gives the system account user access to the $SYS
// requests operator tooling uses.
func TestSystemAccount_ExistingStoreAndClients(t *testing.T) {
	store := t.TempDir()
	port := freeTCPPort(t)
	url := "nats://127.0.0.1:" + strconv.Itoa(port)

	// 1. Without the option: store 3 entries and handle them.
	before := startSysAcctApp(t, store, port)
	for n := 1; n <= 3; n++ {
		before.publish(t, n)
	}
	if got := before.waitReceived(t, 3); len(got) != 3 {
		t.Fatalf("before: received %v, want 3 entries", got)
	}
	if err := before.app.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// 2. Same store, with the option.
	after := startSysAcctApp(t, store, port, mono.WithNATSSystemAccount("admin", "s3cret"))
	t.Cleanup(func() { _ = after.app.Stop(context.Background()) })
	if h := after.app.Health(context.Background()); !h.Healthy || h.JetStream == nil || !h.JetStream.Available {
		t.Fatalf("health = %+v", h)
	}

	// A client without credentials still lands in the global account and
	// sees the stream and the consumer position unchanged.
	anon, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("client without credentials: %v", err)
	}
	defer anon.Close()
	js, err := jetstream.New(anon)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "LEDGER_ENTRIES")
	if err != nil {
		t.Fatalf("stream after enabling the option: %v", err)
	}
	if msgs := stream.CachedInfo().State.Msgs; msgs != 3 {
		t.Errorf("stream messages = %d, want 3", msgs)
	}
	cons, err := js.Consumer(ctx, "LEDGER_ENTRIES", "ledger-entries")
	if err != nil {
		t.Fatalf("consumer after enabling the option: %v", err)
	}
	if info := cons.CachedInfo(); info.AckFloor.Stream != 3 || info.NumPending != 0 {
		t.Errorf("consumer ack floor = %d, pending = %d; want 3 and 0", info.AckFloor.Stream, info.NumPending)
	}

	// The application's own (in-process) client keeps working, with no replay.
	after.publish(t, 4)
	if got := after.waitReceived(t, 1); len(got) != 1 || got[0] != 4 {
		t.Errorf("after: received %v, want only the new entry [4]", got)
	}

	// The system account user can run the $SYS requests.
	admin, err := nats.Connect(url, nats.UserInfo("admin", "s3cret"))
	if err != nil {
		t.Fatalf("system account user: %v", err)
	}
	defer admin.Close()
	reply, err := admin.Request("$SYS.REQ.SERVER.PING.JSZ", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("$SYS request: %v", err)
	}
	var jsz struct {
		Data struct {
			Streams int `json:"streams"`
		} `json:"data"`
	}
	if err := json.Unmarshal(reply.Data, &jsz); err != nil || jsz.Data.Streams != 1 {
		t.Errorf("jsz = %s (err %v), want 1 stream", reply.Data, err)
	}
	// A client without credentials cannot reach the system account.
	if _, err := anon.Request("$SYS.REQ.SERVER.PING.JSZ", nil, 500*time.Millisecond); err == nil {
		t.Error("a client without credentials answered a $SYS request")
	}
	if _, err := nats.Connect(url, nats.UserInfo("admin", "wrong")); err == nil {
		t.Error("a wrong system account password was accepted")
	}
	// A client that sends credentials of its own, accepted regardless before,
	// is now checked (documented compatibility change).
	if _, err := nats.Connect(url, nats.UserInfo("app", "leftover")); err == nil {
		t.Error("a client with credentials that are not the system account user's was accepted")
	}
}

// TestSystemAccount_RejectsConfigFileWithAuthorization checks the
// combinations that would lock out the framework's own client: a config file
// with accounts, or with an authorization block, even an empty one.
func TestSystemAccount_RejectsConfigFileWithAuthorization(t *testing.T) {
	for _, tc := range []struct{ name, conf, want string }{
		{"accounts", "accounts { APP: { users: [ { user: app, password: pw } ] } }\n", "WithNATSSystemAccount cannot be combined with accounts"},
		{"empty authorization block", "authorization {}\n", "WithNATSSystemAccount cannot be combined with client authorization"},
		{"authorization timeout only", "authorization { timeout: 5 }\n", "WithNATSSystemAccount cannot be combined with client authorization"},
		{"client user", "authorization { user: app, password: pw }\n", "WithNATSSystemAccount cannot be combined with client authorization"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := filepath.Join(t.TempDir(), "nats.conf")
			if err := os.WriteFile(conf, []byte(tc.conf), 0o600); err != nil {
				t.Fatal(err)
			}
			app, err := mono.NewMonoApplication(
				mono.WithCustomLogger(&noOpsLogger{}),
				mono.WithNATSConfigFile(conf),
				mono.WithNATSPort(freeTCPPort(t)),
				mono.WithNATSSystemAccount("admin", "s3cret"),
			)
			if err == nil {
				_ = app.Stop(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("error leaks the password: %v", err)
			}
		})
	}
}

// TestSystemAccount_ConfigFileWithSystemAccountOnly checks the documented
// alternative to the option: a config file with only a $SYS user and
// system_account keeps the application's own client and clients without
// credentials working.
func TestSystemAccount_ConfigFileWithSystemAccountOnly(t *testing.T) {
	dir := t.TempDir()
	port := freeTCPPort(t)
	conf := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(conf, []byte(`accounts { "$SYS": { users: [ { user: admin, password: s3cret } ] } }
system_account: "$SYS"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	a := startSysAcctApp(t, filepath.Join(dir, "js"), port, mono.WithNATSConfigFile(conf))
	t.Cleanup(func() { _ = a.app.Stop(context.Background()) })
	a.publish(t, 1)
	if got := a.waitReceived(t, 1); len(got) != 1 {
		t.Fatalf("received %v", got)
	}
	url := "nats://127.0.0.1:" + strconv.Itoa(port)
	anon, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("client without credentials: %v", err)
	}
	anon.Close()
	admin, err := nats.Connect(url, nats.UserInfo("admin", "s3cret"))
	if err != nil {
		t.Fatalf("system account user: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Request("$SYS.REQ.SERVER.PING.JSZ", nil, 2*time.Second); err != nil {
		t.Fatalf("$SYS request: %v", err)
	}
}
