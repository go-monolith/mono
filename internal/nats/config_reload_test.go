package nats

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	"github.com/go-monolith/mono/pkg/types"
)

// SystemServerID returns the id of the server the system account connection
// sys is connected to.
func SystemServerID(t *testing.T, sys *natsgo.Conn) string {
	t.Helper()
	ping, err := sys.Request("$SYS.REQ.SERVER.PING", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("server ping: %v", err)
	}
	var p struct {
		Server struct {
			ID string `json:"id"`
		} `json:"server"`
	}
	if err := json.Unmarshal(ping.Data, &p); err != nil || p.Server.ID == "" {
		t.Fatalf("server ping reply %s: %v", ping.Data, err)
	}
	return p.Server.ID
}

// A reload requested over the system account is refused, and what the
// options added to the config file stays in effect: before, the server
// parsed the file again on its own, and JetStream, client TLS and the system
// account user were gone (issue #88).
func TestConfigFileReloadIsRefused(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	serverCAFile := serverCA.write(t, t.TempDir())
	certFile, keyFile := serverCA.issue(t, t.TempDir(), "server", 10)
	port, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	m := StartConfigFileManager(t, newMockLogger(), fmt.Sprintf("host: 127.0.0.1\nport: %d\n", port),
		WithInProcessConn(),
		WithJetStream(t.TempDir()),
		WithSystemAccount("admin", "s3cret"),
		WithClientTLS(&types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile}),
	)
	conn, err := m.Connection()
	if err != nil {
		t.Fatal(err)
	}
	plaintextClient := func() error {
		nc, err := natsgo.Connect(fmt.Sprintf("nats://127.0.0.1:%d", port), natsgo.NoReconnect(), natsgo.Timeout(2*time.Second))
		if err != nil {
			return err
		}
		defer nc.Close()
		if err := nc.Publish("x", []byte("y")); err != nil {
			return err
		}
		return nc.FlushTimeout(2 * time.Second)
	}
	if err := plaintextClient(); err == nil {
		t.Fatal("a plaintext client was accepted before the reload")
	}

	sys, err := natsgo.Connect(fmt.Sprintf("tls://127.0.0.1:%d", port),
		natsgo.RootCAs(serverCAFile), natsgo.UserInfo("admin", "s3cret"), natsgo.NoReconnect())
	if err != nil {
		t.Fatalf("system account user: %v", err)
	}
	defer sys.Close()
	reply, err := sys.Request("$SYS.REQ.SERVER."+SystemServerID(t, sys)+".RELOAD", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("reload request: %v", err)
	}
	if !strings.Contains(string(reply.Data), "can only reload config when a file is provided") {
		t.Errorf("reload reply = %s, want nats-server's refusal", reply.Data)
	}

	if _, err := conn.Request("$JS.API.INFO", nil, 2*time.Second); err != nil {
		t.Errorf("JetStream after the reload request: %v", err)
	}
	if h := m.JetStreamHealth(); h == nil || !h.Available {
		t.Errorf("JetStream health after the reload request = %+v, want available", h)
	}
	if err := plaintextClient(); err == nil {
		t.Error("a plaintext client was accepted after the reload request")
	}
	if !sys.IsConnected() {
		t.Error("the system account connection was closed by the reload request")
	}
	if _, err := sys.Request("$SYS.REQ.SERVER.PING", nil, 2*time.Second); err != nil {
		t.Errorf("system account request after the reload request: %v", err)
	}
}

// DetachConfigFile clears only the config file path.
func TestDetachConfigFile(t *testing.T) {
	path := t.TempDir() + "/nats.conf"
	if err := writeTestFile(path, "port: 4333\nmax_payload: 2048\n"); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if opts.ConfigFile != path {
		t.Fatalf("ConfigFile = %q before, want %q", opts.ConfigFile, path)
	}
	DetachConfigFile(opts)
	if opts.ConfigFile != "" {
		t.Errorf("ConfigFile = %q, want empty", opts.ConfigFile)
	}
	if opts.Port != 4333 || opts.MaxPayload != 2048 {
		t.Errorf("port %d, max payload %d: the parsed settings were changed", opts.Port, opts.MaxPayload)
	}
}
