// Command client is a NATS client of the k8s-cluster example that runs
// outside the application: a separate Deployment in the same cluster that
// connects to the servers' client listener with mutual TLS
// (mono.WithNATSClientTLS on the servers) and calls the greeter module
// through its contract's subjects.
//
// Configuration, from the environment:
//
//	NATS_URL             server URL, for example
//	                     tls://mono-cluster-client.mono-demo.svc.cluster.local:4222
//	NATS_CLIENT_TLS_DIR  directory with tls.crt and tls.key (this client's
//	                     certificate, TLS client use) and ca.crt (the CA of the
//	                     servers' listener certificate)
//	POD_NAME             instance name in responses (default: host name)
//	HTTP_ADDR            HTTP API address (default :8080)
//
// The client certificate is reloaded when its files change
// (helper.NATSClientTLS), so a renewed certificate is presented on the next
// reconnect without a restart. An HTTP API exposes:
//
//	GET /hello?name=x   call the greeter's hello service and record a visit
//	                    in its JetStream stream, as the servers' /hello does;
//	                    with &id=y the visit is published with message ID y
//	GET /healthz        liveness: the process is serving HTTP
//	GET /readyz         readiness: connected to a server
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/examples/k8s-cluster/greeter/contract"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("k8s-cluster client: %v", err)
	}
}

func run() error {
	instance := envOr("POD_NAME", hostname())
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("instance", instance)
	url := os.Getenv("NATS_URL")
	tlsDir := os.Getenv("NATS_CLIENT_TLS_DIR")
	if url == "" || tlsDir == "" {
		return errors.New("NATS_URL and NATS_CLIENT_TLS_DIR are required")
	}

	tlsOpt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{
		CAFile:   filepath.Join(tlsDir, "ca.crt"),
		CertFile: filepath.Join(tlsDir, "tls.crt"),
		KeyFile:  filepath.Join(tlsDir, "tls.key"),
		Logger:   slogLogger{logger},
	})
	if err != nil {
		return err
	}
	nc, err := nats.Connect(url, tlsOpt,
		nats.Name("k8s-cluster-client-"+instance),
		// Keep trying while the servers start or roll.
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { logger.Warn("NATS disconnected", "error", err) }),
		// With RetryOnFailedConnect, a failing first connection (a wrong CA,
		// an expired certificate) is reported only here.
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) { logger.Warn("NATS connect failed", "error", err) }),
		nats.ReconnectHandler(func(c *nats.Conn) { logger.Info("NATS reconnected", "server", c.ConnectedServerName()) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { logger.Warn("NATS error", "error", err) }),
	)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	ln, err := net.Listen("tcp", envOr("HTTP_ADDR", ":8080"))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{Handler: routes(nc, js, instance, logger), ReadHeaderTimeout: 5 * time.Second}
	httpErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()
	logger.Info("client started", "nats_url", url, "http_addr", ln.Addr().String())

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-sigCtx.Done():
	case err = <-httpErr:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if shutdownErr := srv.Shutdown(ctx); shutdownErr != nil {
		logger.Warn("HTTP shutdown", "error", shutdownErr)
	}
	// srv.Shutdown has waited for the requests in flight, and the client has
	// no subscriptions to drain.
	nc.Close()
	logger.Info("stopped")
	if err != nil {
		return fmt.Errorf("HTTP server: %w", err)
	}
	return nil
}

func routes(nc *nats.Conn, js jetstream.JetStream, instance string, logger *slog.Logger) http.Handler {
	writeJSON := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(v); err != nil {
			logger.Warn("write response", "error", err)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive", "instance": instance})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		if !nc.IsConnected() {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]any{"connected": nc.IsConnected(), "server": nc.ConnectedServerName()})
	})
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		name := r.URL.Query().Get("name")
		greeting, err := hello(ctx, nc, name)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		data, err := json.Marshal(contract.Visit{Name: name, GreetedBy: greeting.Instance, At: time.Now().UTC()})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		var opts []jetstream.PublishOpt
		if id := r.URL.Query().Get("id"); id != "" {
			opts = append(opts, jetstream.WithMsgID("visit-"+id))
		}
		ack, err := js.Publish(ctx, contract.Visits.ToBase().Subject(), data, opts...)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"http_instance": instance,
			"greeting":      greeting,
			"visit_stored":  map[string]any{"stream": ack.Stream, "sequence": ack.Sequence, "duplicate": ack.Duplicate},
		})
	})
	return mux
}

// hello calls the greeter's request-reply service on its contract subject,
// with the request deadline in the Mono-Deadline and Mono-Timeout headers.
func hello(ctx context.Context, nc *nats.Conn, name string) (contract.HelloResponse, error) {
	var resp contract.HelloResponse
	data, err := json.Marshal(contract.HelloRequest{Name: name})
	if err != nil {
		return resp, err
	}
	msg := nats.NewMsg(contract.Hello.ToBase().Subject())
	msg.Data = data
	if deadline, ok := ctx.Deadline(); ok {
		// The deadline headers the framework's own clients send: the server
		// drops a request its caller has given up on.
		msg.Header.Set(types.HeaderDeadline, strconv.FormatInt(deadline.UnixMilli(), 10))
		msg.Header.Set(types.HeaderTimeout, strconv.FormatInt(time.Until(deadline).Milliseconds(), 10))
	}
	reply, err := nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return resp, fmt.Errorf("hello: %w", err)
	}
	if reply.Header.Get(types.HeaderError) == "true" {
		kind, text := reply.Header.Get(types.HeaderErrorType), reply.Header.Get(types.HeaderErrorMessage)
		if kind == "" && text == "" {
			text = "unknown error"
		}
		return resp, fmt.Errorf("hello: %s %s", kind, text)
	}
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return resp, fmt.Errorf("hello: decode reply: %w", err)
	}
	return resp, nil
}

// slogLogger adapts slog to the framework's logger interface, for the
// reload lines of the client certificate.
type slogLogger struct{ *slog.Logger }

func (l slogLogger) With(args ...any) types.Logger    { return slogLogger{l.Logger.With(args...)} }
func (l slogLogger) WithModule(m string) types.Logger { return slogLogger{l.Logger.With("module", m)} }
func (l slogLogger) WithError(err error) types.Logger {
	return slogLogger{l.Logger.With("error", err)}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "client"
	}
	return h
}
