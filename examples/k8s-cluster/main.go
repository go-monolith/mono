// Command k8s-cluster is a mono application meant to run as several replicas
// whose embedded NATS servers form one cluster with clustered JetStream, for
// example as a Kubernetes StatefulSet (see deploy/k8s.yaml and README.md).
//
// Every replica runs the same configuration: its server name (POD_NAME, unique
// and stable in a StatefulSet), the routes to all replicas (NATS_ROUTES), the
// route password (NATS_ROUTE_PASSWORD), the directory of the route TLS
// certificate (NATS_ROUTE_TLS_DIR, optional: tls.crt, tls.key, ca.crt), the
// directory of the client listener certificate (NATS_CLIENT_TLS_DIR,
// optional: tls.crt, tls.key, and ca.crt, the CA client certificates must
// chain to), the password of the system account user "admin"
// (NATS_SYSTEM_PASSWORD, optional) and the replicas of the framework's
// streams (JS_REPLICAS). Start waits for the other replicas, so they can start
// together. An HTTP API on HTTP_ADDR exposes:
//
//	GET /hello?name=x   call the hello service and record a visit in the
//	                    replicated stream; with &id=y the visit is published
//	                    with message ID y, so a retried request is stored once
//	GET /cluster        list the instances answering $SRV.PING across the
//	                    cluster, and the replicas of the visits stream
//	GET /healthz        liveness: the process is serving HTTP
//	GET /readyz         readiness: the framework and NATS are healthy; the
//	                    JetStream state is reported but not required
//
// NATS_DEBUG=true adds the embedded NATS server's own log at debug level.
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
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/examples/k8s-cluster/greeter"
	"github.com/go-monolith/mono/examples/k8s-cluster/greeter/contract"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// config is read from the environment; every value but the routes has a
// default that works in the container image.
type config struct {
	instance      string
	clusterName   string
	clusterPort   int
	routes        []string
	routePassword string
	systemPass    string
	routeTLSDir   string
	clientTLSDir  string
	storeDir      string
	replicas      int
	readyTimeout  time.Duration
	httpAddr      string
	natsDebug     bool
}

func loadConfig() (config, error) {
	cfg := config{
		instance:      envOr("POD_NAME", hostname()),
		clusterName:   envOr("NATS_CLUSTER_NAME", "mono-demo"),
		routePassword: os.Getenv("NATS_ROUTE_PASSWORD"),
		systemPass:    os.Getenv("NATS_SYSTEM_PASSWORD"),
		routeTLSDir:   os.Getenv("NATS_ROUTE_TLS_DIR"),
		clientTLSDir:  os.Getenv("NATS_CLIENT_TLS_DIR"),
		storeDir:      envOr("JS_STORE_DIR", "/data/jetstream"),
		httpAddr:      envOr("HTTP_ADDR", ":8080"),
		natsDebug:     os.Getenv("NATS_DEBUG") == "true",
	}
	for _, r := range strings.Split(os.Getenv("NATS_ROUTES"), ",") {
		if r = strings.TrimSpace(r); r != "" {
			cfg.routes = append(cfg.routes, r)
		}
	}
	if len(cfg.routes) == 0 {
		return cfg, errors.New("NATS_ROUTES must list the cluster routes of every replica, comma separated")
	}
	var err error
	if cfg.clusterPort, err = strconv.Atoi(envOr("NATS_CLUSTER_PORT", "6222")); err != nil {
		return cfg, fmt.Errorf("NATS_CLUSTER_PORT: %w", err)
	}
	if cfg.replicas, err = strconv.Atoi(envOr("JS_REPLICAS", "3")); err != nil {
		return cfg, fmt.Errorf("JS_REPLICAS: %w", err)
	}
	if cfg.readyTimeout, err = time.ParseDuration(envOr("JS_READY_TIMEOUT", "10m")); err != nil {
		return cfg, fmt.Errorf("JS_READY_TIMEOUT: %w", err)
	}
	return cfg, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("k8s-cluster: %v", err)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("instance", cfg.instance)

	opts := []mono.MonoFrameworkOption{
		mono.WithLogFormat(mono.LogFormatJSON),
		mono.WithLogLevel(mono.LogLevelInfo),
		// Clustered JetStream: a server name unique within the cluster and
		// stable across restarts (the StatefulSet pod name), and the same
		// route list on every replica, its own route included.
		mono.WithNATSServerName(cfg.instance),
		mono.WithNATSClustering(cfg.clusterName, "0.0.0.0", cfg.clusterPort, cfg.routes),
		mono.WithJetStreamStorageDir(cfg.storeDir),
		// Replicas of the streams the framework creates (visits, cron).
		mono.WithJetStreamDefaultReplicas(cfg.replicas),
		// How long Start waits for the other replicas.
		mono.WithJetStreamReadyTimeout(cfg.readyTimeout),
		// The application's own client connects in-process.
		mono.WithNATSInProcessConn(),
		mono.WithServiceDiscovery(mono.ServiceDiscoveryConfig{
			Version:     "1.0.0",
			Description: "mono k8s-cluster example",
			Metadata:    map[string]string{"instance": cfg.instance},
		}),
		mono.WithShutdownTimeout(20 * time.Second),
		mono.WithShutdownDrainTimeout(10 * time.Second),
	}
	if cfg.routePassword != "" {
		opts = append(opts, mono.WithNATSClusterAuth("route", cfg.routePassword))
	}
	if cfg.routeTLSDir != "" {
		// Mutual TLS on the routes: tls.crt/tls.key/ca.crt as a Kubernetes
		// TLS Secret (or cert-manager) mounts them. A renewed certificate is
		// picked up without a restart.
		opts = append(opts, mono.WithNATSClusterTLS(
			filepath.Join(cfg.routeTLSDir, "tls.crt"),
			filepath.Join(cfg.routeTLSDir, "tls.key"),
			filepath.Join(cfg.routeTLSDir, "ca.crt"),
		))
	}
	if cfg.clientTLSDir != "" {
		// Clients outside the application (deploy/client.yaml) connect to
		// the client listener on the pod IP, with mutual TLS: a certificate
		// from the client CA in ca.crt is required. The listener leaves
		// loopback only together with TLS.
		opts = append(opts,
			mono.WithNATSHost("0.0.0.0"),
			mono.WithNATSClientTLS(types.ClientTLSConfig{
				CertFile:     filepath.Join(cfg.clientTLSDir, "tls.crt"),
				KeyFile:      filepath.Join(cfg.clientTLSDir, "tls.key"),
				ClientCAFile: filepath.Join(cfg.clientTLSDir, "ca.crt"),
			}))
	} else {
		// Without client TLS the client listener stays on loopback: only the
		// application's own client uses it.
		opts = append(opts, mono.WithNATSHost("127.0.0.1"))
	}
	if cfg.systemPass != "" {
		// A system account user for operator tooling (nats server report,
		// nats server raft peer-remove), through kubectl port-forward (with
		// client TLS, also with a client certificate).
		opts = append(opts, mono.WithNATSSystemAccount("admin", cfg.systemPass))
	}
	if cfg.natsDebug {
		// The embedded server's own log (routes, JetStream meta elections)
		// at debug level, through the framework logger.
		opts = append(opts, mono.WithLogLevel(mono.LogLevelDebug), mono.WithNATSLogging(true, false, false))
	}
	app, err := mono.NewMonoApplication(opts...)
	if err != nil {
		return fmt.Errorf("create app: %w", err)
	}
	if err := app.Register(greeter.NewModule(cfg.instance, logger)); err != nil {
		return fmt.Errorf("register greeter: %w", err)
	}

	// Serve /healthz before starting the application: Start waits for the
	// other servers of the cluster, and the liveness probe must not kill the
	// pod meanwhile. Binding first makes a bad address fail at once.
	ln, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.httpAddr, err)
	}
	srv := &http.Server{
		Handler:           newAPI(app, cfg.instance, logger).routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()
	logger.Info("HTTP API listening", "addr", ln.Addr().String())

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	// A failing HTTP server also cancels a Start that is still waiting for
	// the cluster.
	startCtx, cancelStart := context.WithCancelCause(sigCtx)
	defer cancelStart(nil)
	go func() {
		select {
		case err := <-httpErr:
			cancelStart(err)
			httpErr <- err
		case <-startCtx.Done():
		}
	}()

	if err := app.Start(startCtx); err != nil {
		shutdownHTTP(srv, logger)
		if cause := context.Cause(startCtx); cause != nil && !errors.Is(cause, context.Canceled) {
			return fmt.Errorf("HTTP server: %w", cause)
		}
		if sigCtx.Err() != nil {
			// SIGTERM while waiting for the cluster: a normal pod termination.
			logger.Info("interrupted during start")
			return nil
		}
		return fmt.Errorf("start app: %w", err)
	}
	logger.Info("application started")

	var serveErr error
	select {
	case <-sigCtx.Done():
		logger.Info("shutting down")
	case serveErr = <-httpErr:
		logger.Error("HTTP server failed", "error", serveErr)
	}

	shutdownHTTP(srv, logger)
	// No deadline here: WithShutdownTimeout bounds the stop.
	if err := app.Stop(context.Background()); err != nil {
		return fmt.Errorf("stop app: %w", err)
	}
	logger.Info("stopped")
	if serveErr != nil {
		return fmt.Errorf("HTTP server: %w", serveErr)
	}
	return nil
}

func shutdownHTTP(srv *http.Server, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Warn("HTTP shutdown", "error", err)
	}
}

// api serves the example's HTTP endpoints for one instance.
type api struct {
	app      mono.MonoApplication
	instance string
	logger   *slog.Logger
}

func newAPI(app mono.MonoApplication, instance string, logger *slog.Logger) *api {
	return &api{app: app, instance: instance, logger: logger}
}

func (a *api) routes() http.Handler {
	app, instance, writeJSON := a.app, a.instance, a.writeJSON
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive", "instance": instance})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		h := app.Health(r.Context())
		status := http.StatusOK
		if !h.Healthy {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, h)
	})

	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, r *http.Request) {
		services := app.Services(contract.ModuleName)
		if services == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "application not started"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		name := r.URL.Query().Get("name")
		greeting, err := contract.Hello.Call(ctx, services, contract.HelloRequest{Name: name})
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		// A client that retries a failed request passes the same id: the
		// failed publish may still have been stored (a timeout while the
		// cluster has no quorum), and the message ID makes the stream store
		// the visit once.
		var opts []helper.PublishOption
		if id := r.URL.Query().Get("id"); id != "" {
			opts = append(opts, helper.WithMsgID("visit-"+id))
		}
		ack, err := contract.Visits.Publish(ctx, services, contract.Visit{
			Name: name, GreetedBy: greeting.Instance, At: time.Now().UTC(),
		}, opts...)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"http_instance": instance,
			"greeting":      greeting,
			"visit_stored":  map[string]any{"stream": ack.Stream(), "sequence": ack.Sequence(), "duplicate": ack.Duplicate()},
		})
	})

	mux.HandleFunc("GET /cluster", func(w http.ResponseWriter, r *http.Request) {
		bus := app.EventBus(contract.ModuleName)
		if bus == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "application not started"})
			return
		}
		instances, err := a.pingInstances(r.Context(), bus, time.Second)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		stream, err := streamCluster(ctx, bus, "GREETER_VISITS")
		if err != nil {
			stream = map[string]any{"error": err.Error()}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"http_instance": instance,
			"instances":     instances,
			"visits_stream": stream,
		})
	})
	return mux
}

// pingInstances sends one $SRV.PING (NATS services protocol, enabled with
// WithServiceDiscovery) and collects every instance that answers within wait,
// or until ctx ends. Each instance of the application answers, wherever it
// runs in the cluster.
func (a *api) pingInstances(ctx context.Context, bus mono.EventBus, wait time.Duration) ([]map[string]any, error) {
	inbox := nats.NewInbox()
	sub, err := bus.SubscribeSync(inbox)
	if err != nil {
		return nil, fmt.Errorf("subscribe to inbox: %w", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			a.logger.Warn("unsubscribe ping inbox", "error", err)
		}
	}()
	if err := bus.PublishMsg(&mono.Msg{Subject: "$SRV.PING." + contract.ModuleName, Reply: inbox}); err != nil {
		return nil, fmt.Errorf("publish ping: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	instances := []map[string]any{}
	for {
		msg, err := sub.NextMsgWithContext(waitCtx)
		if err != nil {
			if waitCtx.Err() != nil {
				// The wait is over (or the client went away): report what
				// answered so far.
				return instances, nil
			}
			return instances, fmt.Errorf("read ping reply: %w", err)
		}
		var pong map[string]any
		if err := json.Unmarshal(msg.Data, &pong); err != nil {
			return instances, fmt.Errorf("decode ping reply: %w", err)
		}
		instances = append(instances, pong)
	}
}

// streamCluster asks JetStream for the stream's cluster placement: the leader
// and the state of every replica.
func streamCluster(ctx context.Context, bus mono.EventBus, stream string) (map[string]any, error) {
	reply, err := bus.RequestWithContext(ctx, "$JS.API.STREAM.INFO."+stream, nil)
	if err != nil {
		return nil, fmt.Errorf("stream info request: %w", err)
	}
	var info struct {
		Config struct {
			Replicas int `json:"num_replicas"`
		} `json:"config"`
		State struct {
			Msgs uint64 `json:"messages"`
		} `json:"state"`
		Cluster map[string]any `json:"cluster"`
		Error   map[string]any `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &info); err != nil {
		return nil, fmt.Errorf("decode stream info: %w", err)
	}
	if info.Error != nil {
		return nil, fmt.Errorf("stream info: %v", info.Error["description"])
	}
	return map[string]any{
		"name":     stream,
		"replicas": info.Config.Replicas,
		"messages": info.State.Msgs,
		"cluster":  info.Cluster,
	}, nil
}

func (a *api) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		a.logger.Warn("write response", "error", err)
	}
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
		return "mono"
	}
	return h
}
