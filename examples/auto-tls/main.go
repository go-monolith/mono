// Command auto-tls demonstrates automatic ACME (Let's Encrypt) certificates for
// the embedded NATS server.
//
// Unlike the other examples, this one talks to a real certificate authority, so
// it needs a publicly resolvable domain pointing at this host and inbound port
// 80. Run it without MONO_ACME_DOMAIN to see the configuration it would use and
// the prerequisites it needs, without contacting any CA.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/types"
)

// stagingDirectory is Let's Encrypt's staging endpoint. It issues from an
// untrusted root, which makes it useless in production but ideal here: its rate
// limits are far more forgiving while you are getting DNS and firewall rules
// right. Switch to production only once a staging run succeeds.
const stagingDirectory = "https://acme-staging-v02.api.letsencrypt.org/directory"

func main() {
	fmt.Println("=== Mono-Framework AutoTLS Example ===")
	fmt.Println("Demonstrates: automatic ACME certificates for the embedded NATS server")
	fmt.Println()

	domain := os.Getenv("MONO_ACME_DOMAIN")
	email := os.Getenv("MONO_ACME_EMAIL")
	cacheDir := os.Getenv("MONO_ACME_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "mono-autotls-example")
	}

	autoTLS := types.AutoTLSConfig{
		Domains:   []string{domain},
		Email:     email,
		CacheDir:  cacheDir,
		AcceptTOS: true,

		// Staging by default so a misconfigured first run cannot burn the
		// production rate limit of 5 duplicate certificates per week.
		DirectoryURL: stagingDirectory,

		// ":80" is the default and is what a certificate authority always
		// connects to. Override it only when a reverse proxy forwards
		// /.well-known/acme-challenge/ to a different local port.
		HTTPChallengeAddr: ":80",
	}

	if domain == "" {
		explain(autoTLS)
		return
	}

	app, err := mono.NewMonoApplication(
		// Listen on all interfaces: the certificate authority has to reach
		// this host from the internet to validate the challenge.
		mono.WithNATSHost("0.0.0.0"),
		mono.WithNATSPort(4222),
		mono.WithNATSAutoTLS(autoTLS),
		mono.WithLogLevel(mono.LogLevelInfo),
		mono.WithLogFormat(mono.LogFormatText),
		mono.WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create app: %v", err)
	}
	fmt.Println("✓ App created (AutoTLS enabled)")

	if err := app.Register(NewEchoModule()); err != nil {
		log.Fatalf("Failed to register module: %v", err)
	}
	fmt.Println("✓ Module registered")

	// Start obtains the certificate before returning. A misconfigured domain or
	// an unreachable port 80 surfaces here rather than as a handshake failure
	// on the first client connection.
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalf("Failed to start app (certificate issuance failed): %v", err)
	}
	fmt.Printf("✓ App started — certificate obtained for %s\n", domain)
	fmt.Println()
	fmt.Println("Clients must now connect over TLS, by hostname:")
	fmt.Printf("  nats://%s:4222  ✗ rejected — AutoTLS makes TLS mandatory\n", domain)
	fmt.Printf("  tls://%s:4222   ✓\n", domain)
	fmt.Println()
	fmt.Println("Renewal happens in the background; no restart or reload is needed.")
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Stop(shutdownCtx); err != nil {
		log.Fatalf("Failed to stop app: %v", err)
	}
	fmt.Println("✓ App stopped cleanly")
}

// explain prints what the example would do, so that running it with no
// configuration is informative instead of being a failure or a hang.
func explain(cfg types.AutoTLSConfig) {
	fmt.Println("MONO_ACME_DOMAIN is not set, so no certificate authority will be contacted.")
	fmt.Println()
	fmt.Println("This example would configure AutoTLS as:")
	fmt.Println()
	fmt.Println("    mono.WithNATSAutoTLS(types.AutoTLSConfig{")
	fmt.Println(`        Domains:      []string{"nats.example.com"},`)
	fmt.Println(`        Email:        "ops@example.com",`)
	fmt.Printf("        CacheDir:     %q,\n", cfg.CacheDir)
	fmt.Println("        AcceptTOS:    true,")
	fmt.Printf("        DirectoryURL: %q,\n", cfg.DirectoryURL)
	fmt.Println("    })")
	fmt.Println()
	fmt.Println("To run it for real you need:")
	fmt.Println("  1. A public DNS A record for the domain pointing at this host.")
	fmt.Println("  2. Inbound TCP port 80 reachable from the internet, for the http-01 challenge.")
	fmt.Println("  3. Permission to bind port 80 (root, or CAP_NET_BIND_SERVICE).")
	fmt.Println("  4. A cache directory that survives restarts — losing it forces reissuance")
	fmt.Println("     on every boot and will exhaust the certificate authority's rate limits.")
	fmt.Println()
	fmt.Println("Then:")
	fmt.Println("  MONO_ACME_DOMAIN=nats.example.com \\")
	fmt.Println("  MONO_ACME_EMAIL=ops@example.com \\")
	fmt.Println("  MONO_ACME_CACHE_DIR=/var/lib/mono/acme \\")
	fmt.Println("    go run ./examples/auto-tls")
	fmt.Println()
	fmt.Println("It uses the Let's Encrypt staging environment by default. Staging certificates")
	fmt.Println("chain to an untrusted root, so clients must be told to trust it; drop the")
	fmt.Println("DirectoryURL field to use production once a staging run succeeds.")
	fmt.Println()
	fmt.Println("Example completed!")
}
