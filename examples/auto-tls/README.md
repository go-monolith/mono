# AutoTLS Example

**Automatic Let's Encrypt certificates for the embedded NATS server.** The
framework obtains a certificate over ACME on startup and renews it in the
background — no certificate files to provision, and no restart or reload at
renewal time.

**Unlike every other example, this one talks to a real certificate authority.**
It needs a publicly resolvable domain pointing at the host and inbound port 80,
so it cannot complete inside a dev container or CI. Running it with no
configuration is safe: it prints what it would do and exits without contacting
any CA.

## Running the Example

Prerequisites for a real run:

1. A public DNS A record for the domain, pointing at the host.
2. Inbound TCP port 80 reachable from the internet, for the http-01 challenge.
3. Permission to bind port 80 — root, or `CAP_NET_BIND_SERVICE`.
4. A cache directory that survives restarts.

```bash
# Dry run: prints the configuration and prerequisites, contacts nothing.
cd examples/auto-tls && go run .

# For real, against Let's Encrypt staging. From the repository root:
MONO_ACME_DOMAIN=nats.example.com \
MONO_ACME_EMAIL=ops@example.com \
MONO_ACME_CACHE_DIR=/var/lib/mono/acme \
  go run ./examples/auto-tls
```

`MONO_ACME_CACHE_DIR` defaults to `$TMPDIR/mono-autotls-example` in this
example. Set it to a persistent path for anything but a one-off try.
`MONO_ACME_EMAIL` is optional (`AutoTLSConfig.Email`).

There is no `make run-example-N` target for this example, because it cannot
complete without public DNS.

## Code Structure

```
examples/auto-tls/
├── main.go     # Configuration from the environment, AutoTLS options, dry run when MONO_ACME_DOMAIN is unset
├── module.go   # EchoModule: the "say" request-reply service (services.echo.say)
└── README.md   # This file
```

## Key Patterns

```go
app, err := mono.NewMonoApplication(
    mono.WithNATSHost("0.0.0.0"),
    mono.WithNATSPort(4222),
    mono.WithNATSAutoTLS(types.AutoTLSConfig{
        Domains:   []string{"nats.example.com"},
        Email:     "ops@example.com",
        CacheDir:  "/var/lib/mono/acme",
        AcceptTOS: true,
    }),
)
```

- **Client-to-server connections only**: the certificate is installed on the
  NATS client listener. Route (cluster), gateway, leafnode, websocket and MQTT
  listeners each have their own separate TLS configuration and are untouched, so
  traffic between cluster nodes stays plaintext unless you configure it
  yourself. Routes are peer-to-peer and conventionally use mutual TLS, which
  needs a client certificate autocert does not issue — use an internal CA and a
  `cluster { tls { ... } }` block in a [NATS config file](../nats-config/README.md)
  instead.
- **http-01 only**: the framework serves the challenge from its own listener on
  port 80, for the whole life of the process — every renewal re-runs the
  challenge. tls-alpn-01 would need the listener on port 443, and autocert does
  not implement dns-01, so wildcard domains are rejected.
- **`CacheDir` is required and must persist**: it holds the ACME account key and
  the issued certificates. Losing it on every restart forces reissuance and will
  exhaust Let's Encrypt's limit of 5 duplicate certificates per week. There is
  deliberately no default, so persistence is an explicit decision.
- **Clients must dial by hostname over `tls://`**: the certificate is selected
  from the TLS SNI extension, which `nats.go` takes from the URL host. Dialling
  by IP presents no SNI and is refused, and `Domains` doubles as the allowlist,
  so any other name is refused too.
- **Enabling AutoTLS makes TLS mandatory**: plaintext clients are rejected once
  a certificate is configured. Turning it on is a breaking change for an
  existing deployment.
- **Startup is fail-fast**: the certificate is obtained inside
  `mono.NewMonoApplication` (bounded by `StartupIssueTimeout`, default 60 s),
  so a misconfigured domain or an unreachable port 80 makes
  `NewMonoApplication` return an error instead of surfacing as a handshake
  error hours later.
- **Staging first**: this example defaults to the Let's Encrypt staging
  endpoint, whose rate limits are far more forgiving while DNS and firewall
  rules are still being sorted out. Staging issues from an untrusted root, so
  clients must be told to trust it. Drop `DirectoryURL` to use production once a
  staging run succeeds.

Once started, the echo service is reachable over TLS:

```go
nc, err := nats.Connect("tls://nats.example.com:4222")
reply, err := nc.Request("services.echo.say", []byte("hello"), 5*time.Second)
// reply.Data == "echo: hello"
```

## Expected Output

Without `MONO_ACME_DOMAIN` (the dry run; `CacheDir` shows the default under `$TMPDIR`):

```
=== Mono-Framework AutoTLS Example ===
Demonstrates: automatic ACME certificates for the embedded NATS server

MONO_ACME_DOMAIN is not set, so no certificate authority will be contacted.

This example would configure AutoTLS as:

    mono.WithNATSAutoTLS(types.AutoTLSConfig{
        Domains:      []string{"nats.example.com"},
        Email:        "ops@example.com",
        CacheDir:     "/tmp/mono-autotls-example",
        AcceptTOS:    true,
        DirectoryURL: "https://acme-staging-v02.api.letsencrypt.org/directory",
    })

To run it for real you need:
  ...

Example completed!
```

## Next Steps

- [Foundation Spec — AutoTLS Design](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md)
- [Implementation Spec](https://github.com/go-monolith/mono/blob/main/internal/nats/autotls.spec.md)
- [NATS Config File Example](../nats-config/README.md)
- [SECURITY.md](https://github.com/go-monolith/mono/blob/main/SECURITY.md)
