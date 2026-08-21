# autotls.go — Automatic TLS (ACME) for the client listener

Specification for `internal/nats/autotls.go`. Read this before changing that file.

Companion design section: [`docs/spec/foundation.md` → AutoTLS Design](../../docs/spec/foundation.md).
Public configuration type: `types.AutoTLSConfig` (`pkg/types/config.go`, validated in `pkg/types/autotls.go`).
Public option: `mono.WithNATSAutoTLS` (`options.go`).

## Purpose

Give the embedded NATS server's client listener a certificate that is obtained
and renewed automatically over ACME, so operators do not have to provision
certificate files out of band and restart to pick up renewals.

The file owns exactly two things: the `autocert.Manager` and the HTTP listener
that answers ACME http-01 challenges. Everything else — option validation,
translation, and lifecycle sequencing — lives elsewhere and is listed under
[Collaborators](#collaborators).

## Surface

All unexported; nothing here is part of the public API.

| Symbol | Responsibility |
|---|---|
| `autoTLS` | Owns the manager, the challenge listener and its `http.Server` |
| `newAutoTLS(cfg, logger)` | Builds the manager and creates the cache directory |
| `newACMEClient(cfg)` | Builds the low-level `acme.Client` with a bounded HTTP client |
| `(*autoTLS).tlsConfig()` | Produces the `*tls.Config` installed on `server.Options.TLSConfig` |
| `(*autoTLS).start()` | Binds and serves the http-01 challenge listener |
| `(*autoTLS).prewarm(ctx)` | Bounded, synchronous startup certificate fetch |
| `(*autoTLS).issueAll()` | Fetches a certificate for each configured domain |
| `(*autoTLS).reapAbandoned(done, reason)` | Reports an abandoned in-flight issuance |
| `(*autoTLS).stop(ctx)` | Shuts the challenge server down |
| `stripHostPort(next)` | Normalizes `r.Host` before autocert's host policy runs |

Constants: `challengeReadHeaderTimeout` (10s), `autoTLSHandshakeTimeout` (10s,
assigned to `server.Options.TLSTimeout`), `acmeRequestTimeout` (30s, per ACME
request — not per order).

## Invariants

These are the properties the implementation exists to guarantee. Breaking one
is a defect even if every test still passes.

1. **`HTTPHandler` is called before any issuance can start.** It sets the
   manager's internal `tryHTTP01` flag; until then autocert offers the CA only
   tls-alpn-01, which this framework cannot answer. `start()` must therefore run
   before `prewarm()` and before the NATS listener accepts a handshake.
2. **`tls.Config.Certificates` stays empty.** nats-server's OCSP stapling
   wrapper iterates that slice; an empty slice makes OCSP a harmless no-op
   instead of an error.
3. **`NextProtos` never contains `acme.ALPNProto`.** tls-alpn-01 cannot be
   answered here, and advertising it would claim support that does not exist.
4. **Every ACME request is bounded.** `acme.Client.HTTPClient` must always be
   set. When it is nil, `acme` falls back to `http.DefaultClient`, which has no
   timeout at all, and a half-open certificate authority can hold a connection
   open long after startup has given up on it.
5. **The challenge listener is released on every failure path.** A leaked
   listener holds a privileged port and makes the next start fail for an
   unrelated reason.
6. **`stripHostPort` wraps the autocert handler.** autocert passes `r.Host`
   verbatim to the host policy and `HostWhitelist` matches bare hostnames, so an
   unstripped port turns every challenge on a non-80 listener into a 403.

## Behavior

### Construction

`newAutoTLS` validates the config, creates `CacheDir` with mode `0700`, and
builds an `autocert.Manager` with `AcceptTOS` as the prompt (safe because
validation requires `AcceptTOS`), a `DirCache`, and a `HostWhitelist` over
`Domains`.

`newACMEClient` is always called, even when neither `DirectoryURL` nor
`DirectoryCAPool` is configured — see invariant 4. The directory URL defaults
explicitly to `autocert.DefaultACMEDirectory`. The transport is a clone of
`http.DefaultTransport`, so connection pooling and proxy settings are preserved;
roots are pinned only when `DirectoryCAPool` is set, which is needed solely for
a private ACME CA whose *directory endpoint* is not publicly trusted.

### Challenge server

`start()` binds `cfg.ChallengeAddr()` synchronously so that a bind failure is a
startup error rather than a log line from a goroutine nobody is watching. The
error message names the address and mentions that a privileged port needs root
or `CAP_NET_BIND_SERVICE`, because that is the common cause.

The handler is `stripHostPort(mgr.HTTPHandler(nil))` with a
`ReadHeaderTimeout`, which is required on a public port. Resulting status codes:

| Request | Result |
|---|---|
| `/.well-known/acme-challenge/<unknown>`, `Host` in `Domains` | 404 |
| `/.well-known/acme-challenge/<token>`, `Host` in `Domains` | 200, token payload |
| any challenge path, `Host` not in `Domains` | 403 |
| any other path | 302 to `https://` |

A port in the `Host` header does not change any of these outcomes.

### Startup fetch

`prewarm` returns immediately when `IssueTimeout()` is negative. Otherwise it
runs `issueAll` in a goroutine and selects on the result, a timer, and the
caller's context.

The timeout must be enforced from the outside: `autocert.GetCertificate` builds
its own context and ignores the caller's, so threading a `context.WithTimeout`
into it would look correct and do nothing.

`issueAll` iterates every domain, because autocert issues one certificate per
SNI name rather than one multi-SAN certificate. The synthetic
`tls.ClientHelloInfo` pins signature schemes and versions so the cache key
matches what a real handshake from a Go client produces; without that, the
startup fetch would warm an entry later handshakes never read.

On the timeout and cancellation branches, `reapAbandoned` logs a warning
immediately and then logs the eventual outcome. The issuance cannot be
cancelled, only abandoned, and silently dropping it would hide a repeating leak
from a supervised restart loop whose attempts all race on the same cache.

### Shutdown

`stop` is safe on a nil receiver and safe to call twice. It shuts the server
down under the caller's deadline, force-closes on failure, and falls back to
closing a bound-but-unserved listener. `autocert.Manager` has no exported stop;
its renewal timers are released when the process exits.

## Error semantics

| Condition | Behavior |
|---|---|
| Invalid config | `newAutoTLS` error; wrapped as a startup failure |
| Cache directory not creatable | `newAutoTLS` error naming the directory |
| Challenge port unavailable | `start` error naming the address |
| Issuance fails within the budget | `prewarm` error naming the domain; `Start` rolls back |
| Issuance exceeds the budget | `prewarm` timeout error naming the challenge address |
| Caller cancels during issuance | `prewarm` cancellation error wrapping `ctx.Err()` |
| Startup fetch disabled and issuance later fails | Handshake fails; the application stays up |
| Renewal fails after startup | Existing certificate is served until expiry |

## Collaborators

| File | Responsibility |
|---|---|
| `pkg/types/config.go` | `AutoTLSConfig` and its godoc |
| `pkg/types/autotls.go` | `Validate`, `Clone`, `ChallengeAddr`, `IssueTimeout`, defaults |
| `options.go` | `WithNATSAutoTLS`, wrapping validation errors as configuration errors |
| `internal/app/factory.go` | Translation, and forcing `UseInProcessConn` on |
| `internal/nats/options.go` | `WithAutoTLS`, storing a defensive copy |
| `internal/nats/manager.go` | Sequencing in `Start`/`Stop`, cross-field guards, `ServerInfo` scheme |

Cross-field rules enforced outside this file: AutoTLS with `DontListen` is
rejected, AutoTLS without `UseInProcessConn` is rejected, and AutoTLS combined
with a config file that defines a `tls{}` block is rejected.

## Testing

Unit tests in `autotls_test.go` are hermetic — no network, no Docker. Cases that
must not reach the internet point `DirectoryURL` at a closed local port or an
`httptest.Server` and set `StartupIssueTimeout` negative.

Each invariant above has a guard: `TestAutoTLSTLSConfig` (2, 3),
`TestAutoTLSStartStop` (1, 6, including the `Host`-with-port regression cases),
`TestNewACMEClientAlwaysBounded` (4, across all four directory/CA-pool
combinations), and `TestNATSManager_AutoTLS_ChallengePortConflict` in
`manager_test.go` (5).

End-to-end issuance is covered by
`test/integration/nats_autotls_integration_test.go`, which runs a real ACME
order against Pebble under `make test-integration`. It skips only when the host
is not Linux or has no Docker daemon; on CI it runs and must pass.

Pebble is reached through a small TLS reverse proxy owned by the test. Pebble's
finalize response carries no `Location` header — RFC 8555 does not require one
there — but `x/crypto/acme` reads an order's URL exclusively from that header,
so without the proxy `CreateOrderCert` fails with `Post "": unsupported protocol
scheme ""`. The proxy supplies that one header and nothing else, guarded on it
being absent so it goes inert if `x/crypto` ever stops depending on it. The full
rationale is on `startPebbleProxy` in that file.

## Non-goals

Cluster, gateway, leafnode, websocket and MQTT TLS; HTTPS monitoring; manual
certificate files; tls-alpn-01; dns-01; a certificate cache shared across
clustered nodes.
