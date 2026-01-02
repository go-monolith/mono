# Security Policy

## Commitment to Security

The Mono Framework team takes security seriously. We are committed to addressing security vulnerabilities promptly and transparently. This document outlines how to report vulnerabilities and what to expect from our response.

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| < 1.0   | :x:                |

Only the latest minor version of each major release receives security updates.

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

### Private Disclosure (Preferred)

1. **Do not** open a public issue for security vulnerabilities
2. Email security concerns to the project maintainers via GitHub's private vulnerability reporting feature
3. Or create a private security advisory in this repository

### What to Include

Please include the following in your report:

- A description of the vulnerability
- Steps to reproduce the issue
- Potential impact assessment
- Any suggested fixes (optional)

### Response Timeline

- **Initial Response**: Within 48 hours
- **Triage & Assessment**: Within 7 days
- **Fix Development**: Depends on severity (critical: 7 days, high: 14 days, medium: 30 days)
- **Public Disclosure**: After fix is released and users have time to update

## Security Features

The Mono Framework includes several built-in security features:

### Automatic Log Redaction

Sensitive data is automatically redacted from logs. The following patterns are protected:

- Authentication: `password`, `passwd`, `secret`, `token`, `credential`, `auth`, `authorization`, `bearer`, `jwt`, `session`
- API keys: `apikey`, `api_key`, `access_key`, `secret_key`
- Cryptographic: `key`, `private`, `private_key`, `signing_key`, `encryption_key`, `cert`, `certificate`, `nkey`, `seed`
- OAuth tokens: `oauth`, `refresh_token`, `access_token`, `id_token`
- Database: `connection_string`, `dsn`, `database_url`, `db_password`

Use `logger.RedactSensitiveValue(key, value)` in your handlers for custom redaction.

### Input Validation

The framework validates inputs at all public API boundaries:

- NATS subject naming follows strict kebab-case conventions
- Service names are validated for format compliance
- Configuration options validate parameters immediately
- Port numbers, timeouts, and payload sizes have enforced bounds

### Audit Logging

The audit middleware provides:

- Tamper-resistant hash chaining for audit entries
- Module lifecycle event tracking
- Service registration logging
- Configuration change recording

Enable with:

```go
auditModule, _ := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(true),
)
```

### Error Handling

Error messages are designed to be:

- Actionable for developers
- Free of internal implementation details
- Safe from path traversal information leakage

## Security Best Practices

When using the Mono Framework, follow these practices:

### Secret Management

- **Never** hardcode secrets in source code
- Load secrets from environment variables or secret management systems
- Use `.gitignore` to exclude configuration files with secrets
- Rotate credentials regularly

### Module Isolation

- Design modules with clear boundaries
- Validate all inter-module communication
- Use the principle of least privilege for module dependencies

### Network Security

For production deployments with external NATS access:

- Enable TLS for NATS connections
- Use authentication for NATS clients
- Configure firewall rules to restrict access
- Consider network isolation for the embedded NATS server

### Dependency Security

- Keep dependencies updated
- Review dependency security advisories
- Use `go mod tidy` to remove unused dependencies
- Consider using `govulncheck` for vulnerability scanning

## Known Limitations

### In-Process Communication

Channel services use in-process communication without encryption. For sensitive data:

- Consider using NATS-based services with TLS
- Implement application-level encryption
- Limit sensitive data in channel payloads

### Embedded NATS Server

The embedded NATS server by default:

- Listens on localhost only (127.0.0.1)
- Has no authentication enabled
- Does not use TLS

For production, configure appropriate security settings.

## Security Updates

Security-related changes are documented in release notes with the `[Security]` prefix.

## Contact

For security-related questions that don't involve vulnerability disclosure, open a GitHub Discussion in the Security category.
