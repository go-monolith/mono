// Package logger provides logging functionality with support for redacting sensitive information
// from log output, such as passwords, tokens, API keys, and other credentials.
package logger

import (
	"fmt"
	"strings"
)

// sensitiveKeys contains common key names that should have their values redacted.
// This list covers authentication, authorization, and encryption-related data.
var sensitiveKeys = []string{
	// Authentication
	"password", "passwd", "secret", "token", "credential",
	"auth", "authorization", "bearer", "jwt", "session",
	// API keys and access
	"apikey", "api_key", "access_key", "secret_key",
	// Cryptographic
	"key", "private", "private_key", "signing_key", "encryption_key",
	"cert", "certificate", "nkey", "seed",
	// OAuth and tokens
	"oauth", "refresh_token", "access_token", "id_token",
	// Database and connection
	"connection_string", "dsn", "database_url", "db_password",
}

// RedactSensitiveValue redacts a value if the key appears to contain sensitive information.
func RedactSensitiveValue(key string, value interface{}) interface{} {
	if value == nil {
		return nil
	}

	keyLower := strings.ToLower(key)
	for _, sensitive := range sensitiveKeys {
		if strings.Contains(keyLower, sensitive) {
			return "[REDACTED]"
		}
	}
	return value
}

// RedactMap redacts all sensitive values in a map.
func RedactMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = RedactSensitiveValue(k, v)
	}
	return result
}

// FormatRedactedValue formats a value for display, redacting it if sensitive.
func FormatRedactedValue(key string, value interface{}) string {
	redacted := RedactSensitiveValue(key, value)
	if s, ok := redacted.(string); ok && s == "[REDACTED]" {
		return s
	}
	return fmt.Sprintf("%v", value)
}
