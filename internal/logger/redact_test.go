package logger

import (
	"strings"
	"testing"
)

// TestRedactSensitiveValue tests sensitive value redaction
func TestRedactSensitiveValue(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    interface{}
		expected interface{}
	}{
		// Sensitive keys
		{"password", "password", "secret123", "[REDACTED]"},
		{"Password", "Password", "secret123", "[REDACTED]"},
		{"user_password", "user_password", "secret123", "[REDACTED]"},
		{"secret", "secret", "mysecret", "[REDACTED]"},
		{"api_secret", "api_secret", "mysecret", "[REDACTED]"},
		{"token", "token", "token123", "[REDACTED]"},
		{"access_token", "access_token", "token123", "[REDACTED]"},
		{"refresh_token", "refresh_token", "token123", "[REDACTED]"},
		{"key", "key", "mykey", "[REDACTED]"},
		{"api_key", "api_key", "mykey", "[REDACTED]"},
		{"apikey", "apikey", "mykey", "[REDACTED]"},
		{"credential", "credential", "mycred", "[REDACTED]"},
		{"credentials", "credentials", "mycred", "[REDACTED]"},
		{"auth", "auth", "myauth", "[REDACTED]"},
		{"authorization", "authorization", "myauth", "[REDACTED]"},
		{"private", "private_key", "mykey", "[REDACTED]"},
		{"cert", "cert", "mycert", "[REDACTED]"},
		{"certificate", "certificate", "mycert", "[REDACTED]"},
		{"oauth", "oauth_token", "token", "[REDACTED]"},
		{"bearer", "bearer_token", "token", "[REDACTED]"},
		{"jwt", "jwt_token", "token", "[REDACTED]"},

		// Non-sensitive keys
		{"username", "username", "john", "john"},
		{"user_id", "user_id", 123, 123},
		{"name", "name", "John Doe", "John Doe"},
		{"email", "email", "john@example.com", "john@example.com"},
		{"log_level", "log_level", "debug", "debug"},
		{"module", "module", "test", "test"},
		{"version", "version", "1.0.0", "1.0.0"},

		// Nil value
		{"password", "password", nil, nil},
		{"username", "username", nil, nil},

		// Case insensitive matching
		{"PASSWORD", "PASSWORD", "secret", "[REDACTED]"},
		{"PassWord", "PassWord", "secret", "[REDACTED]"},
		{"API_KEY", "API_KEY", "key", "[REDACTED]"},

		// Partial matches
		{"db_password", "db_password", "secret", "[REDACTED]"},
		{"mysql_password", "mysql_password", "secret", "[REDACTED]"},
		{"service_secret", "service_secret", "secret", "[REDACTED]"},
		{"github_token", "github_token", "token", "[REDACTED]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactSensitiveValue(tt.key, tt.value)
			if result != tt.expected {
				t.Errorf("RedactSensitiveValue(%q, %v) = %v, expected %v",
					tt.key, tt.value, result, tt.expected)
			}
		})
	}
}

// TestRedactMap tests map redaction
func TestRedactMap(t *testing.T) {
	input := map[string]interface{}{
		"username": "john",
		"password": "secret123",
		"email":    "john@example.com",
		"api_key":  "key123",
		"version":  "1.0.0",
	}

	result := RedactMap(input)

	// Verify redacted values
	if result["password"] != "[REDACTED]" {
		t.Errorf("expected password to be redacted, got %v", result["password"])
	}

	if result["api_key"] != "[REDACTED]" {
		t.Errorf("expected api_key to be redacted, got %v", result["api_key"])
	}

	// Verify non-sensitive values are preserved
	if result["username"] != "john" {
		t.Errorf("expected username to be preserved, got %v", result["username"])
	}

	if result["email"] != "john@example.com" {
		t.Errorf("expected email to be preserved, got %v", result["email"])
	}

	if result["version"] != "1.0.0" {
		t.Errorf("expected version to be preserved, got %v", result["version"])
	}
}

// TestRedactMapEmpty tests empty map
func TestRedactMapEmpty(t *testing.T) {
	input := map[string]interface{}{}
	result := RedactMap(input)

	if len(result) != 0 {
		t.Errorf("expected empty map, got %d items", len(result))
	}
}

// TestRedactMapNil tests nil values in map
func TestRedactMapNil(t *testing.T) {
	input := map[string]interface{}{
		"password": nil,
		"username": nil,
	}

	result := RedactMap(input)

	if result["password"] != nil {
		t.Errorf("expected nil password, got %v", result["password"])
	}

	if result["username"] != nil {
		t.Errorf("expected nil username, got %v", result["username"])
	}
}

// TestRedactMapPreservesOriginal tests original map is not modified
func TestRedactMapPreservesOriginal(t *testing.T) {
	input := map[string]interface{}{
		"password": "secret123",
		"username": "john",
	}

	result := RedactMap(input)

	// Original should not be modified
	if input["password"] != "secret123" {
		t.Error("original map was modified")
	}

	// Result should have redacted value
	if result["password"] != "[REDACTED]" {
		t.Error("result map should have redacted value")
	}
}

// TestFormatRedactedValue tests value formatting with redaction
func TestFormatRedactedValue(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    interface{}
		expected string
	}{
		{"redacted string", "password", "secret", "[REDACTED]"},
		{"redacted number", "api_key", 12345, "[REDACTED]"},
		{"normal string", "username", "john", "john"},
		{"normal number", "count", 42, "42"},
		{"normal bool", "enabled", true, "true"},
		{"normal nil", "value", nil, "<nil>"},
		{"redacted nil", "password", nil, "<nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatRedactedValue(tt.key, tt.value)
			if result != tt.expected {
				t.Errorf("FormatRedactedValue(%q, %v) = %q, expected %q",
					tt.key, tt.value, result, tt.expected)
			}
		})
	}
}

// TestSensitiveKeysComprehensive tests all sensitive key patterns
func TestSensitiveKeysComprehensive(t *testing.T) {
	sensitivePatterns := []string{
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

	for _, pattern := range sensitivePatterns {
		t.Run(pattern, func(t *testing.T) {
			// Test exact match
			result := RedactSensitiveValue(pattern, "value")
			if result != "[REDACTED]" {
				t.Errorf("pattern %q should be redacted", pattern)
			}

			// Test with prefix
			result = RedactSensitiveValue("db_"+pattern, "value")
			if result != "[REDACTED]" {
				t.Errorf("pattern with prefix db_%q should be redacted", pattern)
			}

			// Test with suffix
			result = RedactSensitiveValue(pattern+"_value", "value")
			if result != "[REDACTED]" {
				t.Errorf("pattern with suffix %q_value should be redacted", pattern)
			}

			// Test uppercase
			result = RedactSensitiveValue(toUpper(pattern), "value")
			if result != "[REDACTED]" {
				t.Errorf("uppercase pattern %q should be redacted", toUpper(pattern))
			}
		})
	}
}

// TestRedactDifferentValueTypes tests redaction with different value types
func TestRedactDifferentValueTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{"string", "secret123", "[REDACTED]"},
		{"int", 12345, "[REDACTED]"},
		{"int64", int64(12345), "[REDACTED]"},
		{"float", 123.45, "[REDACTED]"},
		{"bool", true, "[REDACTED]"},
		{"slice", []string{"a", "b"}, "[REDACTED]"},
		{"map", map[string]string{"key": "value"}, "[REDACTED]"},
		{"struct", struct{ Name string }{Name: "test"}, "[REDACTED]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactSensitiveValue("password", tt.value)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// toUpper is a helper to convert string to uppercase
func toUpper(s string) string {
	return strings.ToUpper(s)
}

// TestRedactMapWithComplexValues tests map with complex value types
func TestRedactMapWithComplexValues(t *testing.T) {
	input := map[string]interface{}{
		"password": map[string]string{"nested": "value"},
		"config":   []string{"a", "b", "c"},
		"count":    42,
		"enabled":  true,
	}

	result := RedactMap(input)

	// Password should be redacted regardless of type
	if result["password"] != "[REDACTED]" {
		t.Errorf("expected password to be redacted, got %v", result["password"])
	}

	// Other values should be preserved
	config, ok := result["config"].([]string)
	if !ok || len(config) != 3 {
		t.Errorf("expected config to be preserved, got %v", result["config"])
	}

	if result["count"] != 42 {
		t.Errorf("expected count to be preserved, got %v", result["count"])
	}

	if result["enabled"] != true {
		t.Errorf("expected enabled to be preserved, got %v", result["enabled"])
	}
}
