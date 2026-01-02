package kvjetstream

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-monolith/mono/v1/pkg/storage"
)

func TestKeyNotFoundError(t *testing.T) {
	key := "test-key"
	err := keyNotFoundError(key)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Check error wraps ErrKeyNotFound
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected error to wrap ErrKeyNotFound, got: %v", err)
	}

	// Check error message contains key
	if !strings.Contains(err.Error(), key) {
		t.Errorf("expected error message to contain key %q, got: %v", key, err)
	}
}

func TestKeyExistsError(t *testing.T) {
	key := "existing-key"
	err := keyExistsError(key)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Check error wraps ErrKeyExists
	if !errors.Is(err, storage.ErrKeyExists) {
		t.Errorf("expected error to wrap ErrKeyExists, got: %v", err)
	}

	// Check error message contains key
	if !strings.Contains(err.Error(), key) {
		t.Errorf("expected error message to contain key %q, got: %v", key, err)
	}
}

func TestSentinelErrors_Aliases(t *testing.T) {
	// Verify sentinel errors are aliases to storage errors
	tests := []struct {
		name   string
		kvErr  error
		stgErr error
	}{
		{"ErrKeyNotFound", ErrKeyNotFound, storage.ErrKeyNotFound},
		{"ErrKeyExists", ErrKeyExists, storage.ErrKeyExists},
		{"ErrRevisionMismatch", ErrRevisionMismatch, storage.ErrRevisionMismatch},
		{"ErrBucketNotFound", ErrBucketNotFound, storage.ErrBucketNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.kvErr != tt.stgErr {
				t.Errorf("expected %s to be alias for storage.%s", tt.name, tt.name)
			}
		})
	}
}
