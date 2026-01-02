package kvjetstream

import (
	"fmt"

	"github.com/go-monolith/mono/pkg/storage"
)

// Sentinel errors for KV operations.
// These are aliases to the storage package errors for backwards compatibility.
// Use errors.Is() to check for these errors in consumer code.
//
// Example:
//
//	entry, err := bucket.Get(ctx, "key")
//	if errors.Is(err, kvjetstream.ErrKeyNotFound) {
//	    // Handle missing key
//	}
var (
	// ErrKeyNotFound is returned when a key does not exist in the bucket.
	ErrKeyNotFound = storage.ErrKeyNotFound

	// ErrKeyExists is returned by Create() when the key already exists.
	ErrKeyExists = storage.ErrKeyExists

	// ErrRevisionMismatch is returned by Update() or conditional Delete()
	// when the expected revision does not match the current revision.
	// This indicates a concurrent modification occurred.
	ErrRevisionMismatch = storage.ErrRevisionMismatch

	// ErrBucketNotFound is returned when attempting to access a bucket
	// that does not exist in the module configuration.
	ErrBucketNotFound = storage.ErrBucketNotFound
)

// keyNotFoundError creates a key not found error with key context.
func keyNotFoundError(key string) error {
	return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
}

// keyExistsError creates a key exists error with key context.
func keyExistsError(key string) error {
	return fmt.Errorf("%w: %s", ErrKeyExists, key)
}
