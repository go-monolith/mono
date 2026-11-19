package fsjetstream

import "errors"

// ErrStorageMismatch is returned by Start when a configured bucket's stream
// already exists with a different storage type (file or memory) than its
// BucketConfig.Storage, which NATS cannot change in place. Remove the stream,
// or set BucketConfig.RecreateOnStorageMismatch for a bucket whose contents
// can be lost.
var ErrStorageMismatch = errors.New("object store bucket storage type mismatch")
