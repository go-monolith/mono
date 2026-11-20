package kvjetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/go-monolith/mono/internal/jsrecreate"
	"github.com/nats-io/nats.go/jetstream"
)

// kvStreamPrefix is the name prefix JetStream gives the stream of a KV bucket
// (KV_<bucket>). It is part of the KV wire protocol; nats.go keeps its own
// constant unexported.
const kvStreamPrefix = "KV_"

// DefaultRecreateBackupTTL is how long the keys of a backup bucket made by
// RecreateOnStorageMismatch are kept when BucketConfig.RecreateBackupTTL is 0.
const DefaultRecreateBackupTTL = jsrecreate.DefaultBackupTTL

// MinRecreateBackupTTL is the smallest positive BucketConfig.RecreateBackupTTL:
// a shorter one could expire copied keys before the copy finishes.
const MinRecreateBackupTTL = 10 * time.Minute

// streamStore is the part of the JetStream API the storage mismatch check
// needs, so tests can inject lookup and delete outcomes.
type streamStore = jsrecreate.StreamStore

// streamAPI adapts jetstream.JetStream to streamStore.
type streamAPI struct {
	js jetstream.JetStream
}

// StreamInfo returns the info the stream lookup already fetched, without a
// second request.
func (s streamAPI) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	return jsrecreate.StreamAPI{JS: s.js}.StreamInfo(ctx, name)
}

// DeleteStream deletes the named stream.
func (s streamAPI) DeleteStream(ctx context.Context, name string) error {
	return s.js.DeleteStream(ctx, name)
}

// jetStreamStorage maps a bucket's StorageType onto JetStream's: anything
// other than MemoryStorage is file storage. The bucket is created and
// compared with the same mapping.
func jetStreamStorage(s StorageType) jetstream.StorageType {
	if s == MemoryStorage {
		return jetstream.MemoryStorage
	}
	return jetstream.FileStorage
}

// openBucket creates or opens the bucket. When that fails because its stream
// exists with another storage type, which NATS cannot change in place, it
// returns ErrStorageMismatch or, with RecreateOnStorageMismatch, backs the
// bucket up, deletes its stream and creates it again (see jsrecreate.Open).
func (m *PluginModule) openBucket(ctx context.Context, streams streamStore, cfg BucketConfig) (jetstream.KeyValue, error) {
	return jsrecreate.Open(ctx, jsrecreate.Spec[jetstream.KeyValue]{
		JS:           m.js,
		Streams:      streams,
		Logger:       m.logger,
		Bucket:       cfg.Name,
		StreamPrefix: kvStreamPrefix,
		Subjects:     []string{"$KV." + cfg.Name + ".>"},
		Want:         jetStreamStorage(cfg.Storage),
		Options: jsrecreate.Options{
			Recreate:   cfg.RecreateOnStorageMismatch,
			SkipBackup: cfg.SkipBackupOnRecreate,
			BackupTTL:  cfg.RecreateBackupTTL,
		},
		Kind:        "KV bucket",
		ContentNoun: "keys",
		ErrMismatch: ErrStorageMismatch,
		Create: func(ctx context.Context) (jetstream.KeyValue, error) {
			return m.createOrGetKVStore(ctx, cfg)
		},
		CreateBackup: func(ctx context.Context, backup string, info *jetstream.StreamInfo, ttl time.Duration) error {
			return m.createBackupBucket(ctx, cfg, backup, info, ttl)
		},
		DeleteBackup: func(ctx context.Context, backup string) error {
			return m.js.DeleteKeyValue(ctx, backup)
		},
	})
}

// createBackupBucket creates the file-storage KV bucket backup, sourcing the
// stream of bucket cfg.Name with its keys mapped to the backup's ($KV.<bucket>.>
// to $KV.<backup>.>), with
// the stream's per-key history (clamped to the KV limits), replicas,
// placement and compression.
func (m *PluginModule) createBackupBucket(ctx context.Context, cfg BucketConfig, backup string, info *jetstream.StreamInfo, ttl time.Duration) error {
	history := info.Config.MaxMsgsPerSubject
	if history < 1 {
		history = 1
	}
	if history > jetstream.KeyValueMaxHistory {
		history = jetstream.KeyValueMaxHistory
	}
	replicas := info.Config.Replicas
	if replicas < 1 {
		replicas = 1
	}
	_, err := m.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      backup,
		Description: fmt.Sprintf("Backup of bucket %s, taken before it was recreated with %s storage", cfg.Name, jetStreamStorage(cfg.Storage)),
		History:     uint8(history), //nolint:gosec // G115: clamped to 1..KeyValueMaxHistory above
		TTL:         ttl,
		Storage:     jetstream.FileStorage,
		Replicas:    replicas,
		Placement:   info.Config.Placement,
		Compression: info.Config.Compression != jetstream.NoCompression,
		// The full stream name with its own subject transform: given a bare
		// bucket name, nats.go takes one that starts with "KV_" for a stream
		// name, so bucket "KV_x" was backed up from the stream of bucket "x".
		Sources: []*jetstream.StreamSource{{
			Name: kvStreamPrefix + cfg.Name,
			SubjectTransforms: []jetstream.SubjectTransformConfig{{
				Source:      "$KV." + cfg.Name + ".>",
				Destination: "$KV." + backup + ".>",
			}},
		}},
	})
	return err
}

// validateRecreate checks the recreate settings of a bucket.
func validateRecreate(b BucketConfig) error {
	return jsrecreate.ValidateOptions(b.Name, kvStreamPrefix, jsrecreate.Options{
		Recreate:   b.RecreateOnStorageMismatch,
		SkipBackup: b.SkipBackupOnRecreate,
		BackupTTL:  b.RecreateBackupTTL,
	}, MinRecreateBackupTTL)
}
