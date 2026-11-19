package fsjetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/go-monolith/mono/internal/jsrecreate"
	"github.com/nats-io/nats.go/jetstream"
)

// objStreamPrefix is the name prefix JetStream gives the stream of an object
// store (OBJ_<bucket>). It is part of the object store wire protocol; nats.go
// keeps its own constant unexported.
const objStreamPrefix = "OBJ_"

// DefaultRecreateBackupTTL is how long the objects of a backup bucket made by
// RecreateOnStorageMismatch are kept when BucketConfig.RecreateBackupTTL is 0.
const DefaultRecreateBackupTTL = jsrecreate.DefaultBackupTTL

// MinRecreateBackupTTL is the smallest positive BucketConfig.RecreateBackupTTL.
const MinRecreateBackupTTL = time.Hour

// DefaultRecreateBackupTimeout bounds the copy of a bucket into its backup
// when BucketConfig.RecreateBackupTimeout is 0. It is longer than the KV
// plugin's: object buckets hold files.
const DefaultRecreateBackupTimeout = 10 * time.Minute

// streamAPI adapts jetstream.JetStream to jsrecreate.StreamStore.
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

// objectSubjects are the subjects of the stream of object store bucket: its
// chunks and its metadata.
func objectSubjects(bucket string) []string {
	return []string{"$O." + bucket + ".C.>", "$O." + bucket + ".M.>"}
}

// openBucket creates or opens the bucket. When that fails because its stream
// exists with another storage type, which NATS cannot change in place, it
// returns ErrStorageMismatch or, with RecreateOnStorageMismatch, backs the
// bucket up, deletes its stream and creates it again (see jsrecreate.Open).
func (m *PluginModule) openBucket(ctx context.Context, streams jsrecreate.StreamStore, cfg BucketConfig) (jetstream.ObjectStore, error) {
	timeout := cfg.RecreateBackupTimeout
	if timeout <= 0 {
		timeout = DefaultRecreateBackupTimeout
	}
	return jsrecreate.Open(ctx, jsrecreate.Spec[jetstream.ObjectStore]{
		JS:           m.js,
		Streams:      streams,
		Logger:       m.logger,
		Bucket:       cfg.Name,
		StreamPrefix: objStreamPrefix,
		Subjects:     objectSubjects(cfg.Name),
		Want:         jetStreamStorage(cfg.Storage),
		Options: jsrecreate.Options{
			Recreate:   cfg.RecreateOnStorageMismatch,
			SkipBackup: cfg.SkipBackupOnRecreate,
			BackupTTL:  cfg.RecreateBackupTTL,
		},
		Kind:        "object store bucket",
		ContentNoun: "objects",
		ErrMismatch: ErrStorageMismatch,
		CopyTimeout: timeout,
		Create: func(ctx context.Context) (jetstream.ObjectStore, error) {
			return m.createOrGetObjectStore(ctx, cfg)
		},
		CreateBackup: func(ctx context.Context, backup string, info *jetstream.StreamInfo, ttl time.Duration) error {
			return m.createBackupBucket(ctx, cfg, backup, info, ttl)
		},
		DeleteBackup: func(ctx context.Context, backup string) error {
			return m.js.DeleteObjectStore(ctx, backup)
		},
		AfterCopy: m.renameBackupMeta,
	})
}

// createBackupBucket creates the file-storage object store backup, with the
// stream's replicas, placement and compression, and makes it source the
// bucket's stream with its subjects mapped to the backup's ($O.<bucket>.> to
// $O.<backup>.>). An object's chunks are addressed by the bucket being read,
// so the backup is a complete, readable object store. nats.go cannot create
// an object store with a source, hence the stream update.
func (m *PluginModule) createBackupBucket(ctx context.Context, cfg BucketConfig, backup string, info *jetstream.StreamInfo, ttl time.Duration) error {
	replicas := info.Config.Replicas
	if replicas < 1 {
		replicas = 1
	}
	if _, err := m.js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      backup,
		Description: fmt.Sprintf("Backup of bucket %s, taken before it was recreated with %s storage", cfg.Name, jetStreamStorage(cfg.Storage)),
		TTL:         ttl,
		Storage:     jetstream.FileStorage,
		Replicas:    replicas,
		Placement:   info.Config.Placement,
		Compression: info.Config.Compression != jetstream.NoCompression,
	}); err != nil {
		return err
	}
	stream, err := m.js.Stream(ctx, objStreamPrefix+backup)
	if err != nil {
		return err
	}
	scfg := stream.CachedInfo().Config
	scfg.Sources = []*jetstream.StreamSource{{
		Name: objStreamPrefix + cfg.Name,
		SubjectTransforms: []jetstream.SubjectTransformConfig{{
			Source:      "$O." + cfg.Name + ".>",
			Destination: "$O." + backup + ".>",
		}},
	}}
	if _, err := m.js.UpdateStream(ctx, scfg); err != nil {
		return fmt.Errorf("failed to make backup bucket %q copy stream %s: %w", backup, objStreamPrefix+cfg.Name, err)
	}
	return nil
}

// renameBackupMeta rewrites the metadata of every object in backup so that it
// names the backup bucket. Sourcing maps the subjects but not the payloads,
// so the copied metadata still names the original bucket, and nats.go
// publishes the metadata of a Delete or UpdateMeta to the bucket the metadata
// names: deleting an object in the backup would otherwise write a delete
// marker into the recreated bucket. Each object's latest metadata is
// republished to its own subject as a rollup, every other field unchanged.
func (m *PluginModule) renameBackupMeta(ctx context.Context, backup string) error {
	stream, err := m.js.Stream(ctx, objStreamPrefix+backup)
	if err != nil {
		return err
	}
	metaSubjects := "$O." + backup + ".M.>"
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{metaSubjects},
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
	})
	if err != nil {
		return fmt.Errorf("failed to read the metadata of backup bucket %q: %w", backup, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the metadata of backup bucket %q: %w", backup, err)
	}
	pending := info.NumPending
	bucketName, err := json.Marshal(backup)
	if err != nil {
		return err
	}
	// Read every object's latest metadata first: the rewrites land on the
	// same subjects, so publishing while reading would feed the reader.
	type rewrite struct {
		subject string
		data    []byte
	}
	var rewrites []rewrite
	for pending > 0 {
		msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			return fmt.Errorf("failed to read the metadata of backup bucket %q: %w", backup, err)
		}
		md, err := msg.Metadata()
		if err != nil {
			return err
		}
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(msg.Data(), &meta); err != nil {
			return fmt.Errorf("backup bucket %q: object metadata on %s is not JSON: %w", backup, msg.Subject(), err)
		}
		meta["bucket"] = bucketName
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		rewrites = append(rewrites, rewrite{subject: msg.Subject(), data: data})
		pending = md.NumPending
	}
	for _, r := range rewrites {
		out := nats.NewMsg(r.subject)
		out.Data = r.data
		out.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
		if _, err := m.js.PublishMsg(ctx, out, jetstream.WithExpectStream(objStreamPrefix+backup)); err != nil {
			return fmt.Errorf("failed to rewrite the metadata of %s in backup bucket %q: %w", r.subject, backup, err)
		}
	}
	return nil
}

// validateRecreate checks the recreate settings of a bucket. A positive
// backup TTL must also exceed the copy timeout, so copied objects cannot
// expire before the copy finishes.
func validateRecreate(b BucketConfig) error {
	if err := jsrecreate.ValidateOptions(b.Name, objStreamPrefix, jsrecreate.Options{
		Recreate:   b.RecreateOnStorageMismatch,
		SkipBackup: b.SkipBackupOnRecreate,
		BackupTTL:  b.RecreateBackupTTL,
	}, MinRecreateBackupTTL); err != nil {
		return err
	}
	if b.RecreateBackupTimeout < 0 {
		return fmt.Errorf("bucket %q: RecreateBackupTimeout cannot be negative", b.Name)
	}
	if b.RecreateBackupTimeout != 0 && (!b.RecreateOnStorageMismatch || b.SkipBackupOnRecreate) {
		return fmt.Errorf("bucket %q: RecreateBackupTimeout requires RecreateOnStorageMismatch with a backup", b.Name)
	}
	timeout := b.RecreateBackupTimeout
	if timeout == 0 {
		timeout = DefaultRecreateBackupTimeout
	}
	ttl := b.RecreateBackupTTL
	if ttl == 0 {
		ttl = DefaultRecreateBackupTTL
	}
	if b.RecreateOnStorageMismatch && !b.SkipBackupOnRecreate && ttl > 0 && ttl <= timeout {
		return fmt.Errorf("bucket %q: the backup TTL (%v) must be longer than the backup timeout (%v)", b.Name, ttl, timeout)
	}
	return nil
}
