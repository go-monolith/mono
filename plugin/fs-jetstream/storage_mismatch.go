package fsjetstream

import (
	"context"
	"encoding/json"
	"errors"
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
		AfterCopy: func(ctx context.Context, backup string) error {
			return m.renameBackupMeta(ctx, cfg.Name, backup)
		},
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
		return fmt.Errorf("failed to create object store %q: %w", backup, err)
	}
	stream, err := m.js.Stream(ctx, objStreamPrefix+backup)
	if err != nil {
		return fmt.Errorf("failed to read stream %s: %w", objStreamPrefix+backup, err)
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
// marker into the recreated bucket. A link to an object of bucket itself is
// pointed at the backup too, since the object it names is copied there and
// is gone from the recreated bucket; links to other buckets are left as they
// are. Each object's latest metadata is republished to its own subject as a
// rollup, every other field unchanged. Reading stops when ctx ends, so the
// rewrite stays within the copy timeout.
func (m *PluginModule) renameBackupMeta(ctx context.Context, bucket, backup string) error {
	backupStream := objStreamPrefix + backup
	stream, err := m.js.Stream(ctx, backupStream)
	if err != nil {
		return fmt.Errorf("failed to read backup bucket %q: %w", backup, err)
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
	// Read every object's latest metadata first: the rewrites land on the
	// same subjects, so publishing while reading would feed the reader.
	type rewrite struct {
		subject string
		data    []byte
	}
	var rewrites []rewrite
	for pending > 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("failed to read the metadata of backup bucket %q: %w", backup, err)
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(fetchWait(ctx, 5*time.Second)))
		if err != nil {
			return fmt.Errorf("failed to read the metadata of backup bucket %q: %w", backup, err)
		}
		md, err := msg.Metadata()
		if err != nil {
			return fmt.Errorf("failed to read the metadata of backup bucket %q: %w", backup, err)
		}
		data, err := renameMeta(msg.Data(), bucket, backup)
		if err != nil {
			return fmt.Errorf("backup bucket %q: object metadata on %s: %w", backup, msg.Subject(), err)
		}
		rewrites = append(rewrites, rewrite{subject: msg.Subject(), data: data})
		pending = md.NumPending
	}
	// Publish asynchronously, waiting for the acknowledgements of each batch,
	// so a bucket of many objects does not pay one round trip per object.
	const batch = 256
	futures := make([]jetstream.PubAckFuture, 0, batch)
	wait := func() error {
		for _, f := range futures {
			select {
			case <-f.Ok():
			case err := <-f.Err():
				return fmt.Errorf("failed to rewrite the metadata of %s in backup bucket %q: %w", f.Msg().Subject, backup, err)
			case <-ctx.Done():
				return fmt.Errorf("failed to rewrite the metadata of backup bucket %q: %w", backup, ctx.Err())
			}
		}
		futures = futures[:0]
		return nil
	}
	for _, r := range rewrites {
		out := nats.NewMsg(r.subject)
		out.Data = r.data
		out.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
		f, err := m.js.PublishMsgAsync(out, jetstream.WithExpectStream(backupStream))
		if err != nil {
			return fmt.Errorf("failed to rewrite the metadata of %s in backup bucket %q: %w", r.subject, backup, err)
		}
		if futures = append(futures, f); len(futures) == batch {
			if err := wait(); err != nil {
				return err
			}
		}
	}
	return wait()
}

// fetchWait returns limit, shortened to the time left before the deadline
// of ctx (at least 1ms, the shortest wait a fetch accepts).
func fetchWait(ctx context.Context, limit time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if left := time.Until(deadline); left < limit {
			return max(left, time.Millisecond)
		}
	}
	return limit
}

// renameMeta returns the object metadata data with its bucket set to backup
// and, for a link to an object of bucket, the link's bucket too; every other
// field is kept as it is.
func renameMeta(data []byte, bucket, backup string) ([]byte, error) {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	if meta == nil {
		return nil, errors.New("not a JSON object")
	}
	backupName, err := json.Marshal(backup)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the bucket name: %w", err)
	}
	meta["bucket"] = backupName

	if raw, ok := meta["options"]; ok {
		var opts map[string]json.RawMessage
		if err := json.Unmarshal(raw, &opts); err == nil && opts != nil {
			var link map[string]json.RawMessage
			if err := json.Unmarshal(opts["link"], &link); err == nil && link != nil {
				var target string
				if err := json.Unmarshal(link["bucket"], &target); err == nil && target == bucket {
					link["bucket"] = backupName
					if opts["link"], err = json.Marshal(link); err != nil {
						return nil, fmt.Errorf("failed to encode the link: %w", err)
					}
					if meta["options"], err = json.Marshal(opts); err != nil {
						return nil, fmt.Errorf("failed to encode the options: %w", err)
					}
				}
			}
		}
	}
	out, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the metadata: %w", err)
	}
	return out, nil
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
