package kvjetstream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// kvStreamPrefix is the name prefix JetStream gives the stream of a KV bucket
// (KV_<bucket>). It is part of the KV wire protocol; nats.go keeps its own
// constant unexported.
const kvStreamPrefix = "KV_"

// DefaultRecreateBackupTTL is how long the keys of a backup bucket made by
// RecreateOnStorageMismatch are kept when BucketConfig.RecreateBackupTTL is 0.
const DefaultRecreateBackupTTL = 7 * 24 * time.Hour

// backupCopyTimeout bounds the copy of a bucket into its backup;
// backupPollInterval is how often its progress is checked.
const (
	backupCopyTimeout  = 2 * time.Minute
	backupPollInterval = 50 * time.Millisecond
	// backupSourceErrorRounds is how many polls in a row the backup's source
	// may report an error before the backup is given up.
	backupSourceErrorRounds = 40
)

// jsErrCodeStreamInvalidConfig is the JetStream API error code (10052) the
// server answers a stream update that changes the storage type with. It is
// the generic invalid configuration code, so it alone does not identify a
// storage change, but a create that failed with any other code was rejected
// for another reason, which deleting the stream would not fix.
const jsErrCodeStreamInvalidConfig jetstream.ErrorCode = 10052

// streamStore is the part of the JetStream API the storage mismatch check
// needs, so tests can inject lookup and delete outcomes.
type streamStore interface {
	StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error)
	DeleteStream(ctx context.Context, name string) error
}

// streamAPI adapts jetstream.JetStream to streamStore.
type streamAPI struct {
	js jetstream.JetStream
}

// StreamInfo returns the info the stream lookup already fetched, without a
// second request.
func (s streamAPI) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	stream, err := s.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return stream.CachedInfo(), nil
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
// returns ErrStorageMismatch or, with RecreateOnStorageMismatch, deletes the
// stream and creates the bucket again.
//
// The stream is inspected only after the create failed, so a healthy start
// makes no extra request. The server's error code for this (10052) is the
// generic invalid stream configuration code, so the cause is established by
// comparing the storage types; when the lookup fails, the original error is
// returned unchanged, and when the types match or the stream is missing, the
// create is tried once more and both errors are returned if it fails again.
// The stream is deleted only when the create was rejected as an invalid
// configuration (10052), the code the server uses for a storage change.
func (m *PluginModule) openBucket(ctx context.Context, streams streamStore, cfg BucketConfig) (jetstream.KeyValue, error) {
	kv, createErr := m.createOrGetKVStore(ctx, cfg)
	if createErr == nil {
		return kv, nil
	}

	streamName := kvStreamPrefix + cfg.Name
	want := jetStreamStorage(cfg.Storage)
	info, err := streams.StreamInfo(ctx, streamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) || (err == nil && info != nil && info.Config.Storage == want) {
		// Another instance starting at the same time may have recreated the
		// bucket (or be about to) between the failed create and this lookup:
		// try once more before reporting the original failure.
		kv, retryErr := m.createOrGetKVStore(ctx, cfg)
		if retryErr == nil {
			return kv, nil
		}
		if retryErr.Error() == createErr.Error() {
			return nil, createErr
		}
		return nil, fmt.Errorf("%w (retried after the stream was found missing or matching: %w)", createErr, retryErr)
	}
	if err != nil || info == nil {
		return nil, createErr
	}
	have := info.Config.Storage

	if !cfg.RecreateOnStorageMismatch {
		return nil, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and NATS cannot change the storage type of an existing stream: delete stream %s "+
			"(nats stream rm %s, which deletes the bucket's keys) or, if its contents can be lost, "+
			"set BucketConfig.RecreateOnStorageMismatch: %w",
			ErrStorageMismatch, cfg.Name, have, want, streamName, streamName, createErr)
	}
	if !isPlainKVStream(info, cfg.Name) {
		return nil, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and stream %s is not a plain KV bucket (a mirror or other subjects), so it is not recreated: %w",
			ErrStorageMismatch, cfg.Name, have, want, streamName, createErr)
	}
	var apiErr *jetstream.APIError
	if !errors.As(createErr, &apiErr) || apiErr.ErrorCode != jsErrCodeStreamInvalidConfig {
		return nil, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and the create failed for another reason, which deleting stream %s would not fix, so it is not recreated: %w",
			ErrStorageMismatch, cfg.Name, have, want, streamName, createErr)
	}

	// Copy the keys to a backup bucket first, so the delete below loses
	// nothing that existed when the stream was read. A failed backup leaves
	// the bucket as it is.
	backup, repaired, err := m.backupBucket(ctx, streams, cfg, info)
	if err != nil {
		return nil, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and its backup failed, so it is not recreated (nothing was deleted): %w",
			ErrStorageMismatch, cfg.Name, have, want, err)
	}
	// Another instance may have repaired the bucket while it was copied, or
	// after (checked once more right before the delete).
	if repaired || bucketRepaired(ctx, streams, streamName, info, want) {
		if backup != "" {
			m.deleteBackup(ctx, backup)
		}
		kv, err := m.createOrGetKVStore(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("bucket %q was repaired by another instance, but opening it failed: %w", cfg.Name, err)
		}
		return kv, nil
	}

	// The message counts and the backup name are the record of what the
	// delete removed: whether a bucket's contents can be lost is the
	// application's call, which the plugin cannot check.
	m.logger.Warn("Recreating KV bucket whose storage type does not match its configuration; its keys are deleted",
		"bucket", cfg.Name,
		"stream", streamName,
		"existing_storage", have.String(),
		"configured_storage", want.String(),
		"messages", info.State.Msgs,
		"bytes", info.State.Bytes,
		"backup_bucket", backup,
	)
	// Another instance doing the same repair may have deleted it first.
	if err := streams.DeleteStream(ctx, streamName); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, fmt.Errorf("failed to delete stream %s of bucket %q to change its storage type: %w", streamName, cfg.Name, err)
	}
	kv, err = m.createOrGetKVStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("stream %s was deleted, but recreating bucket %q with %s storage failed (the next Start creates it): %w",
			streamName, cfg.Name, want, err)
	}
	return kv, nil
}

// backupBucket copies the bucket described by info into a new file-storage
// bucket <name>-backup-<UTC time>-<random> and returns its name, or "" when
// no backup is made (SkipBackupOnRecreate, or an empty bucket). The copy is
// made by sourcing the bucket's stream; once it is complete (see
// completeBackup) the source is removed, so the backup does not copy what the
// recreated bucket receives.
//
// repaired is true when the bucket's stream turned out to be gone, replaced
// or already of the configured storage type while the copy ran: another
// instance starting at the same time has repaired it. The backup is then
// dropped and the caller must not delete anything. On error the partial
// backup is dropped (best effort) and the bucket is left as it is.
func (m *PluginModule) backupBucket(ctx context.Context, streams streamStore, cfg BucketConfig, info *jetstream.StreamInfo) (name string, repaired bool, err error) {
	if cfg.SkipBackupOnRecreate || info.State.Msgs == 0 {
		return "", false, nil
	}
	name, err = backupBucketName(cfg.Name)
	if err != nil {
		return "", false, err
	}
	ttl := cfg.RecreateBackupTTL
	switch {
	case ttl == 0:
		ttl = DefaultRecreateBackupTTL
	case ttl < 0:
		ttl = 0 // keep forever
	}
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

	ctx, cancel := context.WithTimeout(ctx, backupCopyTimeout)
	defer cancel()
	if _, err := m.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      name,
		Description: fmt.Sprintf("Backup of bucket %s, taken before it was recreated with %s storage", cfg.Name, jetStreamStorage(cfg.Storage)),
		History:     uint8(history), //nolint:gosec // G115: clamped to 1..KeyValueMaxHistory above
		TTL:         ttl,
		Storage:     jetstream.FileStorage,
		Replicas:    replicas,
		Placement:   info.Config.Placement,
		Compression: info.Config.Compression != jetstream.NoCompression,
		Sources:     []*jetstream.StreamSource{{Name: cfg.Name}},
	}); err != nil {
		// A create that timed out on the client may still have succeeded.
		// The random name cannot belong to anyone else, unless it exists.
		if !errors.Is(err, jetstream.ErrBucketExists) {
			m.deleteBackup(ctx, name)
		}
		return "", false, fmt.Errorf("failed to create backup bucket %q: %w", name, err)
	}
	repaired, err = m.completeBackup(ctx, streams, cfg, name, info)
	if err != nil || repaired {
		m.deleteBackup(ctx, name)
		return "", repaired, err
	}
	return name, false, nil
}

// completeBackup waits until the backup bucket has copied the bucket's
// stream, then removes the source from the backup's configuration. The copy
// is complete when the last message stored in the backup comes from a source
// sequence at or after the stream's last sequence as read in info, or when
// the source has no lag and the stream holds no message after that sequence
// (its last messages expired or were removed before they were copied).
// Counting messages would not work: a bucket with a TTL loses keys while the
// copy runs.
//
// On every round it also reads the bucket's stream again, and returns
// repaired when it is gone, was replaced (another creation time) or already
// has the configured storage type.
func (m *PluginModule) completeBackup(ctx context.Context, streams streamStore, cfg BucketConfig, backup string, info *jetstream.StreamInfo) (repaired bool, err error) {
	streamName := kvStreamPrefix + cfg.Name
	backupStream, err := m.js.Stream(ctx, kvStreamPrefix+backup)
	if err != nil {
		return false, fmt.Errorf("failed to read backup bucket %q: %w", backup, err)
	}
	ticker := time.NewTicker(backupPollInterval)
	defer ticker.Stop()
	sourceErrors := 0
	for {
		if bucketRepaired(ctx, streams, streamName, info, jetStreamStorage(cfg.Storage)) {
			return true, nil
		}
		binfo, err := backupStream.Info(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to read backup bucket %q: %w", backup, err)
		}
		if len(binfo.Sources) == 0 {
			return false, fmt.Errorf("backup bucket %q has no source", backup)
		}
		src := binfo.Sources[0]
		if src.Error != nil {
			if sourceErrors++; sourceErrors >= backupSourceErrorRounds {
				return false, fmt.Errorf("backup bucket %q cannot copy stream %s: %w", backup, streamName, src.Error)
			}
		} else {
			sourceErrors = 0
		}
		done, copied, err := m.backupCopied(ctx, backupStream, binfo, streamName, info.State.LastSeq)
		if err != nil {
			return false, err
		}
		if done {
			bcfg := binfo.Config
			bcfg.Sources = nil
			updated, err := m.js.UpdateStream(ctx, bcfg)
			if err != nil {
				return false, fmt.Errorf("failed to detach backup bucket %q from its source: %w", backup, err)
			}
			if updated != nil && len(updated.CachedInfo().Config.Sources) != 0 {
				return false, fmt.Errorf("backup bucket %q still has a source after it was detached", backup)
			}
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("backup bucket %q did not finish copying stream %s (copied through sequence %d of %d): %w",
				backup, streamName, copied, info.State.LastSeq, ctx.Err())
		case <-ticker.C:
		}
	}
}

// backupCopied reports whether the backup described by binfo has copied the
// source stream through lastSeq, and the source sequence it has copied
// through (from the Nats-Stream-Source header of its last message).
func (m *PluginModule) backupCopied(ctx context.Context, backup jetstream.Stream, binfo *jetstream.StreamInfo, streamName string, lastSeq uint64) (bool, uint64, error) {
	var copied uint64
	if binfo.State.Msgs > 0 {
		msg, err := backup.GetMsg(ctx, binfo.State.LastSeq)
		if err != nil {
			return false, 0, fmt.Errorf("failed to read the last message of backup stream %s: %w", backup.CachedInfo().Config.Name, err)
		}
		seq, ok := sourceSequence(msg.Header.Get(streamSourceHeader))
		if !ok {
			return false, 0, fmt.Errorf("the last message of backup stream %s has no source sequence", backup.CachedInfo().Config.Name)
		}
		copied = seq
	}
	if copied >= lastSeq {
		return true, copied, nil
	}
	src := binfo.Sources[0]
	if src.Active < 0 || src.Lag != 0 {
		return false, copied, nil
	}
	// Caught up with no lag, but short of lastSeq: done if the stream holds
	// nothing after what was copied (the rest expired before it was read).
	original, err := m.js.Stream(ctx, streamName)
	if err != nil {
		return false, copied, fmt.Errorf("failed to read stream %s: %w", streamName, err)
	}
	if _, err := original.GetMsg(ctx, copied+1, jetstream.WithGetMsgSubject(">")); err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return true, copied, nil
		}
		return false, copied, fmt.Errorf("failed to read stream %s after sequence %d: %w", streamName, copied, err)
	}
	return false, copied, nil
}

// streamSourceHeader is the header JetStream sets on a message stored by
// sourcing: "<source stream> <source sequence> ...".
const streamSourceHeader = "Nats-Stream-Source"

// sourceSequence parses the source sequence of a Nats-Stream-Source header.
func sourceSequence(header string) (uint64, bool) {
	fields := strings.Fields(header)
	if len(fields) < 2 {
		return 0, false
	}
	seq, err := strconv.ParseUint(fields[1], 10, 64)
	return seq, err == nil
}

// bucketRepaired reports whether the bucket's stream, read again, is gone,
// was replaced since info was read, or already has storage want: another
// instance starting at the same time has repaired it. A failed read is not a
// repair.
func bucketRepaired(ctx context.Context, streams streamStore, streamName string, info *jetstream.StreamInfo, want jetstream.StorageType) bool {
	cur, err := streams.StreamInfo(ctx, streamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return true
	}
	if err != nil || cur == nil {
		return false
	}
	return !cur.Created.Equal(info.Created) || cur.Config.Storage == want
}

// deleteBackup removes a partial backup, best effort: a leftover only costs
// storage, so a failure is logged.
func (m *PluginModule) deleteBackup(ctx context.Context, backup string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := m.js.DeleteKeyValue(ctx, backup); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		m.logger.Warn("Failed to delete incomplete KV bucket backup", "backup_bucket", backup, "error", err)
	}
}

// backupNameSuffixLen is the length backupBucketName adds to a bucket name:
// "-backup-", a UTC time (16) and "-" plus 16 hex digits.
const backupNameSuffixLen = len("-backup-") + len("20060102T150405Z") + 1 + 16

// backupBucketName returns <bucket>-backup-<UTC time>-<16 hex digits>; the
// random part keeps the backups of instances that start in the same second
// apart.
func backupBucketName(bucket string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to name backup bucket: %w", err)
	}
	return fmt.Sprintf("%s-backup-%s-%s", bucket, time.Now().UTC().Format("20060102T150405Z"), hex.EncodeToString(b[:])), nil
}

// MinRecreateBackupTTL is the smallest positive BucketConfig.RecreateBackupTTL:
// a shorter one could expire copied keys before the copy finishes.
const MinRecreateBackupTTL = 10 * time.Minute

// maxStreamNameLen is the longest stream name JetStream accepts.
const maxStreamNameLen = 255

// validateRecreate checks the recreate settings of a bucket.
func validateRecreate(b BucketConfig) error {
	if !b.RecreateOnStorageMismatch {
		if b.SkipBackupOnRecreate || b.RecreateBackupTTL != 0 {
			return fmt.Errorf("bucket %q: SkipBackupOnRecreate and RecreateBackupTTL require RecreateOnStorageMismatch", b.Name)
		}
		return nil
	}
	if b.SkipBackupOnRecreate && b.RecreateBackupTTL != 0 {
		return fmt.Errorf("bucket %q: RecreateBackupTTL has no effect with SkipBackupOnRecreate", b.Name)
	}
	if b.RecreateBackupTTL > 0 && b.RecreateBackupTTL < MinRecreateBackupTTL {
		return fmt.Errorf("bucket %q: RecreateBackupTTL must be at least %v (or negative to keep the backup)", b.Name, MinRecreateBackupTTL)
	}
	if !b.SkipBackupOnRecreate && len(kvStreamPrefix)+len(b.Name)+backupNameSuffixLen > maxStreamNameLen {
		return fmt.Errorf("bucket %q: name too long for a backup bucket (at most %d characters with RecreateOnStorageMismatch; or set SkipBackupOnRecreate)",
			b.Name, maxStreamNameLen-len(kvStreamPrefix)-backupNameSuffixLen)
	}
	return nil
}

// isPlainKVStream reports whether info describes a KV bucket created for
// bucket itself: listening on $KV.<bucket>.> only, and not a mirror (whose
// contents belong to another bucket). Only such a stream is ever recreated.
func isPlainKVStream(info *jetstream.StreamInfo, bucket string) bool {
	if info.Config.Mirror != nil || len(info.Config.Sources) > 0 {
		return false
	}
	subjects := info.Config.Subjects
	return len(subjects) == 1 && subjects[0] == "$KV."+bucket+".>"
}
