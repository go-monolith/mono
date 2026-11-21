// Package jsrecreate opens a JetStream-backed bucket (a KV bucket or an
// object store) whose stream may already exist with another storage type than
// the one configured. NATS cannot change the storage type of an existing
// stream, so such a bucket either fails with a clear error or, when the
// application opted in, is copied to a backup bucket, deleted and created
// again. It is shared by the kv-jetstream and fs-jetstream plugins.
package jsrecreate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultBackupTTL is how long a backup bucket keeps its contents when
	// Options.BackupTTL is 0.
	DefaultBackupTTL = 7 * 24 * time.Hour

	// DefaultCopyTimeout bounds the copy of a bucket into its backup when
	// Spec.CopyTimeout is 0.
	DefaultCopyTimeout = 2 * time.Minute

	// MaxStreamNameLen is the longest stream name JetStream accepts.
	MaxStreamNameLen = 255

	// BackupNameSuffixLen is the length BackupName adds to a bucket name:
	// "-backup-", a UTC time (16) and "-" plus 16 hex digits.
	BackupNameSuffixLen = len("-backup-") + len(backupTimeLayout) + 1 + 16

	// backupTimeLayout is the UTC time in a backup bucket name.
	backupTimeLayout = "20060102T150405Z"

	// pollInterval is how often the progress of a copy is checked.
	pollInterval = 50 * time.Millisecond

	// sourceErrorRounds is how many polls in a row the backup's source may
	// report an error before the backup is given up.
	sourceErrorRounds = 40

	// errCodeStreamInvalidConfig is the JetStream API error code (10052) the
	// server answers a stream update that changes the storage type with. It
	// is the generic invalid configuration code, so it alone does not
	// identify a storage change, but a create that failed with any other code
	// was rejected for another reason, which deleting the stream would not
	// fix.
	errCodeStreamInvalidConfig jetstream.ErrorCode = 10052

	// streamSourceHeader is the header JetStream sets on a message stored by
	// sourcing: "<source stream> <source sequence> ...".
	streamSourceHeader = "Nats-Stream-Source"
)

// StreamStore is the part of the JetStream API used to inspect and delete the
// bucket's stream, so tests can inject lookup and delete outcomes.
type StreamStore interface {
	StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error)
	DeleteStream(ctx context.Context, name string) error
}

// StreamAPI adapts jetstream.JetStream to StreamStore.
type StreamAPI struct {
	JS jetstream.JetStream
}

// StreamInfo returns the info the stream lookup already fetched, without a
// second request.
func (s StreamAPI) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	stream, err := s.JS.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return stream.CachedInfo(), nil
}

// DeleteStream deletes the named stream.
func (s StreamAPI) DeleteStream(ctx context.Context, name string) error {
	return s.JS.DeleteStream(ctx, name)
}

// Options are the per-bucket recreate settings of the plugin configuration.
type Options struct {
	// Recreate deletes and recreates a bucket whose stream has another
	// storage type (RecreateOnStorageMismatch).
	Recreate bool
	// SkipBackup recreates without a backup (SkipBackupOnRecreate).
	SkipBackup bool
	// BackupTTL is how long the backup keeps its contents: 0 means
	// DefaultBackupTTL, negative keeps them (RecreateBackupTTL).
	BackupTTL time.Duration
}

// Spec describes one bucket to open. T is the opened bucket's handle
// (jetstream.KeyValue or jetstream.ObjectStore).
type Spec[T any] struct {
	// JS is used to create, read and delete the backup bucket.
	JS jetstream.JetStream
	// Streams inspects and deletes the bucket's stream.
	Streams StreamStore
	Logger  *slog.Logger

	// Bucket is the bucket name; StreamPrefix makes its stream name
	// ("KV_", "OBJ_"); Subjects are the subjects of a plain bucket of that
	// name, the only kind of stream ever deleted.
	Bucket       string
	StreamPrefix string
	Subjects     []string
	// Want is the configured storage type.
	Want jetstream.StorageType
	Options

	// Kind names the bucket in the delete warning ("KV bucket");
	// ContentNoun names its contents in errors ("keys", "objects").
	Kind        string
	ContentNoun string
	// ErrMismatch is the plugin's sentinel wrapped by every mismatch error.
	ErrMismatch error
	// CopyTimeout bounds the copy into the backup (0: DefaultCopyTimeout).
	CopyTimeout time.Duration

	// Create creates or opens the bucket with its configuration.
	Create func(ctx context.Context) (T, error)
	// CreateBackup creates bucket backup, sourcing the bucket's stream, from
	// the stream as read in info, keeping its contents for ttl (0: forever).
	// When it returns an error other than jetstream.ErrBucketExists, the
	// backup is deleted.
	CreateBackup func(ctx context.Context, backup string, info *jetstream.StreamInfo, ttl time.Duration) error
	// DeleteBackup deletes bucket backup; not found is not an error.
	DeleteBackup func(ctx context.Context, backup string) error
	// AfterCopy, optional, finishes the backup once it holds the bucket's
	// contents and no longer follows the bucket (for example by rewriting
	// metadata that names the original bucket). An error fails the backup:
	// it is deleted and nothing else is.
	AfterCopy func(ctx context.Context, backup string) error
}

// check reports a Spec missing a required field, so a misconfigured caller
// gets an error instead of a panic.
func (s Spec[T]) check() error {
	switch {
	case s.JS == nil || s.Streams == nil || s.Logger == nil:
		return errors.New("jsrecreate: Spec needs JS, Streams and Logger")
	case s.Create == nil || s.CreateBackup == nil || s.DeleteBackup == nil:
		return errors.New("jsrecreate: Spec needs Create, CreateBackup and DeleteBackup")
	case s.ErrMismatch == nil:
		return errors.New("jsrecreate: Spec needs ErrMismatch")
	}
	return nil
}

// Open creates or opens the bucket. When that fails because its stream exists
// with another storage type, it returns an error wrapping ErrMismatch or,
// with Options.Recreate, copies the bucket to a backup bucket (unless
// SkipBackup or it is empty), deletes the stream and creates the bucket
// again.
//
// The stream is inspected only after the create failed, so a healthy start
// makes no extra request. The server's error code for this (10052) is the
// generic invalid stream configuration code, so the cause is established by
// comparing the storage types; when the lookup fails, the original error is
// returned unchanged, and when the types match or the stream is missing, the
// create is tried once more and both errors are returned if it fails again.
// The stream is deleted only when the create was rejected as an invalid
// configuration (10052), the code the server uses for a storage change.
//
// After a recreate, backups of the bucket that an interrupted recreate left
// copying its stream are detached (see detachLeftoverBackups).
func Open[T any](ctx context.Context, s Spec[T]) (T, error) {
	var zero T
	if err := s.check(); err != nil {
		return zero, err
	}
	h, createErr := s.Create(ctx)
	if createErr == nil {
		return h, nil
	}

	streamName := s.StreamPrefix + s.Bucket
	want := s.Want
	info, err := s.Streams.StreamInfo(ctx, streamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) || (err == nil && info != nil && info.Config.Storage == want) {
		// Another instance starting at the same time may have recreated the
		// bucket (or be about to) between the failed create and this lookup:
		// try once more before reporting the original failure.
		h, retryErr := s.Create(ctx)
		if retryErr == nil {
			return h, nil
		}
		if retryErr.Error() == createErr.Error() {
			return zero, createErr
		}
		return zero, fmt.Errorf("%w (retried after the stream was found missing or matching: %w)", createErr, retryErr)
	}
	if err != nil || info == nil {
		return zero, createErr
	}
	have := info.Config.Storage

	if !s.Recreate {
		return zero, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and NATS cannot change the storage type of an existing stream: delete stream %s "+
			"(nats stream rm %s, which deletes the bucket's %s) or, if its contents can be lost, "+
			"set BucketConfig.RecreateOnStorageMismatch: %w",
			s.ErrMismatch, s.Bucket, have, want, streamName, streamName, s.ContentNoun, createErr)
	}
	if !isPlainStream(info, s.Subjects) {
		return zero, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and stream %s is not a plain %s (a mirror or other subjects), so it is not recreated: %w",
			s.ErrMismatch, s.Bucket, have, want, streamName, s.Kind, createErr)
	}
	var apiErr *jetstream.APIError
	if !errors.As(createErr, &apiErr) || apiErr.ErrorCode != errCodeStreamInvalidConfig {
		return zero, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and the create failed for another reason, which deleting stream %s would not fix, so it is not recreated: %w",
			s.ErrMismatch, s.Bucket, have, want, streamName, createErr)
	}

	// Copy the contents to a backup bucket first, so the delete below loses
	// nothing that existed when the stream was read. A failed backup leaves
	// the bucket as it is.
	backup, repaired, err := backupBucket(ctx, s, info)
	if err != nil {
		return zero, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and its backup failed, so it is not recreated (nothing was deleted): %w",
			s.ErrMismatch, s.Bucket, have, want, err)
	}
	// Another instance may have repaired the bucket while it was copied, or
	// after (checked once more right before the delete).
	if repaired || bucketRepaired(ctx, s.Streams, streamName, info, want) {
		if backup != "" {
			deleteBackup(ctx, s, backup)
		}
		h, err := s.Create(ctx)
		if err != nil {
			return zero, fmt.Errorf("bucket %q was repaired by another instance, but opening it failed: %w", s.Bucket, err)
		}
		return h, nil
	}

	// The message counts and the backup name are the record of what the
	// delete removed: whether a bucket's contents can be lost is the
	// application's call, which the plugin cannot check.
	s.Logger.Warn("Recreating "+s.Kind+" whose storage type does not match its configuration; its "+s.ContentNoun+" are deleted",
		"bucket", s.Bucket,
		"stream", streamName,
		"existing_storage", have.String(),
		"configured_storage", want.String(),
		"messages", info.State.Msgs,
		"bytes", info.State.Bytes,
		"backup_bucket", backup,
	)
	// Another instance doing the same repair may have deleted it first.
	if err := s.Streams.DeleteStream(ctx, streamName); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return zero, fmt.Errorf("failed to delete stream %s of bucket %q to change its storage type: %w", streamName, s.Bucket, err)
	}
	h, err = s.Create(ctx)
	// A backup an interrupted recreate left attached follows the stream by
	// name, so it would copy everything the recreated bucket receives. Done
	// after the create, so that the time without a bucket is not prolonged,
	// and whatever the create returned: after a failed one the next Start
	// finds no mismatch and never comes here again.
	detachLeftoverBackups(ctx, s, backup)
	if err != nil {
		return zero, fmt.Errorf("stream %s was deleted, but recreating bucket %q with %s storage failed (the next Start creates it): %w",
			streamName, s.Bucket, want, err)
	}
	return h, nil
}

const (
	// maxLeftoverBackups bounds how many leftover backups one recreate
	// inspects.
	maxLeftoverBackups = 64

	// leftoverTimeout bounds the search for leftover backups.
	leftoverTimeout = 10 * time.Second
)

// detachLeftoverBackups removes the source from the backup buckets of the
// bucket that still copy its stream: backups of a recreate that was
// interrupted (the process was killed during the copy) before it detached
// them. A stream source names its stream, so such a backup attaches itself to
// the recreated bucket and copies whatever that receives, into file storage,
// for as long as the backup lives. current, the backup just made, is skipped.
//
// It runs after the bucket's stream was deleted and created again, so a copy
// another instance is making at the same moment is not cut off before that
// instance can see the repair (completeBackup checks for it again when it
// finds its source gone, or cannot read the stream). The backup of such an
// instance may be reported here as a leftover; that instance then deletes it. A leftover is detached, never deleted: it holds part of what the
// bucket held, and whether that is still wanted is not for the plugin to
// decide. Only names BackupName makes, with exactly this stream as their one
// source, are touched; an application's own bucket that copies this one is
// left alone. A detached leftover is then finished like a backup of this
// recreate (Spec.AfterCopy). Best effort: a failure is logged and the bucket
// stays open.
func detachLeftoverBackups[T any](ctx context.Context, s Spec[T], current string) {
	// With a time of its own: the context of a create that just failed has
	// often ended.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leftoverTimeout)
	defer cancel()
	streamName := s.StreamPrefix + s.Bucket

	var candidates []string
	lister := s.JS.StreamNames(ctx)
	for name := range lister.Name() {
		bucket, ok := strings.CutPrefix(name, s.StreamPrefix)
		if ok && bucket != current && IsBackupName(s.Bucket, bucket) {
			candidates = append(candidates, bucket)
		}
	}
	if err := lister.Err(); err != nil {
		s.Logger.Warn("Failed to look for leftover "+s.Kind+" backups that still copy the bucket",
			"bucket", s.Bucket, "error", err)
		return
	}
	if len(candidates) > maxLeftoverBackups {
		s.Logger.Warn("Too many "+s.Kind+" backups to inspect them all for one that still copies the bucket",
			"bucket", s.Bucket, "backups", len(candidates), "inspected", maxLeftoverBackups)
		candidates = candidates[:maxLeftoverBackups]
	}
	for _, backup := range candidates {
		stream, err := s.JS.Stream(ctx, s.StreamPrefix+backup)
		if err != nil {
			if !errors.Is(err, jetstream.ErrStreamNotFound) {
				s.Logger.Warn("Failed to inspect "+s.Kind+" backup", "bucket", s.Bucket, "backup_bucket", backup, "error", err)
			}
			continue
		}
		info := stream.CachedInfo()
		if len(info.Config.Sources) != 1 || info.Config.Sources[0] == nil || info.Config.Sources[0].Name != streamName {
			continue
		}
		cfg := info.Config
		cfg.Sources = nil
		if _, err := s.JS.UpdateStream(ctx, cfg); err != nil {
			s.Logger.Warn("Failed to detach a leftover "+s.Kind+" backup that still copies the bucket; it keeps copying what the bucket receives",
				"bucket", s.Bucket, "backup_bucket", backup, "error", err)
			continue
		}
		s.Logger.Warn("Detached a leftover "+s.Kind+" backup that still copied the bucket: an earlier recreate was interrupted before it finished, so it may be incomplete; it is kept, delete it when it is not needed",
			"bucket", s.Bucket,
			"backup_bucket", backup,
			"stream", s.StreamPrefix+backup,
			"messages", info.State.Msgs)
		// The interrupted recreate never finished the backup either (for an
		// object store: its metadata still names the bucket, so deleting an
		// object in the backup would write into the bucket).
		if s.AfterCopy != nil {
			if err := s.AfterCopy(ctx, backup); err != nil {
				s.Logger.Warn("Failed to finish a leftover "+s.Kind+" backup after detaching it; do not change single "+s.ContentNoun+" in it, delete it as a whole",
					"bucket", s.Bucket, "backup_bucket", backup, "error", err)
			}
		}
	}
}

// IsBackupName reports whether name is a backup bucket name of bucket as
// BackupName makes them: <bucket>-backup-<UTC time>-<16 hex digits>.
func IsBackupName(bucket, name string) bool {
	rest, ok := strings.CutPrefix(name, bucket+"-backup-")
	if !ok || len(rest) != BackupNameSuffixLen-len("-backup-") {
		return false
	}
	n := len(backupTimeLayout)
	stamp, random := rest[:n], rest[n+1:]
	if rest[n] != '-' {
		return false
	}
	if _, err := time.Parse(backupTimeLayout, stamp); err != nil {
		return false
	}
	_, err := hex.DecodeString(random)
	return err == nil && strings.ToLower(random) == random
}

// backupBucket copies the bucket described by info into a new backup bucket
// <name>-backup-<UTC time>-<random> and returns its name, or "" when no
// backup is made (SkipBackup, or an empty bucket). The copy is made by
// sourcing the bucket's stream; once it is complete (see completeBackup) the
// source is removed, so the backup does not copy what the recreated bucket
// receives.
//
// repaired is true when the bucket's stream turned out to be gone, replaced
// or already of the configured storage type while the copy ran: another
// instance starting at the same time has repaired it. The backup is then
// dropped and the caller must not delete anything. On error the partial
// backup is dropped (best effort) and the bucket is left as it is.
func backupBucket[T any](ctx context.Context, s Spec[T], info *jetstream.StreamInfo) (name string, repaired bool, err error) {
	if s.SkipBackup || info.State.Msgs == 0 {
		return "", false, nil
	}
	name, err = BackupName(s.Bucket)
	if err != nil {
		return "", false, err
	}
	ttl := s.BackupTTL
	switch {
	case ttl == 0:
		ttl = DefaultBackupTTL
	case ttl < 0:
		ttl = 0 // keep forever
	}
	timeout := s.CopyTimeout
	if timeout <= 0 {
		timeout = DefaultCopyTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.CreateBackup(ctx, name, info, ttl); err != nil {
		// A create that timed out on the client may still have succeeded.
		// The random name cannot belong to anyone else, unless it exists.
		if !errors.Is(err, jetstream.ErrBucketExists) {
			deleteBackup(ctx, s, name)
		}
		return "", false, fmt.Errorf("failed to create backup bucket %q: %w", name, err)
	}
	repaired, err = completeBackup(ctx, s, name, info)
	if err != nil || repaired {
		deleteBackup(ctx, s, name)
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
// Counting messages would not work: a bucket with a TTL loses contents while
// the copy runs.
//
// On every round it also reads the bucket's stream again, and returns
// repaired when it is gone, was replaced (another creation time) or already
// has the configured storage type.
func completeBackup[T any](ctx context.Context, s Spec[T], backup string, info *jetstream.StreamInfo) (repaired bool, err error) {
	streamName := s.StreamPrefix + s.Bucket
	backupStream, err := s.JS.Stream(ctx, s.StreamPrefix+backup)
	if err != nil {
		return false, fmt.Errorf("failed to read backup bucket %q: %w", backup, err)
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	sourceErrors := 0
	for {
		if bucketRepaired(ctx, s.Streams, streamName, info, s.Want) {
			return true, nil
		}
		binfo, err := backupStream.Info(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to read backup bucket %q: %w", backup, err)
		}
		if len(binfo.Sources) == 0 {
			// Another instance that finished the repair first detaches
			// the backups still copying the bucket, this one included.
			if bucketRepaired(ctx, s.Streams, streamName, info, s.Want) {
				return true, nil
			}
			return false, fmt.Errorf("backup bucket %q has no source", backup)
		}
		src := binfo.Sources[0]
		// A backup that copies another stream must never stand for this one:
		// the bucket is deleted once the copy counts as complete.
		if len(binfo.Sources) != 1 || src.Name != streamName {
			return false, fmt.Errorf("backup bucket %q copies stream %s, not only stream %s", backup, SourceNames(binfo.Sources), streamName)
		}
		if src.Error != nil {
			if sourceErrors++; sourceErrors >= sourceErrorRounds {
				return false, fmt.Errorf("backup bucket %q cannot copy stream %s: %w", backup, streamName, src.Error)
			}
		} else {
			sourceErrors = 0
		}
		done, copied, err := backupCopied(ctx, s.JS, backupStream, binfo, streamName, info.State.LastSeq)
		if err != nil {
			// The stream it reads may have been deleted and recreated by
			// another instance since the round began.
			if bucketRepaired(ctx, s.Streams, streamName, info, s.Want) {
				return true, nil
			}
			return false, err
		}
		if done {
			if err := CheckNothingSkipped(ctx, s, backupStream, backup, streamName, info.State.LastSeq); err != nil {
				return false, err
			}
			bcfg := binfo.Config
			bcfg.Sources = nil
			updated, err := s.JS.UpdateStream(ctx, bcfg)
			if err != nil {
				return false, fmt.Errorf("failed to detach backup bucket %q from its source: %w", backup, err)
			}
			if updated != nil && len(updated.CachedInfo().Config.Sources) != 0 {
				return false, fmt.Errorf("backup bucket %q still has a source after it was detached", backup)
			}
			if s.AfterCopy != nil {
				if err := s.AfterCopy(ctx, backup); err != nil {
					return false, fmt.Errorf("failed to finish backup bucket %q: %w", backup, err)
				}
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

// CheckNothingSkipped refuses a backup that holds fewer messages than the
// bucket. The copy counts as complete by the source sequence of the backup's
// last message, which does not see a message skipped before it: the backup
// stream drops a message that carries the Nats-Msg-Id of another one it
// stored within its duplicate window, and the whole copy fits into that
// window. (KV and object store writes carry no such ID; a foreign writer
// that sets and reuses one does.) The bucket would then be deleted with a
// key that is in no backup.
//
// The counts can only be compared when nothing was written to the bucket
// since it was read (its last sequence is still lastSeq): then every message
// it holds had to be copied, while the backup may hold more (messages that
// expired or were removed from the bucket after they were copied). When the
// bucket was written to in the meantime, as by instances of another version
// during a rolling deploy, the check is skipped, as it cannot tell a skipped
// message from one that is yet to be copied; that case is as it was.
func CheckNothingSkipped[T any](ctx context.Context, s Spec[T], backupStream jetstream.Stream, backup, streamName string, lastSeq uint64) error {
	cur, err := s.Streams.StreamInfo(ctx, streamName)
	if err != nil || cur == nil {
		// A stream that is gone, or cannot be read, is dealt with by the
		// repair check before the delete.
		return nil
	}
	if cur.State.LastSeq != lastSeq {
		return nil
	}
	binfo, err := backupStream.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to read backup bucket %q: %w", backup, err)
	}
	if binfo.State.Msgs >= cur.State.Msgs {
		return nil
	}
	return fmt.Errorf("backup bucket %q holds fewer messages (%d) than stream %s (%d): some were not copied. "+
		"One possible cause is a message that reuses the Nats-Msg-Id of another one (the backup drops it as a duplicate). "+
		"The %s cannot be backed up this way: remove stream %s by hand, or set SkipBackupOnRecreate",
		backup, binfo.State.Msgs, streamName, cur.State.Msgs, s.Kind, streamName)
}

// backupCopied reports whether the backup described by binfo has copied the
// source stream through lastSeq, and the source sequence it has copied
// through (from the Nats-Stream-Source header of its last message).
func backupCopied(ctx context.Context, js jetstream.JetStream, backup jetstream.Stream, binfo *jetstream.StreamInfo, streamName string, lastSeq uint64) (bool, uint64, error) {
	var copied uint64
	if binfo.State.Msgs > 0 {
		msg, err := backup.GetMsg(ctx, binfo.State.LastSeq)
		if err != nil {
			return false, 0, fmt.Errorf("failed to read the last message of backup stream %s: %w", binfo.Config.Name, err)
		}
		header := msg.Header.Get(streamSourceHeader)
		seq, ok := SourceSequence(header)
		if !ok {
			return false, 0, fmt.Errorf("the last message of backup stream %s has no source sequence", binfo.Config.Name)
		}
		// A sequence of another stream says nothing about this one.
		if from := SourceStream(header); from != streamName {
			return false, 0, fmt.Errorf("the last message of backup stream %s was copied from stream %s, not %s", binfo.Config.Name, from, streamName)
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
	holds, err := HoldsMsgAfter(ctx, js, streamName, copied)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			// The stream has no leader at the moment: asked again on the
			// next round, within the copy timeout.
			return false, copied, nil
		}
		return false, copied, fmt.Errorf("failed to read stream %s after sequence %d: %w", streamName, copied, err)
	}
	return !holds, copied, nil
}

// HoldsMsgAfter reports whether the stream holds a message after sequence
// seq, as answered by the stream's leader.
//
// Stream.GetMsg cannot be used for it: for a stream with AllowDirect, which
// every KV and object store bucket has, nats.go sends a direct get, and any
// replica of the stream answers one. A replica that is behind answers "no
// message" for a message the leader holds, the copy would count as complete
// without it, and the bucket would be deleted. The request is therefore sent
// to the message get API itself, with the body Stream.GetMsg sends.
//
// It returns nats.ErrNoResponders while the stream has no leader.
func HoldsMsgAfter(ctx context.Context, js jetstream.JetStream, streamName string, seq uint64) (bool, error) {
	nc := js.Conn()
	if nc == nil {
		return false, errors.New("jsrecreate: the JetStream client has no connection")
	}
	req, err := json.Marshal(MsgGetRequest{Seq: seq + 1, NextFor: ">"})
	if err != nil {
		return false, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, MsgGetTimeout)
		defer cancel()
	}
	reply, err := nc.RequestWithContext(ctx, APIPrefix(js.Options())+"STREAM.MSG.GET."+streamName, req)
	if err != nil {
		return false, err
	}
	var resp MsgGetResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return false, fmt.Errorf("unreadable answer of the message get API: %w", err)
	}
	switch {
	case resp.Error != nil && resp.Error.ErrorCode == jetstream.JSErrCodeMessageNotFound:
		return false, nil
	case resp.Error != nil:
		return false, resp.Error
	case len(resp.Message) == 0:
		return false, errors.New("the message get API answered without a message and without an error")
	}
	return true, nil
}

// MsgGetTimeout bounds the message get request of a caller whose context has
// no deadline (nats.go requires one).
const MsgGetTimeout = 5 * time.Second

// MsgGetRequest is the body of a JetStream message get request: the first
// message at or after Seq whose subject matches NextFor.
type MsgGetRequest struct {
	Seq     uint64 `json:"seq,omitempty"`
	NextFor string `json:"next_by_subj,omitempty"`
}

// MsgGetResponse is what HoldsMsgAfter reads of the answer.
type MsgGetResponse struct {
	Error   *jetstream.APIError `json:"error,omitempty"`
	Message json.RawMessage     `json:"message,omitempty"`
}

// APIPrefix returns the subject prefix of the JetStream API a client with
// opts sends its requests to, with its trailing dot: the client's own prefix,
// that of its domain, or the default. nats.go keeps the prefix it computed
// private, so this repeats its rule.
func APIPrefix(opts jetstream.JetStreamOptions) string {
	switch {
	case opts.APIPrefix != "" && strings.HasSuffix(opts.APIPrefix, "."):
		return opts.APIPrefix
	case opts.APIPrefix != "":
		return opts.APIPrefix + "."
	case opts.Domain != "":
		return "$JS." + opts.Domain + ".API."
	default:
		return jetstream.DefaultAPIPrefix
	}
}

// SourceSequence parses the source sequence of a Nats-Stream-Source header.
func SourceSequence(header string) (uint64, bool) {
	fields := strings.Fields(header)
	if len(fields) < 2 {
		return 0, false
	}
	seq, err := strconv.ParseUint(fields[1], 10, 64)
	return seq, err == nil
}

// SourceStream returns the stream a message was copied from, the first field
// of its Nats-Stream-Source header, or "" when the header is empty.
func SourceStream(header string) string {
	fields := strings.Fields(header)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// SourceNames lists the streams the given sources copy, for an error message.
func SourceNames(sources []*jetstream.StreamSourceInfo) string {
	names := make([]string, 0, len(sources))
	for _, src := range sources {
		if src != nil {
			names = append(names, src.Name)
		}
	}
	return strings.Join(names, ", ")
}

// bucketRepaired reports whether the bucket's stream, read again, is gone,
// was replaced since info was read, or already has storage want: another
// instance starting at the same time has repaired it. A failed read is not a
// repair.
func bucketRepaired(ctx context.Context, streams StreamStore, streamName string, info *jetstream.StreamInfo, want jetstream.StorageType) bool {
	cur, err := streams.StreamInfo(ctx, streamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return true
	}
	if err != nil || cur == nil {
		return false
	}
	return !cur.Created.Equal(info.Created) || cur.Config.Storage == want
}

// deleteBackup removes a partial or redundant backup, best effort: a
// leftover only costs storage, so a failure is logged.
func deleteBackup[T any](ctx context.Context, s Spec[T], backup string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := s.DeleteBackup(ctx, backup); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		s.Logger.Warn("Failed to delete incomplete "+s.Kind+" backup", "backup_bucket", backup, "error", err)
	}
}

// BackupName returns <bucket>-backup-<UTC time>-<16 hex digits>; the random
// part keeps the backups of instances that start in the same second apart.
func BackupName(bucket string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to name backup bucket: %w", err)
	}
	return fmt.Sprintf("%s-backup-%s-%s", bucket, time.Now().UTC().Format(backupTimeLayout), hex.EncodeToString(b[:])), nil
}

// isPlainStream reports whether info describes a stream created for the
// bucket itself: listening on exactly subjects (in any order), and neither a
// mirror nor sourced (whose contents belong to another bucket). Only such a
// stream is ever recreated.
func isPlainStream(info *jetstream.StreamInfo, subjects []string) bool {
	if info.Config.Mirror != nil || len(info.Config.Sources) > 0 {
		return false
	}
	have := info.Config.Subjects
	if len(have) != len(subjects) {
		return false
	}
	want := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		want[s] = true
	}
	for _, s := range have {
		if !want[s] {
			return false
		}
		delete(want, s)
	}
	return len(want) == 0
}

// ValidateOptions checks the recreate settings of bucket name, whose stream
// name is streamPrefix+name. minTTL is the smallest positive BackupTTL.
func ValidateOptions(name, streamPrefix string, o Options, minTTL time.Duration) error {
	if !o.Recreate {
		if o.SkipBackup || o.BackupTTL != 0 {
			return fmt.Errorf("bucket %q: SkipBackupOnRecreate and RecreateBackupTTL require RecreateOnStorageMismatch", name)
		}
		return nil
	}
	if o.SkipBackup && o.BackupTTL != 0 {
		return fmt.Errorf("bucket %q: RecreateBackupTTL has no effect with SkipBackupOnRecreate", name)
	}
	if o.BackupTTL > 0 && o.BackupTTL < minTTL {
		return fmt.Errorf("bucket %q: RecreateBackupTTL must be at least %v (or negative to keep the backup)", name, minTTL)
	}
	if !o.SkipBackup && len(streamPrefix)+len(name)+BackupNameSuffixLen > MaxStreamNameLen {
		return fmt.Errorf("bucket %q: name too long for a backup bucket (at most %d characters with RecreateOnStorageMismatch; or set SkipBackupOnRecreate)",
			name, MaxStreamNameLen-len(streamPrefix)-BackupNameSuffixLen)
	}
	return nil
}
