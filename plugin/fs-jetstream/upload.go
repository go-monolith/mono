package fsjetstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

const (
	// DefaultChunkSize is the size of the chunk messages of an upload, the
	// object store's own default.
	DefaultChunkSize = 128 * 1024

	// UploadAckWindow is the number of chunks of one upload that may await
	// their acknowledgement at a time.
	UploadAckWindow = 32

	// UploadAckTimeout is how long an upload waits for the acknowledgement of
	// one chunk once it has to have it.
	UploadAckTimeout = 10 * time.Second
)

var (
	// ErrUploadAckTimeout is returned by an upload whose chunk was not
	// acknowledged within UploadAckTimeout.
	ErrUploadAckTimeout = errors.New("fs-jetstream: a chunk of the upload was not acknowledged in time")

	// ErrUploadOutOfOrder is returned by an upload whose chunks were not
	// stored in the order they were read.
	ErrUploadOutOfOrder = errors.New("fs-jetstream: the chunks of the upload were stored out of order")
)

// UploadPublishOptions are the options every chunk of an upload is published
// with: no retry (see UploadChunks).
var UploadPublishOptions = []jetstream.PublishOpt{jetstream.WithRetryAttempts(0)}

// ChunkSubject returns the subject of the chunk messages of the object
// version id in bucket.
func ChunkSubject(bucket, id string) string {
	return fmt.Sprintf("$O.%s.C.%s", bucket, id)
}

// MetaSubject returns the subject of the metadata message of the object name
// in bucket.
func MetaSubject(bucket, name string) string {
	return fmt.Sprintf("$O.%s.M.%s", bucket, base64.URLEncoding.EncodeToString([]byte(name)))
}

// UploadObject stores an object from reader: its chunk messages first, and
// its metadata message only after the stream has acknowledged every chunk.
//
// The object store client (jetstream.ObjectStore.Put) publishes the chunks
// and the metadata without waiting, and looks at the outcome afterwards. A
// bucket with MaxBytes that has no room for a chunk rejects it and still
// accepts the small metadata message sent right behind it, which replaces the
// metadata of the object stored under that name: the caller is told that the
// upload failed, and the object it had is gone (its metadata describes chunks
// that were never stored, and its own chunks are referred to by nothing). A
// rejected upload of a new name leaves a listed object without content.
//
// Here the metadata is published once the chunks are stored, so an upload
// that fails for any chunk leaves the object as it was:
//
//   - a failed read, a rejected or unacknowledged chunk: the chunks of this
//     upload are removed and the error is returned;
//   - a metadata message the stream rejects: the same;
//   - a metadata message whose outcome is unknown (a timeout): the object is
//     read back. When it is this upload, the upload succeeded. When it is not,
//     the chunks are left in the bucket, since the message may still arrive
//     and would then refer to them; that is logged.
//
// At most UploadAckWindow chunks await their acknowledgement at a time. ctx
// bounds the requests and must not be the caller's cancellable context (see
// PutObject): a cancelled upload is ended through its reader, so that its
// chunks can still be removed. A nil reader stores an empty object.
func (b *JetStreamBackend) UploadObject(ctx context.Context, meta jetstream.ObjectMeta, reader io.Reader) (*jetstream.ObjectInfo, error) {
	if meta.Name == "" {
		return nil, jetstream.ErrBadObjectMeta
	}
	opts := jetstream.ObjectMetaOptions{ChunkSize: DefaultChunkSize}
	if meta.Opts != nil {
		if meta.Opts.Link != nil {
			return nil, jetstream.ErrLinkNotAllowed
		}
		opts = *meta.Opts
		if opts.ChunkSize == 0 {
			opts.ChunkSize = DefaultChunkSize
		}
	}
	meta.Opts = &opts

	// The version stored under the name now; its chunks are removed at the end.
	existing, err := b.store.GetInfo(ctx, meta.Name, jetstream.GetObjectInfoShowDeleted())
	if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, err
	}

	id := nuid.Next()
	info := &jetstream.ObjectInfo{Bucket: b.bucketName, NUID: id, ObjectMeta: meta}
	chunkSubject := ChunkSubject(b.bucketName, id)
	expectStream := jetstream.WithExpectStream(objStreamPrefix + b.bucketName)
	chunkOpts := append([]jetstream.PublishOpt{expectStream}, UploadPublishOptions...)

	if reader != nil {
		size, chunks, digest, err := b.UploadChunks(reader, chunkSubject, int(opts.ChunkSize), chunkOpts...)
		if err != nil {
			b.RemoveChunks(ctx, chunkSubject, "the upload of "+meta.Name+" failed")
			return nil, err
		}
		info.Size, info.Chunks, info.Digest = size, chunks, digest
	}

	metaMsg := nats.NewMsg(MetaSubject(b.bucketName, meta.Name))
	metaMsg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	if metaMsg.Data, err = json.Marshal(info); err != nil {
		b.RemoveChunks(ctx, chunkSubject, "the metadata of "+meta.Name+" could not be encoded")
		return nil, err
	}
	if _, err := b.js.PublishMsg(ctx, metaMsg, expectStream); err != nil {
		if stored, keep := b.MetadataOutcome(ctx, meta.Name, id, err); !stored {
			if !keep {
				b.RemoveChunks(ctx, chunkSubject, "the metadata of "+meta.Name+" was rejected")
			}
			return nil, err
		}
	}
	info.ModTime = time.Now().UTC()

	if existing != nil && !existing.Deleted && existing.NUID != "" && existing.NUID != id {
		b.RemoveChunks(ctx, ChunkSubject(b.bucketName, existing.NUID), "the object "+meta.Name+" was replaced")
	}
	return info, nil
}

// UploadChunks publishes what reader yields as chunk messages on subject, one
// message per Read, and returns once the stream has acknowledged all of them,
// with the size, the number of chunks and the digest of the content.
//
// A chunk is published once, without the retry a publish makes by default
// when no stream answers: a retried chunk is stored behind the chunks sent
// after it, and an object is read back in the order of the stream, so its
// content would not match its digest. For the same reason an acknowledgement
// whose sequence is not after the one before it fails the upload.
//
// On an error it waits for the chunks already published to be acknowledged or
// rejected (each within UploadAckTimeout, the acknowledgement timeout of the
// upload's publisher), so that removing them afterwards removes them. A chunk
// that is stored later still, after its acknowledgement was given up on, stays
// in the bucket referred to by nothing.
func (b *JetStreamBackend) UploadChunks(reader io.Reader, subject string, chunkSize int, opts ...jetstream.PublishOpt) (uint64, uint32, string, error) {
	var (
		pending []jetstream.PubAckFuture
		size    uint64
		chunks  uint32
		hash    = sha256.New()
		buf     = make([]byte, chunkSize)
	)
	fail := func(err error) (uint64, uint32, string, error) {
		SettleAcks(pending, 2*UploadAckTimeout)
		return 0, 0, "", err
	}
	// awaitOldest takes the oldest pending chunk off before its outcome is
	// looked at: an outcome is delivered once, so fail must not wait for it a
	// second time.
	var lastSequence uint64
	awaitOldest := func() error {
		oldest := pending[0]
		pending = pending[1:]
		ack, err := AwaitAck(oldest, 2*UploadAckTimeout)
		if err != nil {
			return err
		}
		if ack != nil {
			if ack.Sequence <= lastSequence {
				return fmt.Errorf("%w: sequence %d after %d", ErrUploadOutOfOrder, ack.Sequence, lastSequence)
			}
			lastSequence = ack.Sequence
		}
		return nil
	}
	for {
		n, readErr := reader.Read(buf)
		if readErr != nil && readErr != io.EOF {
			return fail(readErr)
		}
		if n > 0 {
			// The publish keeps the message until it is acknowledged, so it
			// gets a copy of what the next Read overwrites.
			msg := nats.NewMsg(subject)
			msg.Data = bytes.Clone(buf[:n])
			hash.Write(msg.Data)
			future, err := b.uploads.PublishMsgAsync(msg, opts...)
			if err != nil {
				return fail(err)
			}
			pending = append(pending, future)
			chunks++
			size += uint64(n)
			if len(pending) >= UploadAckWindow {
				if err := awaitOldest(); err != nil {
					return fail(err)
				}
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	for len(pending) > 0 {
		if err := awaitOldest(); err != nil {
			return fail(err)
		}
	}
	return size, chunks, jetstream.GetObjectDigestValue(hash), nil
}

// AwaitAck waits until the stream acknowledged or rejected a published
// message, for at most timeout, and returns the acknowledgement.
func AwaitAck(future jetstream.PubAckFuture, timeout time.Duration) (*jetstream.PubAck, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ack := <-future.Ok():
		return ack, nil
	case err := <-future.Err():
		if errors.Is(err, jetstream.ErrAsyncPublishTimeout) {
			return nil, fmt.Errorf("%w: %w", ErrUploadAckTimeout, err)
		}
		return nil, err
	case <-timer.C:
		return nil, ErrUploadAckTimeout
	}
}

// SettleAcks waits until every one of futures was acknowledged or rejected,
// for at most timeout in all. It reports no outcome: it is for the upload that
// has already failed and is about to remove its chunks, so that none of them
// is stored after the removal.
func SettleAcks(futures []jetstream.PubAckFuture, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for _, future := range futures {
		select {
		case <-future.Ok():
		case <-future.Err():
		case <-timer.C:
			return
		}
	}
}

// MetadataOutcome decides what became of a metadata message whose publish
// failed with publishErr. It reports whether the stream stored it after all,
// and otherwise whether the chunks of the upload must be kept.
//
// A rejection by the stream (an API error, such as a bucket without room for
// the message) is final, as is a publish no stream answered
// (jetstream.ErrNoStreamResponse, returned after every attempt found nothing
// listening): the message is not stored and the chunks can go. Any other
// error (a timeout, a connection that closed) leaves the outcome open,
// so the object is read back: when it is the upload id, the message was
// stored. When it is not, or cannot be read, the message may still arrive, and
// removing the chunks would leave it describing an object without content;
// they are kept, which is logged.
func (b *JetStreamBackend) MetadataOutcome(ctx context.Context, name, id string, publishErr error) (stored, keepChunks bool) {
	var apiErr *jetstream.APIError
	if errors.As(publishErr, &apiErr) || errors.Is(publishErr, jetstream.ErrNoStreamResponse) {
		return false, false
	}
	current, err := b.store.GetInfo(ctx, name, jetstream.GetObjectInfoShowDeleted())
	if err == nil && current.NUID == id {
		return true, false
	}
	if b.logger != nil {
		b.logger.Warn("the outcome of an object's metadata message is unknown; the chunks of the upload are kept and are not referred to unless the message arrives",
			"bucket", b.bucketName,
			"key", name,
			"chunk_subject", ChunkSubject(b.bucketName, id),
			"error", publishErr,
			"lookup_error", err,
		)
	}
	return false, true
}

// RemoveChunks removes the chunk messages on subject. A failure leaves them in
// the bucket, referred to by nothing and counting towards its limits; it is
// logged with the subject, since nothing else names them.
func (b *JetStreamBackend) RemoveChunks(ctx context.Context, subject, reason string) {
	stream, err := b.BucketStream(ctx)
	if err == nil {
		err = stream.Purge(ctx, jetstream.WithPurgeSubject(subject))
	}
	if err != nil && b.logger != nil {
		b.logger.Warn("failed to remove the chunks of an object version; they stay in the bucket and count towards its limits",
			"bucket", b.bucketName,
			"chunk_subject", subject,
			"reason", reason,
			"error", err,
		)
	}
}

// BucketStream returns the stream of the bucket, looked up once.
func (b *JetStreamBackend) BucketStream(ctx context.Context) (jetstream.Stream, error) {
	b.streamMu.Lock()
	defer b.streamMu.Unlock()
	if b.stream != nil {
		return b.stream, nil
	}
	stream, err := b.js.Stream(ctx, objStreamPrefix+b.bucketName)
	if err != nil {
		return nil, err
	}
	b.stream = stream
	return stream, nil
}
