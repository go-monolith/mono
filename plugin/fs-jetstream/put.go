package fsjetstream

import (
	"context"
	"io"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// ContextReader reads from Reader until Ctx ends, and then returns the error
// of Ctx. It is how an upload learns that its caller gave up (see PutObject).
type ContextReader struct {
	Ctx    context.Context //nolint:containedctx // the reader is the carrier of the caller's cancellation
	Reader io.Reader
}

// Read implements io.Reader.
func (r *ContextReader) Read(p []byte) (int, error) {
	if err := r.Ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

// KeyedMutex is a set of mutexes, one per key, that exist only while they are
// held or waited for.
type KeyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

// keyedLock is the mutex of one key: a buffered channel that holds a token
// while the key is locked, and the number of holders and waiters.
type keyedLock struct {
	held chan struct{}
	refs int
}

// Lock locks key and returns the function that unlocks it. It returns the
// error of ctx, and no unlock function, when ctx ends first.
func (k *KeyedMutex) Lock(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLock)
	}
	lock, ok := k.locks[key]
	if !ok {
		lock = &keyedLock{held: make(chan struct{}, 1)}
		k.locks[key] = lock
	}
	lock.refs++
	k.mu.Unlock()

	release := func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		lock.refs--
		if lock.refs == 0 {
			delete(k.locks, key)
		}
	}

	select {
	case lock.held <- struct{}{}:
		return func() {
			<-lock.held
			release()
		}, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}

// PutObject stores an object from reader, leaving no chunks behind when the
// upload is given up.
//
// An object is stored as chunk messages followed by a metadata message, and
// chunks no metadata refers to are invisible (List, Delete and Reset do not
// see them) while they count towards the bucket's limits. The object store
// client removes the chunks of a failed upload, but with the context of the
// upload: when that context was cancelled or timed out, which is why the
// upload failed, the removal fails too and the chunks stay for good. A bucket
// with MaxBytes then fills up with them until every Put is rejected.
//
// The store is therefore called with a context the caller's cancellation does
// not reach, and learns of it through the reader instead (ContextReader): a
// failed read is cleaned up with a context that still works. Each request of
// the store is then bounded by the JetStream default timeout instead of the
// caller's deadline, so a Put can return a few seconds after its deadline. An
// upload whose data was read completely is finished even when ctx ends
// meanwhile.
//
// Uploads of one name are serialised within this backend, with each other
// and with deletes of that name (DeleteObject). The store removes the chunks
// of the version it found when the upload began, so of several concurrent
// uploads of one name only one is referred to and the others' chunks stay.
// Uploads of one name from several processes at once are not covered: they
// cannot see each other.
//
// A reader that blocks cannot be interrupted: the upload, and with it every
// later upload or delete of that name, waits until the Read returns. The
// callers that wait give up with their own context; the variants without one
// (Set, PutReader, Delete, Reset) wait for as long as it takes.
//
// A nil reader stores an empty object, as the store does.
//
// Chunks left behind before this (or by a process that died during an upload)
// are not removed: they cannot be told from the chunks of an upload another
// process is making.
func (b *JetStreamBackend) PutObject(ctx context.Context, meta jetstream.ObjectMeta, reader io.Reader) (*jetstream.ObjectInfo, error) {
	unlock, err := b.puts.Lock(ctx, meta.Name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader == nil {
		return b.store.Put(context.WithoutCancel(ctx), meta, nil)
	}
	return b.store.Put(context.WithoutCancel(ctx), meta, &ContextReader{Ctx: ctx, Reader: reader})
}

// DeleteObject deletes the object name. It returns
// jetstream.ErrObjectNotFound when there is none.
//
// It waits for an upload of that name in this backend: a delete in between
// would mark the version it found as deleted over the metadata of the upload,
// whose chunks nothing would refer to any more. The store marks the object
// deleted and then removes its chunks, in two requests; like PutObject it is
// called with a context the caller's cancellation does not reach, so a delete
// that has begun removes the chunks too. A context that has already ended
// deletes nothing.
func (b *JetStreamBackend) DeleteObject(ctx context.Context, name string) error {
	unlock, err := b.puts.Lock(ctx, name)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.store.Delete(context.WithoutCancel(ctx), name)
}
