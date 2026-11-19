package kvjetstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// kvStreamPrefix is the name prefix JetStream gives the stream of a KV bucket
// (KV_<bucket>). It is part of the KV wire protocol; nats.go keeps its own
// constant unexported.
const kvStreamPrefix = "KV_"

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
// comparing the storage types; when the lookup fails or the types match, the
// original error is returned unchanged.
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
		if kv, retryErr := m.createOrGetKVStore(ctx, cfg); retryErr == nil {
			return kv, nil
		}
		return nil, createErr
	}
	if err != nil || info == nil {
		return nil, createErr
	}
	have := info.Config.Storage

	if !cfg.RecreateOnStorageMismatch || !isPlainKVStream(info, cfg.Name) {
		if cfg.RecreateOnStorageMismatch {
			return nil, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
				"and stream %s is not a plain KV bucket (a mirror or other subjects), so it is not recreated: %w",
				ErrStorageMismatch, cfg.Name, have, want, streamName, createErr)
		}
		return nil, fmt.Errorf("%w: bucket %q exists with %s storage but is configured with %s storage, "+
			"and NATS cannot change the storage type of an existing stream: delete stream %s "+
			"(nats stream rm %s, which deletes the bucket's keys) or, if its contents can be lost, "+
			"set BucketConfig.RecreateOnStorageMismatch: %w",
			ErrStorageMismatch, cfg.Name, have, want, streamName, streamName, createErr)
	}

	// The message counts are the record of what the delete destroyed:
	// whether a bucket's contents can be lost is the application's call,
	// which the plugin cannot check.
	m.logger.Warn("Recreating KV bucket whose storage type does not match its configuration; its keys are deleted",
		"bucket", cfg.Name,
		"stream", streamName,
		"existing_storage", have.String(),
		"configured_storage", want.String(),
		"messages", info.State.Msgs,
		"bytes", info.State.Bytes,
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
