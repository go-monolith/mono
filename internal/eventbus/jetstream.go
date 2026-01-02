package eventbus

import (
	"context"
	"errors"
	"fmt"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NatsJetStream wraps the new jetstream.JetStream interface from nats.go/jetstream package.
// This provides access to the simplified JetStream API for creating streams, consumers,
// and performing fetch operations.
type NatsJetStream struct {
	js     jetstream.JetStream
	logger types.Logger
}

// NewJetStream creates a new JetStream wrapper from a NATS connection.
func NewJetStream(nc *nats.Conn, logger types.Logger) (*NatsJetStream, error) {
	if nc == nil {
		return nil, fmt.Errorf("NATS connection cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &NatsJetStream{
		js:     js,
		logger: logger,
	}, nil
}

// msgPubAck implements types.MsgPubAck by wrapping jetstream.PubAck.
type msgPubAck struct {
	ack *jetstream.PubAck
}

func (p *msgPubAck) Stream() string {
	if p.ack == nil {
		return ""
	}
	return p.ack.Stream
}

func (p *msgPubAck) Sequence() uint64 {
	if p.ack == nil {
		return 0
	}
	return p.ack.Sequence
}

func (p *msgPubAck) Duplicate() bool {
	if p.ack == nil {
		return false
	}
	return p.ack.Duplicate
}

func (p *msgPubAck) Domain() string {
	if p.ack == nil {
		return ""
	}
	return p.ack.Domain
}

// CreateOrUpdateStream creates or updates a stream with the given configuration.
// This operation is idempotent - if the stream exists with compatible config, it returns it.
func (j *NatsJetStream) CreateOrUpdateStream(ctx context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	// Convert internal config to JetStream config
	jsCfg, err := toJetStreamStreamConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config for stream %s: %w", cfg.Name, err)
	}

	stream, err := j.js.CreateOrUpdateStream(ctx, jsCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create/update stream %s: %w", cfg.Name, err)
	}

	j.logger.Debug("Stream created/updated",
		"stream", cfg.Name,
		"subjects", cfg.Subjects)

	return stream, nil
}

// CreateOrUpdateConsumer creates or updates a durable consumer on a stream.
// This operation is idempotent - if the consumer exists with compatible config, it returns it.
func (j *NatsJetStream) CreateOrUpdateConsumer(ctx context.Context, streamName string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	// First get the stream
	stream, err := j.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %s: %w", streamName, err)
	}

	// Convert internal config to JetStream config
	jsCfg, err := toJetStreamConsumerConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to convert consumer config %s on stream %s: %w", cfg.Name, streamName, err)
	}

	// Create or update the consumer
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jsCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create/update consumer on stream %s: %w", streamName, err)
	}

	j.logger.Debug("Consumer created/updated",
		"stream", streamName,
		"consumer", cfg.Name,
		"filter_subject", cfg.FilterSubject)

	return consumer, nil
}

// Publish publishes a message to JetStream synchronously.
// Returns a PubAck with stream information on success.
func (j *NatsJetStream) Publish(ctx context.Context, subject string, data []byte) (types.MsgPubAck, error) {
	ack, err := j.js.Publish(ctx, subject, data)
	if err != nil {
		// Handle no responders error - indicates no stream is configured for this subject
		// Messages cannot be persisted when this occurs
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, monoerrors.WrapEventStreamNotAvailable(subject, "publish", err)
		}
		return nil, fmt.Errorf("failed to publish to %s: %w", subject, err)
	}

	j.logger.Debug("Published to JetStream",
		"subject", subject,
		"stream", ack.Stream,
		"sequence", ack.Sequence)

	return &msgPubAck{ack: ack}, nil
}

// PublishMsg publishes a complete types.Msg to JetStream synchronously.
// Returns a PubAck with stream information on success.
func (j *NatsJetStream) PublishMsg(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	// Convert types.Msg to nats.Msg for underlying JetStream call
	natsMsg := &nats.Msg{
		Subject: msg.Subject,
		Reply:   msg.Reply,
		Data:    msg.Data,
		Header:  nats.Header(msg.Header),
	}

	ack, err := j.js.PublishMsg(ctx, natsMsg)
	if err != nil {
		// Handle no responders error - indicates no stream is configured for this subject
		// Messages cannot be persisted when this occurs
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, monoerrors.WrapEventStreamNotAvailable(msg.Subject, "publish", err)
		}
		return nil, fmt.Errorf("failed to publish message to %s: %w", msg.Subject, err)
	}

	j.logger.Debug("Published message to JetStream",
		"subject", msg.Subject,
		"stream", ack.Stream,
		"sequence", ack.Sequence)

	return &msgPubAck{ack: ack}, nil
}

// Stream returns a stream handle for advanced operations.
func (j *NatsJetStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := j.js.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %s: %w", name, err)
	}
	return stream, nil
}

// DeleteStream deletes a stream.
func (j *NatsJetStream) DeleteStream(ctx context.Context, name string) error {
	if err := j.js.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", name, err)
	}

	j.logger.Debug("Stream deleted", "stream", name)
	return nil
}

// WrapJetStreamMsg wraps a jetstream.Msg into a types.Msg.
func WrapJetStreamMsg(msg jetstream.Msg) *types.Msg {
	return &types.Msg{
		Subject: msg.Subject(),
		Reply:   "", // JetStream uses acknowledgment instead of reply subjects
		Data:    msg.Data(),
		Header:  types.Header(msg.Headers()),
		// Store the underlying jetstream.Msg for acknowledgment support
		NatsMsg: msg,
	}
}
