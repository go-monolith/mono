package helper

import (
	stderrors "errors"

	"github.com/go-monolith/mono/pkg/types"
)

// MsgIDHeader is the JetStream message ID header (Nats-Msg-Id). A stream
// stores a message ID once within its duplicate window
// (StreamConfig.Duplicates, 2 minutes when zero) and acknowledges a second
// publish with the same ID as a duplicate instead of storing it again.
const MsgIDHeader = "Nats-Msg-Id"

// ErrEmptyMsgID is returned by a publish given WithMsgID with an empty ID.
var ErrEmptyMsgID = stderrors.New("message ID cannot be empty")

// PublishOption configures one publish to a stream consumer service, through
// StreamConsumerDefinition.Publish or PublishStreamConsumerService.
type PublishOption func(*publishOptions)

// publishOptions holds the settings of one publish.
type publishOptions struct {
	msgID    string
	msgIDSet bool
}

// WithMsgID sets the JetStream message ID (the Nats-Msg-Id header) of the
// published message, so that publishing it again with the same ID within the
// stream's duplicate window stores it only once. The acknowledgement of a
// repeated publish reports Duplicate() == true and the sequence of the stored
// message.
//
// Use it to retry a publish safely. A publish that fails, for example with a
// timeout while a cluster has no quorum, may still have been stored: retrying
// it without an ID can store the message twice. Derive the ID from the
// message's identity (an order ID, an idempotency key from the caller), not
// from the attempt, and retry within the duplicate window
// (StreamConfig.Duplicates, 2 minutes by default).
//
// An empty id makes the publish fail with ErrEmptyMsgID.
//
// Example:
//
//	ack, err := contract.OrderPlaced.Publish(ctx, services, event,
//	    helper.WithMsgID("order-"+event.OrderID))
//	if err == nil && ack.Duplicate() {
//	    // already stored by an earlier attempt
//	}
func WithMsgID(id string) PublishOption {
	return func(o *publishOptions) {
		o.msgID = id
		o.msgIDSet = true
	}
}

// buildPublishOptions applies opts in order (last wins) and validates them.
func buildPublishOptions(opts []PublishOption) (publishOptions, error) {
	var o publishOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.msgIDSet && o.msgID == "" {
		return o, ErrEmptyMsgID
	}
	return o, nil
}

// message builds the message to publish when an option needs headers, or
// returns nil when a plain publish of data suffices.
func (o publishOptions) message(data []byte) *types.Msg {
	if !o.msgIDSet {
		return nil
	}
	return &types.Msg{Data: data, Header: types.Header{MsgIDHeader: []string{o.msgID}}}
}
