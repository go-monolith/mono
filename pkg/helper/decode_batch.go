package helper

import (
	stderrors "errors"
	"fmt"

	"github.com/go-monolith/mono/pkg/types"
)

// ErrUndecodableMessage is wrapped by the error a typed batch handler
// (RegisterTypedStreamConsumerService, RegisterTypedEventStreamConsumer, and
// the Register method of a typed stream consumer definition) returns when a
// message of the batch could not be unmarshaled and was left out.
var ErrUndecodableMessage = stderrors.New("failed to unmarshal message")

// DecodeBatch decodes the messages of a batch with decode. It returns the
// payloads and the messages they came from, index-aligned (payloads[i] was
// decoded from decoded[i]), in the order of msgs, and an error wrapping
// ErrUndecodableMessage when at least one message was left out because decode
// failed for it; the error says how many and wraps the first failure.
//
// A message that cannot be decoded must not cost the others of its batch:
// they would be redelivered with it and, with a MaxDeliver, given up on with
// it, so one malformed message published to the subject would make its
// neighbours undeliverable. The message left out is not acknowledged,
// terminated or otherwise touched: the server redelivers it after AckWait,
// until the consumer's MaxDeliver, and it stays in the stream. Terminating it
// would remove it from a work-queue or interest stream, which is not the
// helper's decision; a handler that wants that works on the raw messages.
func DecodeBatch[T any](msgs []*types.Msg, decode func(*types.Msg) (T, error)) ([]T, []*types.Msg, error) {
	payloads := make([]T, 0, len(msgs))
	decoded := make([]*types.Msg, 0, len(msgs))
	var first error
	firstSubject := ""
	for _, msg := range msgs {
		payload, err := decode(msg)
		if err != nil {
			if first == nil {
				first = err
				if msg != nil {
					firstSubject = msg.Subject
				}
			}
			continue
		}
		payloads = append(payloads, payload)
		decoded = append(decoded, msg)
	}
	if first == nil {
		return payloads, decoded, nil
	}
	return payloads, decoded, fmt.Errorf(
		"%w: %d of %d messages in the batch left out and not acknowledged (redelivered after AckWait, until MaxDeliver), first on subject %q: %w",
		ErrUndecodableMessage, len(msgs)-len(decoded), len(msgs), firstSubject, first)
}
