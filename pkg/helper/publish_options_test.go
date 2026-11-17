package helper_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// recordingStreamClient records which publish method was used and the
// message given to PublishMsg.
func recordingStreamClient() (*mockStreamConsumerServiceClient, *[]string, **types.Msg) {
	var calls []string
	var last *types.Msg
	client := &mockStreamConsumerServiceClient{
		publishFunc: func(_ context.Context, _ []byte) (types.MsgPubAck, error) {
			calls = append(calls, "Publish")
			return &mockMsgPubAck{sequence: 1}, nil
		},
		publishMsgFunc: func(_ context.Context, msg *types.Msg) (types.MsgPubAck, error) {
			calls = append(calls, "PublishMsg")
			last = msg
			return &mockMsgPubAck{sequence: 2}, nil
		},
	}
	return client, &calls, &last
}

func TestPublishStreamConsumerService_WithMsgID(t *testing.T) {
	ctx := context.Background()

	t.Run("without options a plain publish is used", func(t *testing.T) {
		client, calls, _ := recordingStreamClient()
		container := &mockContainerWithClients{streamClient: client}
		if _, err := helper.PublishStreamConsumerService(ctx, container, "svc", json.Marshal, testRequest{Name: "a"}); err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 || (*calls)[0] != "Publish" {
			t.Errorf("calls = %v, want [Publish]", *calls)
		}
	})

	t.Run("the message ID is sent as the Nats-Msg-Id header", func(t *testing.T) {
		client, calls, last := recordingStreamClient()
		container := &mockContainerWithClients{streamClient: client}
		ack, err := helper.PublishStreamConsumerService(ctx, container, "svc", json.Marshal, testRequest{Name: "a"},
			helper.WithMsgID("first"), helper.WithMsgID("order-1")) // last wins
		if err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 || (*calls)[0] != "PublishMsg" || ack.Sequence() != 2 {
			t.Fatalf("calls = %v, ack = %d", *calls, ack.Sequence())
		}
		msg := *last
		if got := msg.Header[helper.MsgIDHeader]; len(got) != 1 || got[0] != "order-1" {
			t.Errorf("header %s = %v, want [order-1]", helper.MsgIDHeader, got)
		}
		if string(msg.Data) != `{"name":"a","value":0}` || msg.Subject != "" {
			t.Errorf("message = %+v; want the marshaled payload and the service subject (empty, filled by the client)", msg)
		}
	})

	t.Run("an empty message ID is rejected before publishing", func(t *testing.T) {
		client, calls, _ := recordingStreamClient()
		container := &mockContainerWithClients{streamClient: client}
		_, err := helper.PublishStreamConsumerService(ctx, container, "svc", json.Marshal, testRequest{}, helper.WithMsgID(""))
		if !errors.Is(err, helper.ErrEmptyMsgID) {
			t.Fatalf("err = %v, want ErrEmptyMsgID", err)
		}
		if len(*calls) != 0 {
			t.Errorf("nothing must be published, got %v", *calls)
		}
	})

	t.Run("a nil option is ignored", func(t *testing.T) {
		client, calls, _ := recordingStreamClient()
		container := &mockContainerWithClients{streamClient: client}
		if _, err := helper.PublishStreamConsumerService(ctx, container, "svc", json.Marshal, testRequest{}, nil); err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 || (*calls)[0] != "Publish" {
			t.Errorf("calls = %v, want [Publish]", *calls)
		}
	})
}

func TestStreamConsumerDefinitionPublish_WithMsgID(t *testing.T) {
	def := helper.StreamConsumerService[cReq]("audit", "trail")
	client, _, last := recordingStreamClient()
	container := &mockContainerWithClients{streamClient: client}
	if _, err := def.Publish(context.Background(), container, cReq{Value: "p"}, helper.WithMsgID("trail-1")); err != nil {
		t.Fatal(err)
	}
	if got := (*last).Header[helper.MsgIDHeader]; len(got) != 1 || got[0] != "trail-1" {
		t.Errorf("header = %v", got)
	}
	_, err := def.Publish(context.Background(), container, cReq{}, helper.WithMsgID(""))
	if !errors.Is(err, helper.ErrEmptyMsgID) {
		t.Errorf("err = %v, want ErrEmptyMsgID wrapped with the contract", err)
	}
}
