package helper_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

type cReq struct {
	Value string `json:"value"`
}

type cResp struct {
	Result string `json:"result"`
}

// contractRecordingContainer is a mock container that also implements
// types.ContractRegistrar, to check that Register records the contract.
type contractRecordingContainer struct {
	mockServiceContainer
	recorded    map[string]types.BaseServiceDefinition
	registerErr error
}

func (c *contractRecordingContainer) RegisterContract(def types.BaseServiceDefinition) error {
	if c.registerErr != nil {
		return c.registerErr
	}
	if c.recorded == nil {
		c.recorded = make(map[string]types.BaseServiceDefinition)
	}
	c.recorded[def.Name] = def
	return nil
}

func TestServiceDefinitionConstructors(t *testing.T) {
	rr := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock")
	if rr.ModuleName != "inventory" || rr.Name != "check-stock" || rr.Type != types.ServiceTypeRequestReply {
		t.Errorf("unexpected request-reply identity: %+v", rr.BaseServiceDefinition)
	}
	if rr.Subject() != "services.inventory.check-stock" {
		t.Errorf("Subject = %q", rr.Subject())
	}
	if rr.ToBase() != rr.BaseServiceDefinition {
		t.Error("ToBase must return the embedded identity")
	}

	qg := helper.QueueGroupService[cReq]("notification", "on-order-created")
	if qg.Type != types.ServiceTypeQueueGroup || qg.ToBase().Name != "on-order-created" {
		t.Errorf("unexpected queue group identity: %+v", qg.BaseServiceDefinition)
	}

	sc := helper.StreamConsumerService[cReq]("audit", "trail")
	if sc.Type != types.ServiceTypeStreamConsumer || sc.ToBase().ModuleName != "audit" {
		t.Errorf("unexpected stream consumer identity: %+v", sc.BaseServiceDefinition)
	}
}

func TestServiceDefinitionConstructorsPanicOnInvalidSubject(t *testing.T) {
	cases := []struct{ module, name string }{
		{"Inventory", "check-stock"}, // upper case
		{"inventory", "check_stock"}, // underscore
		{"inventory", ""},            // empty service
		{"", "check-stock"},          // empty module
		{"inventory", "a.b"},         // extra token
	}
	for _, tc := range cases {
		t.Run(tc.module+"/"+tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic")
				}
				if !strings.Contains(fmt.Sprint(r), "invalid service contract") {
					t.Errorf("unexpected panic message: %v", r)
				}
			}()
			helper.RequestReplyService[cReq, cResp](tc.module, tc.name)
		})
	}
}

func TestServiceDefinitionCustomCodecs(t *testing.T) {
	marshalCalls, unmarshalCalls := 0, 0
	m := func(v any) ([]byte, error) { marshalCalls++; return json.Marshal(v) }
	u := func(data []byte, v any) error { unmarshalCalls++; return json.Unmarshal(data, v) }

	def := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock").WithMarshaler(m).WithUnmarshaler(u)
	client := &mockRequestReplyServiceClient{callFunc: func(context.Context, []byte) (*types.Msg, error) {
		return &types.Msg{Data: []byte(`{"result":"ok"}`)}, nil
	}}
	container := &mockContainerWithClients{requestReplyClient: client}

	resp, err := def.Call(context.Background(), container, cReq{Value: "x"})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if resp.Result != "ok" {
		t.Errorf("Result = %q", resp.Result)
	}
	if marshalCalls != 1 || unmarshalCalls != 1 {
		t.Errorf("codecs not used: marshal=%d unmarshal=%d", marshalCalls, unmarshalCalls)
	}
	// The original definition is unchanged (value semantics): calling through
	// it does not touch the custom codecs.
	orig := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock")
	if _, err := orig.Call(context.Background(), container, cReq{Value: "y"}); err != nil {
		t.Fatalf("Call on original failed: %v", err)
	}
	if marshalCalls != 1 || unmarshalCalls != 1 {
		t.Error("WithMarshaler/WithUnmarshaler must return a copy")
	}
}

func TestRequestReplyDefinitionRegister(t *testing.T) {
	def := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock")

	t.Run("registers typed handler and records contract", func(t *testing.T) {
		container := &contractRecordingContainer{}
		handler := func(_ context.Context, req cReq, _ *types.Msg) (cResp, error) {
			return cResp{Result: "got " + req.Value}, nil
		}
		if err := def.Register(container, handler, types.WithMaxConcurrency(3)); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		if len(container.registeredServiceNames) != 1 || container.registeredServiceNames[0] != "check-stock" {
			t.Errorf("registered names = %v", container.registeredServiceNames)
		}
		if container.requestReplyOptions.MaxConcurrency != 3 {
			t.Errorf("options not forwarded: %+v", container.requestReplyOptions)
		}
		if got := container.recorded["check-stock"]; got != def.ToBase() {
			t.Errorf("contract not recorded: %+v", got)
		}
		out, err := container.requestReplyHandler(context.Background(), &types.Msg{Data: []byte(`{"value":"v"}`)})
		if err != nil {
			t.Fatalf("handler failed: %v", err)
		}
		if string(out) != `{"result":"got v"}` {
			t.Errorf("handler output = %s", out)
		}
	})

	t.Run("container without registrar", func(t *testing.T) {
		container := &mockServiceContainer{}
		err := def.Register(container, func(context.Context, cReq, *types.Msg) (cResp, error) {
			return cResp{}, nil
		})
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}
	})

	t.Run("registration error is returned", func(t *testing.T) {
		container := &mockServiceContainer{returnError: errors.New("boom")}
		err := def.Register(container, func(context.Context, cReq, *types.Msg) (cResp, error) {
			return cResp{}, nil
		})
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("expected registration error, got %v", err)
		}
	})

	t.Run("contract mismatch is returned", func(t *testing.T) {
		mismatch := monoerrors.WrapServiceContractMismatch(def.ToBase(), "other", types.ServiceTypeRequestReply)
		container := &contractRecordingContainer{registerErr: mismatch}
		err := def.Register(container, func(context.Context, cReq, *types.Msg) (cResp, error) {
			return cResp{}, nil
		})
		if !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("expected ErrServiceContractMismatch, got %v", err)
		}
	})
}

func TestQueueGroupDefinitionRegisterAndSend(t *testing.T) {
	def := helper.QueueGroupService[cReq]("notification", "on-order-created")

	container := &contractRecordingContainer{}
	var received cReq
	err := def.Register(container, types.TypedQGHP[cReq]{
		QueueGroup: "workers",
		Handler: func(_ context.Context, payload cReq, _ *types.Msg) error {
			received = payload
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if len(container.queueGroupPairs) != 1 || container.queueGroupPairs[0].QueueGroup != "workers" {
		t.Fatalf("pairs = %+v", container.queueGroupPairs)
	}
	if container.recorded["on-order-created"] != def.ToBase() {
		t.Error("contract not recorded")
	}
	if err := container.queueGroupPairs[0].Handler(context.Background(), &types.Msg{Data: []byte(`{"value":"q"}`)}); err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if received.Value != "q" {
		t.Errorf("payload not deserialized: %+v", received)
	}

	var sent []byte
	client := &mockQueueGroupServiceClient{sendFunc: func(_ context.Context, data []byte) error { sent = data; return nil }}
	sender := &mockContainerWithClients{queueGroupClient: client}
	if err := def.Send(context.Background(), sender, cReq{Value: "s"}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if string(sent) != `{"value":"s"}` {
		t.Errorf("sent = %s", sent)
	}
}

func TestStreamConsumerDefinitionRegisterAndPublish(t *testing.T) {
	def := helper.StreamConsumerService[cReq]("audit", "trail")
	config := types.StreamConsumerConfig{}

	container := &contractRecordingContainer{}
	var batch []cReq
	err := def.Register(container, config, func(_ context.Context, payloads []cReq, _ []*types.Msg) error {
		batch = payloads
		return nil
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if container.streamConsumerHandler == nil || container.recorded["trail"] != def.ToBase() {
		t.Fatal("handler or contract not recorded")
	}
	msgs := []*types.Msg{{Data: []byte(`{"value":"1"}`)}, {Data: []byte(`{"value":"2"}`)}}
	if err := container.streamConsumerHandler(context.Background(), msgs); err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if len(batch) != 2 || batch[1].Value != "2" {
		t.Errorf("batch = %+v", batch)
	}

	var published []byte
	client := &mockStreamConsumerServiceClient{publishFunc: func(_ context.Context, data []byte) (types.MsgPubAck, error) {
		published = data
		return &mockMsgPubAck{sequence: 7}, nil
	}}
	publisher := &mockContainerWithClients{streamClient: client}
	ack, err := def.Publish(context.Background(), publisher, cReq{Value: "p"})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if ack.Sequence() != 7 || string(published) != `{"value":"p"}` {
		t.Errorf("ack=%v published=%s", ack, published)
	}
}

func TestServiceDefinitionCallErrors(t *testing.T) {
	def := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock")

	t.Run("nil container", func(t *testing.T) {
		if _, err := def.Call(context.Background(), nil, cReq{}); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("service not found is wrapped with the contract", func(t *testing.T) {
		notFound := monoerrors.WrapServiceNotFound("check-stock", "inventory")
		container := &mockContainerWithClients{getServiceError: notFound}
		_, err := def.Call(context.Background(), container, cReq{})
		if !errors.Is(err, monoerrors.ErrServiceNotFound) {
			t.Fatalf("expected ErrServiceNotFound, got %v", err)
		}
		if !strings.Contains(err.Error(), "inventory/check-stock") {
			t.Errorf("error should name the contract: %v", err)
		}
	})

	t.Run("zero value definition", func(t *testing.T) {
		var zero helper.RequestReplyDefinition[cReq, cResp]
		if _, err := zero.Call(context.Background(), &mockContainerWithClients{}, cReq{}); err == nil {
			t.Fatal("expected error for zero-value definition")
		}
		if err := zero.Register(&mockServiceContainer{}, nil); err == nil {
			t.Fatal("expected error for zero-value definition")
		}
		var zeroQG helper.QueueGroupDefinition[cReq]
		if err := zeroQG.Send(context.Background(), &mockContainerWithClients{}, cReq{}); err == nil {
			t.Fatal("expected error for zero-value queue group definition")
		}
		var zeroSC helper.StreamConsumerDefinition[cReq]
		if _, err := zeroSC.Publish(context.Background(), &mockContainerWithClients{}, cReq{}); err == nil {
			t.Fatal("expected error for zero-value stream consumer definition")
		}
	})

	t.Run("nil container for send and publish", func(t *testing.T) {
		if err := helper.QueueGroupService[cReq]("m", "s").Send(context.Background(), nil, cReq{}); err == nil {
			t.Fatal("expected error")
		}
		if _, err := helper.StreamConsumerService[cReq]("m", "s").Publish(context.Background(), nil, cReq{}); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestQueueGroupAndStreamDefinitionRegisterErrors(t *testing.T) {
	qg := helper.QueueGroupService[cReq]("notification", "on-order-created")
	sc := helper.StreamConsumerService[cReq]("audit", "trail")
	qgPair := types.TypedQGHP[cReq]{QueueGroup: "w", Handler: func(context.Context, cReq, *types.Msg) error { return nil }}
	scHandler := func(context.Context, []cReq, []*types.Msg) error { return nil }

	t.Run("container without registrar", func(t *testing.T) {
		if err := qg.Register(&mockServiceContainer{}, qgPair); err != nil {
			t.Fatalf("queue group Register failed: %v", err)
		}
		if err := sc.Register(&mockServiceContainer{}, types.StreamConsumerConfig{}, scHandler); err != nil {
			t.Fatalf("stream consumer Register failed: %v", err)
		}
	})

	t.Run("registration error is returned", func(t *testing.T) {
		boom := errors.New("boom")
		if err := qg.Register(&mockServiceContainer{returnError: boom}, qgPair); !errors.Is(err, boom) {
			t.Fatalf("queue group: expected boom, got %v", err)
		}
		if err := sc.Register(&mockServiceContainer{returnError: boom}, types.StreamConsumerConfig{}, scHandler); !errors.Is(err, boom) {
			t.Fatalf("stream consumer: expected boom, got %v", err)
		}
	})

	t.Run("contract mismatch is returned", func(t *testing.T) {
		mismatch := monoerrors.WrapServiceContractMismatch(qg.ToBase(), "other", types.ServiceTypeQueueGroup)
		if err := qg.Register(&contractRecordingContainer{registerErr: mismatch}, qgPair); !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("queue group: expected ErrServiceContractMismatch, got %v", err)
		}
		if err := sc.Register(&contractRecordingContainer{registerErr: mismatch}, types.StreamConsumerConfig{}, scHandler); !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("stream consumer: expected ErrServiceContractMismatch, got %v", err)
		}
	})
}

func TestServiceDefinitionCallWrapsHandlerError(t *testing.T) {
	def := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock")
	handlerErr := errors.New("unknown product")
	client := &mockRequestReplyServiceClient{callFunc: func(context.Context, []byte) (*types.Msg, error) {
		return nil, handlerErr
	}}
	_, err := def.Call(context.Background(), &mockContainerWithClients{requestReplyClient: client}, cReq{})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected the handler error to be wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), "inventory/check-stock") {
		t.Errorf("error should name the contract: %v", err)
	}
}

func TestServiceDefinitionNilCodecsAreIgnored(t *testing.T) {
	def := helper.RequestReplyService[cReq, cResp]("inventory", "check-stock").WithMarshaler(nil).WithUnmarshaler(nil)
	client := &mockRequestReplyServiceClient{callFunc: func(context.Context, []byte) (*types.Msg, error) {
		return &types.Msg{Data: []byte(`{"result":"ok"}`)}, nil
	}}
	resp, err := def.Call(context.Background(), &mockContainerWithClients{requestReplyClient: client}, cReq{Value: "x"})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if resp.Result != "ok" {
		t.Errorf("Result = %q", resp.Result)
	}
}
