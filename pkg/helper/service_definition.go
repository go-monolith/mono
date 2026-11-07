package helper

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// This file holds the typed service contracts: the service-side counterpart of
// EventDefinition. A contract is declared once, in the provider module's
// contract package, with the request and response types it carries, and is
// imported by every consumer. The provider registers its handler through the
// contract and the consumer calls through it, so the service name, subject,
// kind and serialization are written in exactly one place:
//
//	// inventory/contract/contract.go — the port, importable by any module
//	package contract
//
//	type CheckStockRequest struct{ ProductID string; Quantity int }
//	type CheckStockResponse struct{ Available bool; Stock int }
//
//	var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")
//
//	// inventory/module.go — the driving adapter
//	func (m *Module) RegisterServices(c mono.ServiceContainer) error {
//	    return contract.CheckStock.Register(c, m.checkStock)
//	}
//
//	// order/adapter.go — the driven adapter
//	resp, err := contract.CheckStock.Call(ctx, m.inventory, contract.CheckStockRequest{ProductID: id, Quantity: 1})
//
// A contract package must import only the standard library, pkg/types and
// pkg/helper: never its own module package and never another module's
// non-contract package. That rule is what keeps consumers decoupled from
// provider internals and what lets two modules share contracts without import
// cycles.

// RequestReplyDefinition is the typed contract of a request-reply service. Req
// is the request type, Resp the response type. Create it with
// RequestReplyService; the zero value is not usable.
type RequestReplyDefinition[Req any, Resp any] struct {
	types.BaseServiceDefinition

	marshal   types.Marshaler
	unmarshal types.Unmarshaler
}

// QueueGroupDefinition is the typed contract of a queue group service. T is the
// payload type. Create it with QueueGroupService; the zero value is not usable.
type QueueGroupDefinition[T any] struct {
	types.BaseServiceDefinition

	marshal   types.Marshaler
	unmarshal types.Unmarshaler
}

// StreamConsumerDefinition is the typed contract of a stream consumer service.
// T is the payload type. Create it with StreamConsumerService; the zero value is
// not usable.
type StreamConsumerDefinition[T any] struct {
	types.BaseServiceDefinition

	marshal   types.Marshaler
	unmarshal types.Unmarshaler
}

// RequestReplyService declares the contract of the request-reply service
// serviceName provided by moduleName, serialized as JSON by default (see
// WithMarshaler and WithUnmarshaler).
//
// Panics if services.<moduleName>.<serviceName> is not a valid service subject
// (both tokens must be kebab-case). This is intentional, and the same as
// EventDefinition: contracts are package-level variables created at
// initialization time, and an invalid one is a programming error.
//
// Example:
//
//	var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")
func RequestReplyService[Req any, Resp any](moduleName, serviceName string) RequestReplyDefinition[Req, Resp] {
	return RequestReplyDefinition[Req, Resp]{
		BaseServiceDefinition: newBaseServiceDefinition(moduleName, serviceName, types.ServiceTypeRequestReply),
		marshal:               json.Marshal,
		unmarshal:             json.Unmarshal,
	}
}

// QueueGroupService declares the contract of the queue group service
// serviceName provided by moduleName, serialized as JSON by default. It panics
// on an invalid subject, like RequestReplyService.
//
// Example:
//
//	var OnOrderCreated = helper.QueueGroupService[OrderCreatedNotification]("notification", "on-order-created")
func QueueGroupService[T any](moduleName, serviceName string) QueueGroupDefinition[T] {
	return QueueGroupDefinition[T]{
		BaseServiceDefinition: newBaseServiceDefinition(moduleName, serviceName, types.ServiceTypeQueueGroup),
		marshal:               json.Marshal,
		unmarshal:             json.Unmarshal,
	}
}

// StreamConsumerService declares the contract of the stream consumer service
// serviceName provided by moduleName, serialized as JSON by default. It panics
// on an invalid subject, like RequestReplyService.
//
// Example:
//
//	var AuditTrail = helper.StreamConsumerService[AuditEntry]("audit", "trail")
func StreamConsumerService[T any](moduleName, serviceName string) StreamConsumerDefinition[T] {
	return StreamConsumerDefinition[T]{
		BaseServiceDefinition: newBaseServiceDefinition(moduleName, serviceName, types.ServiceTypeStreamConsumer),
		marshal:               json.Marshal,
		unmarshal:             json.Unmarshal,
	}
}

// newBaseServiceDefinition builds and validates the identity shared by the
// typed definitions.
func newBaseServiceDefinition(moduleName, serviceName string, serviceType types.ServiceType) types.BaseServiceDefinition {
	def := types.BaseServiceDefinition{ModuleName: moduleName, Name: serviceName, Type: serviceType}
	if err := errors.ValidateServiceSubject(def.Subject()); err != nil {
		panic(fmt.Sprintf("invalid service contract %s/%s: %v", moduleName, serviceName, err))
	}
	return def
}

// ---------------------------------------------------------------------------
// RequestReplyDefinition
// ---------------------------------------------------------------------------

// WithMarshaler returns a copy of the contract that serializes requests (on the
// consumer side) and responses (on the provider side) with m. A nil m keeps
// the current marshaler.
func (d RequestReplyDefinition[Req, Resp]) WithMarshaler(m types.Marshaler) RequestReplyDefinition[Req, Resp] {
	if m != nil {
		d.marshal = m
	}
	return d
}

// WithUnmarshaler returns a copy of the contract that deserializes requests (on
// the provider side) and responses (on the consumer side) with u. A nil u keeps
// the current unmarshaler.
func (d RequestReplyDefinition[Req, Resp]) WithUnmarshaler(u types.Unmarshaler) RequestReplyDefinition[Req, Resp] {
	if u != nil {
		d.unmarshal = u
	}
	return d
}

// ToBase returns the non-generic identity of the contract, for
// ServiceConsumerModule.ConsumeServices.
func (d RequestReplyDefinition[Req, Resp]) ToBase() types.BaseServiceDefinition {
	return d.BaseServiceDefinition
}

// Register registers handler as the provider of this contract on container,
// which must be the provider module's own container (the one passed to
// RegisterServices). The handler receives the request already deserialized and
// its response is serialized with the contract's marshaler. opts are the usual
// per-service options (WithMaxConcurrency, ...).
//
// When the container implements types.ContractRegistrar (the framework's
// container does), the contract is recorded on the service entry; a container
// bound to a different module than the contract names fails with
// ErrServiceContractMismatch.
func (d RequestReplyDefinition[Req, Resp]) Register(container types.ServiceContainer, handler types.TypedRequestReplyHandler[Req, Resp], opts ...types.ServiceOption) error {
	if err := d.check(); err != nil {
		return err
	}
	if err := RegisterTypedRequestReplyService(container, d.Name, d.unmarshal, d.marshal, handler, opts...); err != nil {
		return err
	}
	return registerContract(container, d.BaseServiceDefinition)
}

// Call sends req to the service through container, the provider's container as
// received in SetDependencyServiceContainer, and returns the deserialized
// response. The caller's context deadline is propagated to the handler (see
// "Request Deadlines" in docs/spec/foundation.md). Errors are wrapped with the
// contract identity; ErrServiceNotFound means the provider has not registered
// the service on that container.
func (d RequestReplyDefinition[Req, Resp]) Call(ctx context.Context, container types.ServiceContainer, req Req) (Resp, error) {
	var resp Resp
	if err := d.check(); err != nil {
		return resp, err
	}
	if container == nil {
		return resp, fmt.Errorf("contract %s: container cannot be nil", d.BaseServiceDefinition)
	}
	if err := CallRequestReplyService(ctx, container, d.Name, d.marshal, d.unmarshal, req, &resp); err != nil {
		return resp, fmt.Errorf("contract %s: %w", d.BaseServiceDefinition, err)
	}
	return resp, nil
}

func (d RequestReplyDefinition[Req, Resp]) check() error {
	return checkDefinition(d.BaseServiceDefinition, d.marshal != nil && d.unmarshal != nil)
}

// ---------------------------------------------------------------------------
// QueueGroupDefinition
// ---------------------------------------------------------------------------

// WithMarshaler returns a copy of the contract that serializes payloads with m.
// A nil m keeps the current marshaler.
func (d QueueGroupDefinition[T]) WithMarshaler(m types.Marshaler) QueueGroupDefinition[T] {
	if m != nil {
		d.marshal = m
	}
	return d
}

// WithUnmarshaler returns a copy of the contract that deserializes payloads with u.
// A nil u keeps the current unmarshaler.
func (d QueueGroupDefinition[T]) WithUnmarshaler(u types.Unmarshaler) QueueGroupDefinition[T] {
	if u != nil {
		d.unmarshal = u
	}
	return d
}

// ToBase returns the non-generic identity of the contract.
func (d QueueGroupDefinition[T]) ToBase() types.BaseServiceDefinition {
	return d.BaseServiceDefinition
}

// Register registers the queue group handlers of this contract on the
// provider's container. Each pair receives the payload already deserialized.
// See RequestReplyDefinition.Register for the contract recording rules.
func (d QueueGroupDefinition[T]) Register(container types.ServiceContainer, pairs ...types.TypedQGHP[T]) error {
	if err := d.check(); err != nil {
		return err
	}
	if err := RegisterTypedQueueGroupService(container, d.Name, d.unmarshal, pairs...); err != nil {
		return err
	}
	return registerContract(container, d.BaseServiceDefinition)
}

// Send sends payload to the queue group service through the provider's
// container and waits for the acceptance ACK (or publishes optimistically when
// the queue group optimistic window is enabled). Errors are wrapped with the
// contract identity.
func (d QueueGroupDefinition[T]) Send(ctx context.Context, container types.ServiceContainer, payload T) error {
	if err := d.check(); err != nil {
		return err
	}
	if container == nil {
		return fmt.Errorf("contract %s: container cannot be nil", d.BaseServiceDefinition)
	}
	if err := SendQueueGroupService(ctx, container, d.Name, d.marshal, payload); err != nil {
		return fmt.Errorf("contract %s: %w", d.BaseServiceDefinition, err)
	}
	return nil
}

func (d QueueGroupDefinition[T]) check() error {
	return checkDefinition(d.BaseServiceDefinition, d.marshal != nil && d.unmarshal != nil)
}

// ---------------------------------------------------------------------------
// StreamConsumerDefinition
// ---------------------------------------------------------------------------

// WithMarshaler returns a copy of the contract that serializes payloads with m.
// A nil m keeps the current marshaler.
func (d StreamConsumerDefinition[T]) WithMarshaler(m types.Marshaler) StreamConsumerDefinition[T] {
	if m != nil {
		d.marshal = m
	}
	return d
}

// WithUnmarshaler returns a copy of the contract that deserializes payloads with u.
// A nil u keeps the current unmarshaler.
func (d StreamConsumerDefinition[T]) WithUnmarshaler(u types.Unmarshaler) StreamConsumerDefinition[T] {
	if u != nil {
		d.unmarshal = u
	}
	return d
}

// ToBase returns the non-generic identity of the contract.
func (d StreamConsumerDefinition[T]) ToBase() types.BaseServiceDefinition {
	return d.BaseServiceDefinition
}

// Register registers handler as the durable consumer of this contract on the
// provider's container, with config describing the JetStream stream and
// consumer. The handler receives the batch already deserialized and owns the
// acknowledgement of each message. See RequestReplyDefinition.Register for the
// contract recording rules.
func (d StreamConsumerDefinition[T]) Register(container types.ServiceContainer, config types.StreamConsumerConfig, handler types.TypedStreamConsumerHandler[T]) error {
	if err := d.check(); err != nil {
		return err
	}
	if err := RegisterTypedStreamConsumerService(container, d.Name, config, d.unmarshal, handler); err != nil {
		return err
	}
	return registerContract(container, d.BaseServiceDefinition)
}

// Publish publishes payload to the stream behind the service through the
// provider's container and returns the JetStream acknowledgement. Errors are
// wrapped with the contract identity.
func (d StreamConsumerDefinition[T]) Publish(ctx context.Context, container types.ServiceContainer, payload T) (types.MsgPubAck, error) {
	if err := d.check(); err != nil {
		return nil, err
	}
	if container == nil {
		return nil, fmt.Errorf("contract %s: container cannot be nil", d.BaseServiceDefinition)
	}
	ack, err := PublishStreamConsumerService(ctx, container, d.Name, d.marshal, payload)
	if err != nil {
		return nil, fmt.Errorf("contract %s: %w", d.BaseServiceDefinition, err)
	}
	return ack, nil
}

func (d StreamConsumerDefinition[T]) check() error {
	return checkDefinition(d.BaseServiceDefinition, d.marshal != nil && d.unmarshal != nil)
}

// ---------------------------------------------------------------------------
// shared
// ---------------------------------------------------------------------------

// checkDefinition rejects a zero-value definition (one not created through the
// constructors), which would otherwise fail later with a nil function call or
// an empty subject.
func checkDefinition(def types.BaseServiceDefinition, hasCodecs bool) error {
	if def.ModuleName == "" || def.Name == "" || !hasCodecs {
		return fmt.Errorf("service contract is not initialized: create it with helper.RequestReplyService, helper.QueueGroupService or helper.StreamConsumerService")
	}
	return nil
}

// registerContract records def on container when the container supports it.
func registerContract(container types.ServiceContainer, def types.BaseServiceDefinition) error {
	registrar, ok := container.(types.ContractRegistrar)
	if !ok {
		return nil
	}
	if err := registrar.RegisterContract(def); err != nil {
		return fmt.Errorf("contract %s: %w", def, err)
	}
	return nil
}
