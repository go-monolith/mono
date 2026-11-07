package container

import (
	"context"
	"time"

	"github.com/go-monolith/mono/internal/boundary"
	"github.com/go-monolith/mono/pkg/types"
)

// dependencyView is the ServiceContainer a consumer module receives for one of
// its dependencies in SetDependencyServiceContainer. It wraps the dependency's
// real container so that:
//
//   - every successful lookup (Get*) is attributed to the consumer through the
//     boundary recorder, which is what MonoFramework.ModuleBoundaries reports;
//   - a GetChannelService call that passes a consumerModule other than the
//     consumer's own name is a channel-consumer-mismatch violation (the
//     consumer channel would be registered under a name targeted replies do
//     not use);
//   - mutating methods (Register*, Unregister, BindModule, Set*,
//     StartChannelRouters, RegisterContract) are register-on-dependency
//     violations: a consumer may only look its dependency's services up.
//
// In strict mode a violation fails with ErrModuleBoundaryViolation (void
// methods become no-ops, MustGetChannelService panics like the real one does
// on a missing service); without it the call is recorded, warned once, and
// delegated unchanged so that existing applications keep working.
//
// Entries() hands out the dependency's *ServiceEntry values as the real
// container does; the view does not copy them.
type dependencyView struct {
	inner      types.ServiceContainer
	dependency string
	rec        *boundary.Recorder
}

// NewDependencyView wraps dependency's container for the consumer module whose
// recorder is rec. The returned container implements types.ContractRegistrar
// (as a violation) so that type assertions on it behave like on the real one.
func NewDependencyView(inner types.ServiceContainer, dependency string, rec *boundary.Recorder) types.ServiceContainer {
	return &dependencyView{inner: inner, dependency: dependency, rec: rec}
}

var (
	_ types.ServiceContainer  = (*dependencyView)(nil)
	_ types.ContractRegistrar = (*dependencyView)(nil)
)

// violation records (or, in strict mode, rejects) a mutating call.
func (v *dependencyView) violation() error {
	return v.rec.Violation(types.BoundaryViolationRegisterOnDependency, v.dependency, "")
}

// --- mutating methods: boundary violations -------------------------------

func (v *dependencyView) BindModule(module types.Module) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.BindModule(module)
}

func (v *dependencyView) SetEventBus(bus types.EventBus) {
	if v.violation() != nil {
		return
	}
	v.inner.SetEventBus(bus)
}

func (v *dependencyView) SetQueueGroupOptimisticWindow(window time.Duration) {
	if v.violation() != nil {
		return
	}
	v.inner.SetQueueGroupOptimisticWindow(window)
}

func (v *dependencyView) SetMiddlewareChain(chain types.MiddlewareChainRunner) {
	if v.violation() != nil {
		return
	}
	v.inner.SetMiddlewareChain(chain)
}

func (v *dependencyView) StartChannelRouters(ctx context.Context) {
	if v.violation() != nil {
		return
	}
	v.inner.StartChannelRouters(ctx)
}

func (v *dependencyView) RegisterChannelService(name string, in chan *types.Msg, out chan *types.Msg) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.RegisterChannelService(name, in, out)
}

func (v *dependencyView) RegisterRequestReplyService(name string, handler types.RequestReplyHandler, opts ...types.ServiceOption) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.RegisterRequestReplyService(name, handler, opts...)
}

func (v *dependencyView) RegisterQueueGroupService(name string, pairs ...types.QGHP) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.RegisterQueueGroupService(name, pairs...)
}

func (v *dependencyView) RegisterStreamConsumerService(name string, config types.StreamConsumerConfig, handler types.StreamConsumerHandler) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.RegisterStreamConsumerService(name, config, handler)
}

func (v *dependencyView) RegisterCronService(name string, config types.CronServiceConfig, handler types.CronHandler) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.RegisterCronService(name, config, handler)
}

func (v *dependencyView) Unregister(name string) error {
	if err := v.violation(); err != nil {
		return err
	}
	return v.inner.Unregister(name)
}

func (v *dependencyView) RegisterContract(def types.BaseServiceDefinition) error {
	if err := v.violation(); err != nil {
		return err
	}
	if registrar, ok := v.inner.(types.ContractRegistrar); ok {
		return registrar.RegisterContract(def)
	}
	return nil
}

// --- lookups: attributed to the consumer ---------------------------------

func (v *dependencyView) GetChannelService(serviceName string, consumerModule string) (chan *types.Msg, chan *types.Msg, error) {
	if consumerModule != v.rec.Module() {
		// The offending name travels in the Subject field of the violation.
		if err := v.rec.Violation(types.BoundaryViolationChannelConsumerMismatch, v.dependency, consumerModule); err != nil {
			return nil, nil, err
		}
	}
	in, out, err := v.inner.GetChannelService(serviceName, consumerModule)
	if err == nil {
		v.rec.Lookup(v.dependency, serviceName, types.ServiceTypeChannel)
	}
	return in, out, err
}

// MustGetChannelService panics on a strict-mode violation, as the real
// container panics on a missing service: a Must method has no error to return,
// and nil channels would block the caller forever.
func (v *dependencyView) MustGetChannelService(serviceName string, consumerModule string) (chan *types.Msg, chan *types.Msg) {
	if consumerModule != v.rec.Module() {
		if err := v.rec.Violation(types.BoundaryViolationChannelConsumerMismatch, v.dependency, consumerModule); err != nil {
			panic(err)
		}
	}
	in, out := v.inner.MustGetChannelService(serviceName, consumerModule)
	v.rec.Lookup(v.dependency, serviceName, types.ServiceTypeChannel)
	return in, out
}

func (v *dependencyView) GetRequestReplyService(name string) (types.RequestReplyServiceClient, error) {
	client, err := v.inner.GetRequestReplyService(name)
	if err == nil {
		v.rec.Lookup(v.dependency, name, types.ServiceTypeRequestReply)
	}
	return client, err
}

func (v *dependencyView) GetQueueGroupService(name string) (types.QueueGroupServiceClient, error) {
	client, err := v.inner.GetQueueGroupService(name)
	if err == nil {
		v.rec.Lookup(v.dependency, name, types.ServiceTypeQueueGroup)
	}
	return client, err
}

func (v *dependencyView) GetStreamConsumerService(name string) (types.StreamConsumerServiceClient, error) {
	client, err := v.inner.GetStreamConsumerService(name)
	if err == nil {
		v.rec.Lookup(v.dependency, name, types.ServiceTypeStreamConsumer)
	}
	return client, err
}

// --- read-only introspection: delegated ----------------------------------

func (v *dependencyView) Has(name string) bool { return v.inner.Has(name) }

func (v *dependencyView) Entries() []*types.ServiceEntry { return v.inner.Entries() }
