package types

// BaseServiceDefinition is the non-generic identity of a service contract: the
// module that provides the service, the service name and its kind. It is the
// service-side counterpart of BaseEventDefinition and is what a module reports
// through ServiceConsumerModule.ConsumeServices, what the framework records on a
// ServiceEntry, and what interfaces use because Go interface methods cannot have
// type parameters.
//
// Typed definitions that carry request and response types, serialization and the
// Call/Register methods live in pkg/helper (helper.RequestReplyDefinition,
// helper.QueueGroupDefinition, helper.StreamConsumerDefinition). A contract is
// meant to be declared once, in the provider module's contract package, and
// imported by every consumer:
//
//	// inventory/contract/contract.go
//	var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")
type BaseServiceDefinition struct {
	// ModuleName is the name of the module that provides the service.
	ModuleName string

	// Name is the service name, unique within the providing module (kebab-case).
	Name string

	// Type is the kind of service the contract describes.
	Type ServiceType
}

// Subject returns the NATS subject of the service, services.<module>.<service>.
func (d BaseServiceDefinition) Subject() string {
	return ServiceSubject(d.ModuleName, d.Name)
}

// String returns a short human-readable identity, "<module>/<service> (<type>)".
func (d BaseServiceDefinition) String() string {
	return d.ModuleName + "/" + d.Name + " (" + FormatServiceType(d.Type) + ")"
}

// ServiceSubject computes the NATS subject of a service from its module and
// service name. The format is services.<module>.<service>; it is the single
// definition of the service subject convention used by the framework.
func ServiceSubject(moduleName, serviceName string) string {
	return "services." + moduleName + "." + serviceName
}

// ContractRegistrar is implemented by service containers that record the
// contract a registered service was declared with. The typed definitions in
// pkg/helper call RegisterContract right after registering the handler; the
// framework's container stores the contract on the ServiceEntry so that
// consumers and tooling can tell contract-backed services from ad hoc ones.
//
// The interface is optional: a container that does not implement it (for
// example a test double) simply records nothing.
type ContractRegistrar interface {
	// RegisterContract attaches def to the already registered service
	// def.Name. It returns ErrContainerNotBound when the container has no
	// module, ErrServiceNotFound when the service is not registered, and
	// ErrServiceContractMismatch when the contract names a different module
	// than the one the container is bound to or a different service type than
	// the one registered.
	RegisterContract(def BaseServiceDefinition) error
}
