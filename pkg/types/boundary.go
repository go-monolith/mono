package types

import "time"

// BoundaryViolationKind names a way in which a module crossed a boundary it did
// not declare. See ModuleBoundaryReport.
type BoundaryViolationKind string

const (
	// BoundaryViolationRegisterOnDependency: the module called a mutating
	// method (Register*, Unregister, BindModule, Set*, StartChannelRouters,
	// RegisterContract) on a dependency's ServiceContainer. A consumer may
	// only look services up.
	BoundaryViolationRegisterOnDependency BoundaryViolationKind = "register-on-dependency"

	// BoundaryViolationChannelConsumerMismatch: the module asked for a channel
	// service with a consumerModule argument that is not its own name, so the
	// consumer channel is registered under a name that targeted replies
	// (Msg.Reply set to the module name) do not reach. The name passed is
	// reported in BoundaryViolation.Subject.
	BoundaryViolationChannelConsumerMismatch BoundaryViolationKind = "channel-consumer-mismatch"

	// BoundaryViolationUndeclaredServiceSubject: the module published to or
	// requested a services.<module>.<service> subject of a module that is
	// neither itself, one of its declared dependencies nor a plugin,
	// bypassing the port (the dependency container) altogether.
	BoundaryViolationUndeclaredServiceSubject BoundaryViolationKind = "undeclared-service-subject"

	// BoundaryViolationForeignServiceSubscription: the module subscribed to
	// another module's services.<module>.<service> subject, which would let it
	// intercept that module's requests.
	BoundaryViolationForeignServiceSubscription BoundaryViolationKind = "foreign-service-subscription"
)

// ServiceUse is one dependency service a module looked up at runtime, with
// how many times it did so (every helper.Call* / contract Call looks the
// service up once).
type ServiceUse struct {
	// Module is the dependency that provides the service.
	Module string `json:"module"`
	// Service is the service name.
	Service string `json:"service"`
	// Type is the kind of service.
	Type ServiceType `json:"type"`
	// Lookups is the number of successful Get* calls for the service.
	Lookups int64 `json:"lookups"`
}

// BoundaryViolation is one kind of boundary crossing observed for a module,
// aggregated over the run.
type BoundaryViolation struct {
	// Kind classifies the violation.
	Kind BoundaryViolationKind `json:"kind"`
	// Dependency is the module whose boundary was crossed.
	Dependency string `json:"dependency"`
	// Subject is the NATS subject involved when the violation was a bus
	// operation, the consumerModule name passed for a channel-consumer
	// mismatch, and empty for other container operations.
	Subject string `json:"subject"`
	// Count is how many times it happened.
	Count int64 `json:"count"`
	// First is when it was first observed.
	First time.Time `json:"first"`
}

// ModuleBoundaryReport describes how one module used its boundaries during
// the current run: what it declared, what it was observed to use, and where
// it stepped outside. It is returned by MonoFramework.ModuleBoundaries for
// every started module, and the framework logs a summary of it at Stop.
//
// With WithStrictModuleBoundaries the operations listed under Violations fail
// instead of being recorded, so a strict run reports none.
type ModuleBoundaryReport struct {
	// Module is the module the report is about.
	Module string `json:"module"`
	// Declared lists the module's Dependencies(), deduplicated, in order.
	Declared []string `json:"declared"`
	// Consumed lists the contracts the module declared through
	// ServiceConsumerModule.ConsumeServices, if any.
	Consumed []BaseServiceDefinition `json:"consumed,omitempty"`
	// Observed lists the dependency services the module looked up, sorted by
	// module then service.
	Observed []ServiceUse `json:"observed"`
	// UnusedDependencies lists declared dependencies on which the module
	// looked nothing up. A dependency declared only for start ordering is
	// legitimate; this is information, not an error.
	UnusedDependencies []string `json:"unused_dependencies"`
	// Violations lists the boundary crossings observed, sorted by kind,
	// dependency and subject.
	Violations []BoundaryViolation `json:"violations"`
}
