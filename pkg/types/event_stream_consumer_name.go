package types

import (
	"regexp"
	"strings"
)

// consumerNameSeparators maps the separators that commonly appear in module,
// event and version names to a hyphen. The dot is included because JetStream
// rejects it in consumer names (it is the subject token separator).
var consumerNameSeparators = strings.NewReplacer(" ", "-", ".", "-")

// invalidConsumerNameChars matches every character that SanitizeConsumerName
// strips after separators have been mapped.
var invalidConsumerNameChars = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// SanitizeConsumerName converts name into a valid JetStream consumer name.
//
// Spaces and dots become hyphens and every other character outside
// [a-zA-Z0-9_-] is removed. An input that sanitizes to the empty string yields
// "consumer". The framework applies this to every durable consumer name it
// derives, so the result is always accepted by JetStream.
//
// Example:
//
//	types.SanitizeConsumerName("billing.v2 worker") // "billing-v2-worker"
func SanitizeConsumerName(name string) string {
	name = consumerNameSeparators.Replace(name)
	name = invalidConsumerNameChars.ReplaceAllString(name, "")
	if name == "" {
		return "consumer"
	}
	return name
}

// EventStreamConsumerName returns the durable JetStream consumer name the
// framework uses when consumerModule registers an event stream consumer for
// def, in the form "<consumer-module>-<event-module>-<event>-<version>"
// (sanitized with SanitizeConsumerName).
//
// The name depends only on which module consumes which event. It does not
// depend on module start order or on which other modules are registered, so it
// is stable across restarts, deployments and changes to the module set. When
// the same module registers the same event on the same stream more than once,
// the second and later registrations get a "-2", "-3", ... suffix, counted in
// that module's registration order.
//
// Example:
//
//	name := types.EventStreamConsumerName("analytics", paymentProcessedV1.ToBase())
//	// "analytics-billing-PaymentProcessed-v1"
func EventStreamConsumerName(consumerModule string, def BaseEventDefinition) string {
	return SanitizeConsumerName(consumerModule + "-" + def.ModuleName + "-" + def.Name + "-" + def.Version)
}
