package types

import "testing"

func TestServiceSubject(t *testing.T) {
	if got := ServiceSubject("inventory", "check-stock"); got != "services.inventory.check-stock" {
		t.Fatalf("ServiceSubject = %q", got)
	}
}

func TestBaseServiceDefinition(t *testing.T) {
	def := BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: ServiceTypeRequestReply}
	if got := def.Subject(); got != "services.inventory.check-stock" {
		t.Errorf("Subject = %q", got)
	}
	if got := def.String(); got != "inventory/check-stock (request_reply)" {
		t.Errorf("String = %q", got)
	}
	if def != (BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: ServiceTypeRequestReply}) {
		t.Error("BaseServiceDefinition must stay comparable")
	}
}
