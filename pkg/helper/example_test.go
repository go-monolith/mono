package helper_test

import (
	"fmt"

	"github.com/go-monolith/mono/v1/pkg/helper"
)

// OrderCreatedEvent is an example event payload type.
type OrderCreatedEvent struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

// ExampleEventDefinition demonstrates creating a type-safe event definition.
func ExampleEventDefinition() {
	// Create event definition with auto-computed subject
	// Subject will be: events.order.v1.order-created
	orderCreated := helper.EventDefinition[OrderCreatedEvent](
		"order",        // moduleName
		"OrderCreated", // name
		"v1",           // version
	)

	fmt.Println("Event name:", orderCreated.Name)
	fmt.Println("Event subject:", orderCreated.Subject)
	fmt.Println("Event version:", orderCreated.Version)
	// Output:
	// Event name: OrderCreated
	// Event subject: events.order.v1.order-created
	// Event version: v1
}

// ExampleEventDefinition_customSubject demonstrates creating an event with a custom subject.
func ExampleEventDefinition_customSubject() {
	// Create event definition with explicit custom subject
	orderCreated := helper.EventDefinition[OrderCreatedEvent](
		"order",
		"OrderCreated",
		"v1",
		"events.orders.v1.created", // custom subject
	)

	fmt.Println("Event subject:", orderCreated.Subject)
	// Output: Event subject: events.orders.v1.created
}

// UserRegisteredEvent is an example event payload type.
type UserRegisteredEvent struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

// ExampleEventDefinition_multipleEvents demonstrates creating multiple event definitions.
func ExampleEventDefinition_multipleEvents() {
	// Define multiple events for a module
	userRegistered := helper.EventDefinition[UserRegisteredEvent](
		"user", "UserRegistered", "v1",
	)

	orderCreated := helper.EventDefinition[OrderCreatedEvent](
		"order", "OrderCreated", "v1",
	)

	fmt.Println("User event:", userRegistered.Subject)
	fmt.Println("Order event:", orderCreated.Subject)
	// Output:
	// User event: events.user.v1.user-registered
	// Order event: events.order.v1.order-created
}
