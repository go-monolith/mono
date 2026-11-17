// Package contract is the port of the greeter module: the DTOs of its services
// and the typed service contracts callers use. It is the only package of the
// greeter module the host program imports.
package contract

import (
	"time"

	"github.com/go-monolith/mono/pkg/helper"
)

// ModuleName is the name the greeter module registers under.
const ModuleName = "greeter"

// HelloRequest asks the greeter to greet someone.
type HelloRequest struct {
	Name string `json:"name"`
}

// HelloResponse is the greeting, with the instance (pod) that produced it.
type HelloResponse struct {
	Message  string `json:"message"`
	Instance string `json:"instance"`
}

// Visit records one greeting. Visits are stored in a replicated JetStream
// stream, so they survive the loss of either instance.
type Visit struct {
	Name      string    `json:"name"`
	GreetedBy string    `json:"greeted_by"`
	At        time.Time `json:"at"`
}

// Hello is the request-reply service that returns a greeting.
var Hello = helper.RequestReplyService[HelloRequest, HelloResponse](ModuleName, "hello")

// Visits is the stream consumer service that receives every recorded visit.
var Visits = helper.StreamConsumerService[Visit](ModuleName, "visits")
