// Package contract is the port of the inventory module: the request and
// response types of its services and the typed service contracts other modules
// call them through. It is the only package of the inventory module another
// module may import.
//
// A contract package imports only the standard library, pkg/types and
// pkg/helper, never the module package itself or another module's internals.
package contract

import "github.com/go-monolith/mono/pkg/helper"

// CheckStockRequest asks whether Quantity units of ProductID are available.
type CheckStockRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// CheckStockResponse reports availability and the current stock level.
type CheckStockResponse struct {
	Available bool `json:"available"`
	Stock     int  `json:"stock"`
}

// CheckStock is the request-reply service that checks stock availability.
// The inventory module registers its handler through it and consumers call
// through it, so name, subject, kind and serialization are defined once.
var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")
