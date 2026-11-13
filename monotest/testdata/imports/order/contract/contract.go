// Package contract is the published contract of the order module. Importing
// another module's contract package is allowed.
package contract

import inventorycontract "example.test/shop/inventory/contract"

// PlaceOrder is an order request.
type PlaceOrder struct {
	Stock inventorycontract.CheckStockRequest
}
