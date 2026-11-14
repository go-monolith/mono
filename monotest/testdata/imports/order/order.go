// Package order is the order module. It breaks the import rule: it imports the
// inventory module package and one of its internal packages instead of the
// inventory contract.
package order

import (
	"example.test/shop/inventory"
	inventorycontract "example.test/shop/inventory/contract"
	"example.test/shop/inventory/store"
	"example.test/shop/order/contract"
)

// Place places an order by calling the inventory module directly.
func Place(req contract.PlaceOrder) int {
	return inventory.Check(inventorycontract.CheckStockRequest{ProductID: req.Stock.ProductID}) + store.Stock("x")
}
