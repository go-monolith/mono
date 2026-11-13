// Package inventory is the inventory module. Importing its own contract and
// its own sub-packages is allowed.
package inventory

import (
	"example.test/shop/inventory/contract"
	"example.test/shop/inventory/store"
)

// Check answers a stock request.
func Check(req contract.CheckStockRequest) int { return store.Stock(req.ProductID) }
