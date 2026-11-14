// Package contract is the published contract of the inventory module. It
// follows the import rule: standard library only.
package contract

import "time"

// CheckStockRequest asks whether a product is in stock.
type CheckStockRequest struct {
	ProductID string
	At        time.Time
}
