// Package contract is the published contract of the payment module. It breaks
// the import rule twice: it imports a non-contract package of another module
// and a package that is neither the standard library nor a contract.
package contract

import (
	"example.test/shop/audit"
	"example.test/shop/inventory/store"
)

// Charge is a payment request.
type Charge struct {
	Stock int
	Note  string
}

// NewCharge builds a charge.
func NewCharge(product string) Charge {
	return Charge{Stock: store.Stock(product), Note: audit.Note()}
}
