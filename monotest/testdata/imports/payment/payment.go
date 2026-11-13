// Package payment is the payment module.
package payment

import "example.test/shop/payment/contract"

// Pay charges for a product.
func Pay(product string) contract.Charge { return contract.NewCharge(product) }
