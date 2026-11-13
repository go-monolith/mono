// Package wiring is a composition root that is not package main.
package wiring

import (
	"example.test/shop/order"
	ordercontract "example.test/shop/order/contract"
	"example.test/shop/payment"
)

// Run wires the modules together.
func Run() int {
	return order.Place(ordercontract.PlaceOrder{}) + payment.Pay("x").Stock
}
