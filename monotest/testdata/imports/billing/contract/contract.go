// Package contract is the published contract of the billing module. No
// package of this application imports it, so the billing module is found only
// when every package of the application is listed.
package contract

// Invoice is a billing request.
type Invoice struct {
	Amount int
}
