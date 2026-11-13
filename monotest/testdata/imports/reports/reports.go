// Package reports breaks the import rule: it imports an internal package of
// the billing module, and nothing it depends on imports the billing contract.
package reports

import "example.test/shop/billing/ledger"

// Total reports the ledger total.
func Total() int { return ledger.Total() }
