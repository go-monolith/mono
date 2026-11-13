// Command shop is the host program: package main may import module packages.
package main

import (
	"fmt"

	"example.test/shop/inventory"
	inventorycontract "example.test/shop/inventory/contract"
	"example.test/shop/wiring"
)

func main() {
	fmt.Println(inventory.Check(inventorycontract.CheckStockRequest{}), wiring.Run())
}
