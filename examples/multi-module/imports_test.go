package main

import (
	"testing"

	"github.com/go-monolith/mono/monotest"
)

// TestImportRules checks the import rule the Ports and Adapters layout rests
// on, for every package of this example: a contract package imports only the
// standard library, mono's pkg/types and pkg/helper and other contract
// packages, and a module imports nothing of another module but its contract
// package. Only this host program (package main) imports the module packages,
// to construct and register them.
func TestImportRules(t *testing.T) {
	monotest.CheckImports(t)
}
