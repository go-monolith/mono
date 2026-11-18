package types_test

import (
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

func TestClientTLSConfig(t *testing.T) {
	var nilCfg *types.ClientTLSConfig
	if nilCfg.Validate() == nil || nilCfg.Clone() != nil {
		t.Error("nil config must be invalid and clone to nil")
	}
	c := &types.ClientTLSConfig{CertFile: "c", KeyFile: "k", ClientCAFile: "ca"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	cp := c.Clone()
	cp.CertFile = "other"
	if c.CertFile != "c" {
		t.Error("Clone shares state")
	}
	if (&types.ClientTLSConfig{CertFile: "c"}).Validate() == nil {
		t.Error("missing key accepted")
	}
}
