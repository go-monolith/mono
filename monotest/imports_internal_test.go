package monotest

import "testing"

// app builds a main-module package for findImportViolations.
func app(importPath, name string, imports ...string) listedPackage {
	return listedPackage{ImportPath: importPath, Name: name, Imports: imports, Module: &listedModule{Path: "app", Main: true}}
}

func TestFindImportViolationsEdgeCases(t *testing.T) {
	std := listedPackage{ImportPath: "time", Name: "time", Standard: true, DepOnly: true}
	types := listedPackage{ImportPath: monoTypesPackage, Name: "types", DepOnly: true, Module: &listedModule{Path: "github.com/go-monolith/mono"}}
	helper := listedPackage{ImportPath: monoHelperPackage, Name: "helper", DepOnly: true, Module: &listedModule{Path: "github.com/go-monolith/mono"}}
	// A dependency with a directory named contract is not a module of the application.
	thirdParty := listedPackage{ImportPath: "example.com/lib/contract", Name: "contract", DepOnly: true, Module: &listedModule{Path: "example.com/lib"}}
	thirdPartyRoot := listedPackage{ImportPath: "example.com/lib", Name: "lib", DepOnly: true, Module: &listedModule{Path: "example.com/lib"}}

	tests := []struct {
		name string
		pkgs []listedPackage
		cfg  importConfig
		want []string // "<rule> <package> -> <import>"
		// wantChecked is the number of packages a rule was applied to.
		wantChecked int
	}{
		{
			name:        "a contract package may import the standard library, pkg/types, pkg/helper, cgo and other contracts",
			wantChecked: 3,
			pkgs: []listedPackage{
				std, types, helper,
				app("app/order/contract", "contract", "time", "C", monoTypesPackage, monoHelperPackage, "app/stock/contract", "app/order/contract/v2"),
				app("app/order/contract/v2", "v2", "time"),
				app("app/stock/contract", "contract"),
			},
		},
		{
			name:        "a contract package must not import its own module",
			wantChecked: 2,
			pkgs: []listedPackage{
				app("app/order", "order"),
				app("app/order/contract", "contract", "app/order"),
			},
			want: []string{"contract-import app/order/contract -> app/order"},
		},
		{
			name:        "a versioned contract package is a contract package",
			wantChecked: 3,
			pkgs: []listedPackage{
				app("app/order/contract/v2", "v2", "app/order/internal/model"),
				app("app/order/internal/model", "model"),
				app("app/stock", "stock", "app/order/contract/v2"),
			},
			want: []string{"contract-import app/order/contract/v2 -> app/order/internal/model"},
		},
		{
			name:        "a third-party import of a contract package needs to be allowed",
			wantChecked: 1,
			pkgs: []listedPackage{
				thirdParty, thirdPartyRoot,
				app("app/order/contract", "contract", "example.com/lib", "example.com/lib/contract"),
			},
			want: []string{
				"contract-import app/order/contract -> example.com/lib",
				"contract-import app/order/contract -> example.com/lib/contract",
			},
		},
		{
			name:        "allowed contract imports cover the packages below them",
			wantChecked: 1,
			pkgs: []listedPackage{
				thirdParty, thirdPartyRoot,
				app("app/order/contract", "contract", "example.com/lib", "example.com/lib/contract"),
			},
			cfg: importConfig{contractAllowed: []string{"example.com/lib/"}},
		},
		{
			name:        "a dependency's contract directory does not make it a module",
			wantChecked: 1,
			pkgs: []listedPackage{
				thirdParty, thirdPartyRoot,
				app("app/order", "order", "example.com/lib", "example.com/lib/contract"),
			},
		},
		{
			name:        "packages of one module import each other freely; other modules only the contract",
			wantChecked: 5,
			pkgs: []listedPackage{
				app("app/order", "order", "app/order/internal/model", "app/order/contract", "app/stock/contract"),
				app("app/order/internal/model", "model", "app/order"),
				app("app/order/contract", "contract"),
				app("app/stock", "stock", "app/order/internal/model"),
				app("app/stock/contract", "contract"),
			},
			want: []string{"module-import app/stock -> app/order/internal/model"},
		},
		{
			name:        "nested modules: the innermost module owns a package",
			wantChecked: 4,
			pkgs: []listedPackage{
				app("app/sales", "sales", "app/sales/order", "app/sales/order/contract"),
				app("app/sales/contract", "contract"),
				app("app/sales/order", "order", "app/sales", "app/sales/contract"),
				app("app/sales/order/contract", "contract"),
			},
			// sales is outside its sub-module order; order is inside sales.
			want: []string{"module-import app/sales -> app/sales/order"},
		},
		{
			name:        "package main and host packages are exempt, dependencies are not checked",
			wantChecked: 2,
			pkgs: []listedPackage{
				app("app/cmd", "main", "app/order"),
				app("app/boot", "boot", "app/order"),
				app("app/boot/wire", "wire", "app/order"),
				app("app/order", "order"),
				app("app/order/contract", "contract"),
				{ImportPath: "app/legacy", Name: "legacy", DepOnly: true, Imports: []string{"app/order"}, Module: &listedModule{Path: "app", Main: true}},
			},
			cfg: importConfig{hosts: []string{"app/boot"}},
		},
		{
			name:        "a module named with WithModuleRoots needs no contract package",
			wantChecked: 2,
			pkgs: []listedPackage{
				app("app/audit", "audit"),
				app("app/order", "order", "app/audit"),
			},
			cfg:  importConfig{moduleRoots: []string{"app/audit/"}},
			want: []string{"module-import app/order -> app/audit"},
		},
		{
			name:        "a Go module named contract does not make its packages contract packages",
			wantChecked: 2,
			pkgs: []listedPackage{
				thirdPartyRoot,
				{ImportPath: "example.com/acme/contract", Name: "contract", Imports: []string{"example.com/lib"}, Module: &listedModule{Path: "example.com/acme/contract", Main: true}},
				{ImportPath: "example.com/acme/contract/order", Name: "order", Imports: []string{"example.com/lib", "example.com/acme/contract"}, Module: &listedModule{Path: "example.com/acme/contract", Main: true}},
			},
		},
		{
			name:        "a contract directory at the root of the Go module makes the whole Go module one module",
			wantChecked: 3,
			pkgs: []listedPackage{
				thirdPartyRoot,
				app("app/contract", "contract", "example.com/lib", "app/internal/model"),
				app("app/internal/model", "model", "app"),
				app("app", "app", "app/internal/model", "app/contract"),
			},
			want: []string{
				"contract-import app/contract -> app/internal/model",
				"contract-import app/contract -> example.com/lib",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			violations, checked := findImportViolations(tc.pkgs, tc.cfg)
			for _, v := range violations {
				got = append(got, string(v.Rule)+" "+v.Package+" -> "+v.Import)
			}
			if checked != tc.wantChecked {
				t.Errorf("checked = %d, want %d", checked, tc.wantChecked)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("violations:\n  got  %q\n  want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("violations:\n  got  %q\n  want %q", got, tc.want)
				}
			}
		})
	}
}
