package monotest_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/go-monolith/mono/monotest"
)

// shopDir is a small application (its own Go module, standard library only)
// whose packages break the import rules in known places.
const (
	shopDir = "testdata/imports"
	shop    = "example.test/shop"
)

// useShop prepares a test that lists the packages of the fixture application.
// The fixture is its own Go module, so the listing must not be redirected by
// a go.work file of a parent directory or by flags from the environment, and
// it is absent when this package is tested from the module cache (a module
// zip leaves nested modules out).
func useShop(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(shopDir + "/go.mod"); err != nil {
		t.Skipf("fixture application not available: %v", err)
	}
	t.Setenv("GOWORK", "off")
	t.Setenv("GOFLAGS", "")
}

// summarize renders violations as "<rule> <package> -> <import>" lines.
func summarize(violations []monotest.ImportViolation) []string {
	lines := make([]string, 0, len(violations))
	for _, v := range violations {
		lines = append(lines, string(v.Rule)+" "+strings.TrimPrefix(v.Package, shop+"/")+" -> "+strings.TrimPrefix(v.Import, shop+"/"))
	}
	return lines
}

func equalLines(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestImportViolations(t *testing.T) {
	useShop(t)
	tests := []struct {
		name string
		opts []monotest.ImportOption
		want []string
	}{
		{
			name: "defaults",
			want: []string{
				"module-import order -> inventory",
				"module-import order -> inventory/store",
				"contract-import payment/contract -> audit",
				"contract-import payment/contract -> inventory/store",
				"module-import reports -> billing/ledger",
				"module-import wiring -> order",
				"module-import wiring -> payment",
			},
		},
		{
			name: "a composition root named as host is exempt",
			opts: []monotest.ImportOption{monotest.WithHostPackages(shop + "/wiring")},
			want: []string{
				"module-import order -> inventory",
				"module-import order -> inventory/store",
				"contract-import payment/contract -> audit",
				"contract-import payment/contract -> inventory/store",
				"module-import reports -> billing/ledger",
			},
		},
		{
			name: "an allowed contract import is exempt",
			opts: []monotest.ImportOption{monotest.WithHostPackages(shop + "/wiring"), monotest.WithAllowedContractImports(shop + "/audit")},
			want: []string{
				"module-import order -> inventory",
				"module-import order -> inventory/store",
				"contract-import payment/contract -> inventory/store",
				"module-import reports -> billing/ledger",
			},
		},
		{
			name: "one module only: the other modules are still known",
			opts: []monotest.ImportOption{monotest.WithImportPatterns("./order/...")},
			want: []string{
				"module-import order -> inventory",
				"module-import order -> inventory/store",
			},
		},
		{
			// Nothing reports depends on imports billing/contract, so the
			// billing module is known only from the listing of the whole
			// application.
			name: "a module outside the import closure of the patterns is still known",
			opts: []monotest.ImportOption{monotest.WithImportPatterns("./reports/...")},
			want: []string{"module-import reports -> billing/ledger"},
		},
		{
			name: "conforming packages",
			opts: []monotest.ImportOption{monotest.WithImportPatterns("./inventory/...", "./order/contract", "./cmd/...")},
			want: []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]monotest.ImportOption{monotest.WithImportDir(shopDir)}, tc.opts...)
			violations, err := monotest.ImportViolations(context.Background(), opts...)
			if err != nil {
				t.Fatalf("ImportViolations: %v", err)
			}
			if got := summarize(violations); !equalLines(got, tc.want) {
				t.Errorf("violations:\n  got  %q\n  want %q", got, tc.want)
			}
		})
	}
}

func TestImportViolationsDescribeTheFix(t *testing.T) {
	useShop(t)
	violations, err := monotest.ImportViolations(context.Background(), monotest.WithImportDir(shopDir), monotest.WithModuleRoots(shop+"/audit"))
	if err != nil {
		t.Fatalf("ImportViolations: %v", err)
	}
	byImport := map[string]monotest.ImportViolation{}
	for _, v := range violations {
		byImport[strings.TrimPrefix(v.Package, shop+"/")+" -> "+strings.TrimPrefix(v.Import, shop+"/")] = v
	}

	v, ok := byImport["order -> inventory"]
	if !ok || v.Module != shop+"/inventory" || !strings.Contains(v.Reason, "import "+shop+"/inventory/contract") {
		t.Errorf("module-import violation = %+v", v)
	}
	if got := v.String(); !strings.HasPrefix(got, "module-import: "+shop+"/order imports "+shop+"/inventory: ") {
		t.Errorf("String() = %q", got)
	}
	// audit is now a module (without a contract package), so the contract
	// package importing it imports another module's non-contract package.
	v, ok = byImport["payment/contract -> audit"]
	if !ok || v.Rule != monotest.ImportRuleContract || v.Module != shop+"/payment" || !strings.Contains(v.Reason, "non-contract package of another module ("+shop+"/audit)") {
		t.Errorf("contract-import violation = %+v", v)
	}
}

func TestCheckImportsReportsEachViolation(t *testing.T) {
	useShop(t)
	rec := &recordingTB{TB: t}
	monotest.CheckImports(rec, monotest.WithImportDir(shopDir), monotest.WithImportPatterns("./order/..."))
	if len(rec.errors) != 2 || rec.fatal {
		t.Fatalf("expected two errors, got %v (fatal=%v)", rec.errors, rec.fatal)
	}
	if !rec.has("module-import: " + shop + "/order imports " + shop + "/inventory/store") {
		t.Errorf("errors = %v", rec.errors)
	}
}

func TestCheckImportsFailsWhenPackagesCannotBeListed(t *testing.T) {
	useShop(t)
	rec := &recordingTB{TB: t}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(fatalSentinel); !ok {
					panic(r)
				}
			}
		}()
		monotest.CheckImports(rec, monotest.WithImportDir(shopDir), monotest.WithImportPatterns("./no-such-package"))
	}()
	if !rec.fatal || !rec.has("monotest.CheckImports: go list") {
		t.Fatalf("expected a fatal listing failure, got %v", rec.errors)
	}
}

// A check that looked at nothing must fail, not pass.
func TestImportViolationsFailWhenNothingIsChecked(t *testing.T) {
	useShop(t)
	tests := []struct {
		name string
		opts []monotest.ImportOption
	}{
		{
			// Run beside a cmd/<app>/main.go: "./..." matches package main
			// only, which is exempt.
			name: "only package main matched",
			opts: []monotest.ImportOption{monotest.WithImportDir(shopDir + "/cmd/shop")},
		},
		{
			name: "only host packages matched",
			opts: []monotest.ImportOption{monotest.WithImportDir(shopDir), monotest.WithImportPatterns("./wiring/..."), monotest.WithHostPackages(shop + "/wiring")},
		},
		{
			// go list only warns about a wildcard that matches nothing.
			name: "a wildcard pattern that matches no package",
			opts: []monotest.ImportOption{monotest.WithImportDir(shopDir), monotest.WithImportPatterns("./docs/...")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations, err := monotest.ImportViolations(context.Background(), tc.opts...)
			if err == nil || !strings.Contains(err.Error(), "no package was checked") {
				t.Fatalf("expected a \"no package was checked\" error, got violations=%v err=%v", violations, err)
			}
			if !strings.Contains(err.Error(), "monotest.WithImportDir") {
				t.Errorf("the error should say how to fix it: %v", err)
			}
		})
	}
}

// The same check pointed at the root of the application from that directory
// finds the violations.
func TestImportViolationsFromASubdirectoryWithImportDir(t *testing.T) {
	useShop(t)
	t.Chdir(shopDir + "/cmd/shop")
	violations, err := monotest.ImportViolations(context.Background(), monotest.WithImportDir("../.."))
	if err != nil {
		t.Fatalf("ImportViolations: %v", err)
	}
	if len(violations) != 7 {
		t.Errorf("violations = %q", summarize(violations))
	}
}

func TestImportViolationsStopWithTheContext(t *testing.T) {
	useShop(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := monotest.ImportViolations(ctx, monotest.WithImportDir(shopDir))
	if err == nil || !strings.Contains(err.Error(), "stopped before it finished") || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected an error naming the cancelled context, got %v", err)
	}
}
