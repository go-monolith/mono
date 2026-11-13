package monotest_test

import (
	"context"
	"os"
	"path/filepath"
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

// writeApplication writes a small application (its own Go module, standard
// library only) to a temporary directory and returns the directory. As for
// the fixture, its listing must not be redirected by the environment.
func writeApplication(t *testing.T, files map[string]string) string {
	t.Helper()
	t.Setenv("GOWORK", "off")
	t.Setenv("GOFLAGS", "")
	dir := t.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// A host program that has helper packages of its own: "./..." beside its
// main.go matches a package to check, but none of a module, so the violation
// between the modules would go unreported.
func TestImportViolationsFailBesideTheModules(t *testing.T) {
	const tiny = "example.test/tiny"
	dir := writeApplication(t, map[string]string{
		"go.mod":                             "module " + tiny + "\n\ngo 1.25\n",
		"order/order.go":                     "package order\n\nimport _ \"" + tiny + "/payment/store\"\n",
		"order/contract/contract.go":         "package contract\n",
		"payment/contract/contract.go":       "package contract\n",
		"payment/store/store.go":             "package store\n",
		"cmd/tiny/main.go":                   "package main\n\nimport _ \"" + tiny + "/cmd/tiny/internal/config\"\n\nfunc main() {}\n",
		"cmd/tiny/internal/config/config.go": "package config\n",
	})
	beside := filepath.Join(dir, "cmd", "tiny")

	violations, err := monotest.ImportViolations(context.Background(), monotest.WithImportDir(beside))
	if err == nil || !strings.Contains(err.Error(), "no package of a module was checked") {
		t.Fatalf("expected a \"no package of a module was checked\" error, got violations=%v err=%v", violations, err)
	}
	for _, want := range []string{"monotest.WithImportDir", "monotest.WithImportPatterns", tiny + "/order", tiny + "/payment"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should mention %s: %v", want, err)
		}
	}

	// Naming the patterns says that these packages are the ones meant.
	violations, err = monotest.ImportViolations(context.Background(), monotest.WithImportDir(beside), monotest.WithImportPatterns("./..."))
	if err != nil || len(violations) != 0 {
		t.Errorf("with explicit patterns: violations=%v err=%v", violations, err)
	}

	// From the root of the application the violation is found.
	violations, err = monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir))
	if err != nil {
		t.Fatalf("ImportViolations: %v", err)
	}
	if len(violations) != 1 || violations[0].Package != tiny+"/order" || violations[0].Import != tiny+"/payment/store" {
		t.Errorf("violations = %v", violations)
	}
}

// A directory named contract that is not a module's contract package turns
// its parent into a module until it is named with WithNonContractPackages.
func TestImportViolationsWithNonContractPackages(t *testing.T) {
	const firm = "example.test/firm"
	dir := writeApplication(t, map[string]string{
		"go.mod":                              "module " + firm + "\n\ngo 1.25\n",
		"order/order.go":                      "package order\n\nimport (\n\t_ \"" + firm + "/legal/contract\"\n\t_ \"" + firm + "/legal/store\"\n\t_ \"" + firm + "/order/internal/contract\"\n\t_ \"" + firm + "/order/internal/store\"\n)\n",
		"order/contract/contract.go":          "package contract\n",
		"order/internal/contract/contract.go": "package contract\n\nimport _ \"" + firm + "/order/internal/store\"\n",
		"order/internal/store/store.go":       "package store\n",
		"legal/contract/contract.go":          "// Package contract models customer contracts.\npackage contract\n\nimport _ \"" + firm + "/legal/store\"\n",
		"legal/store/store.go":                "package store\n",
	})

	violations, err := monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir))
	if err != nil {
		t.Fatalf("ImportViolations: %v", err)
	}
	var got []string
	for _, v := range violations {
		got = append(got, string(v.Rule)+" "+strings.TrimPrefix(v.Package, firm+"/")+" -> "+strings.TrimPrefix(v.Import, firm+"/"))
	}
	want := []string{
		"contract-import legal/contract -> legal/store",
		"module-import order -> legal/store",
		"module-import order -> order/internal/store",
		"contract-import order/internal/contract -> order/internal/store",
	}
	if !equalLines(got, want) {
		t.Fatalf("without the option:\n  got  %q\n  want %q", got, want)
	}

	violations, err = monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir),
		monotest.WithNonContractPackages(firm+"/legal/contract", firm+"/order/internal/contract/"))
	if err != nil || len(violations) != 0 {
		t.Errorf("with the option: violations=%v err=%v", violations, err)
	}

	// A path that does not name a contract directory would do nothing.
	for _, path := range []string{firm + "/legal", "contract", ""} {
		_, err = monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir), monotest.WithNonContractPackages(path))
		if err == nil || !strings.Contains(err.Error(), "monotest.WithNonContractPackages") {
			t.Errorf("WithNonContractPackages(%q): expected an error naming the option, got %v", path, err)
		}
	}
}

// An application without a module has nothing the rules protect: whatever its
// packages import, the check would pass.
func TestImportViolationsFailWhenTheApplicationHasNoModule(t *testing.T) {
	dir := writeApplication(t, map[string]string{
		"go.mod":            "module example.test/flat\n\ngo 1.25\n",
		"store/store.go":    "package store\n",
		"order/order.go":    "package order\n\nimport _ \"example.test/flat/store\"\n",
		"cmd/flat/main.go":  "package main\n\nimport _ \"example.test/flat/order\"\n\nfunc main() {}\n",
		"contracts/dto.go":  "package contracts\n",
		"docs/contract.txt": "not a package\n",
	})

	violations, err := monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir))
	if err == nil || !strings.Contains(err.Error(), "no module was found") {
		t.Fatalf("expected a \"no module was found\" error, got violations=%v err=%v", violations, err)
	}
	if !strings.Contains(err.Error(), "monotest.WithModuleRoots") {
		t.Errorf("the error should say how to fix it: %v", err)
	}

	// A mistyped module root names no package, so it does not hide the error.
	if _, err := monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir),
		monotest.WithModuleRoots("example.test/flat/stroe")); err == nil || !strings.Contains(err.Error(), "no module was found") {
		t.Errorf("a module root without a package must not count as a module, got err=%v", err)
	}

	// Naming a module without a contract package is enough to check.
	violations, err = monotest.ImportViolations(context.Background(), monotest.WithImportDir(dir),
		monotest.WithModuleRoots("example.test/flat/store"))
	if err != nil {
		t.Fatalf("ImportViolations with a named module: %v", err)
	}
	if len(violations) != 1 || violations[0].Rule != monotest.ImportRuleModule || violations[0].Package != "example.test/flat/order" {
		t.Errorf("violations = %v", violations)
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
