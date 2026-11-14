package monotest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The two framework packages a contract package may import.
const (
	monoTypesPackage  = "github.com/go-monolith/mono/pkg/types"
	monoHelperPackage = "github.com/go-monolith/mono/pkg/helper"
)

// contractSegment is the directory name that marks a module's contract
// package: <module>/contract.
const contractSegment = "contract"

// ImportRule names one of the two import rules CheckImports verifies.
type ImportRule string

const (
	// ImportRuleContract: a contract package (<module>/contract and the
	// packages below it) imports only the standard library, mono's pkg/types
	// and pkg/helper, and other contract packages. It never imports its own
	// module package nor another module's non-contract package.
	ImportRuleContract ImportRule = "contract-import"

	// ImportRuleModule: a package outside a module imports nothing of that
	// module but its contract package. Only the host program (package main,
	// and the packages named with WithHostPackages) may import a module
	// package, to construct and register the module.
	ImportRuleModule ImportRule = "module-import"
)

// ImportViolation is one import that breaks the contract import rule.
type ImportViolation struct {
	// Rule is the rule the import breaks.
	Rule ImportRule
	// Package is the import path of the importing package.
	Package string
	// Import is the import path it must not import.
	Import string
	// Module is the import path of the module the rule protects: the module
	// the contract package belongs to (ImportRuleContract), or the module
	// whose internals were imported (ImportRuleModule).
	Module string
	// Reason says what is wrong and what to do instead.
	Reason string
}

// String returns "<rule>: <package> imports <import>: <reason>".
func (v ImportViolation) String() string {
	return fmt.Sprintf("%s: %s imports %s: %s", v.Rule, v.Package, v.Import, v.Reason)
}

// ImportOption configures CheckImports and ImportViolations.
type ImportOption func(*importConfig)

type importConfig struct {
	dir             string
	patterns        []string
	hosts           []string
	moduleRoots     []string
	contractAllowed []string
	nonContracts    []string
}

// defaultImportPattern is what is checked when WithImportPatterns is not used.
const defaultImportPattern = "./..."

// importListTimeout bounds the "go list" runs of one check, on top of the
// caller's context.
const importListTimeout = 2 * time.Minute

// WithImportDir runs the check in dir instead of the current directory (the
// package directory, in a test). The package patterns are resolved from it.
// It should be the root directory of the application: the one whose "./..."
// covers every module.
func WithImportDir(dir string) ImportOption {
	return func(c *importConfig) { c.dir = dir }
}

// WithImportPatterns sets the package patterns to check, as accepted by
// "go list" (default "./..."). Only the matched packages have their imports
// checked; the modules of the whole application are still known, so a check
// limited to one module ("./order/...") reports its imports of any other.
//
// Naming the patterns also says that these packages are the ones meant, so
// the check accepts patterns that match no package of a module (a package of
// reports that reads several modules, say). Without the option that is taken
// for a test in the wrong directory and fails.
func WithImportPatterns(patterns ...string) ImportOption {
	return func(c *importConfig) {
		if len(patterns) > 0 {
			c.patterns = append([]string(nil), patterns...)
		}
	}
}

// WithHostPackages names packages, by import path, that may import module
// packages although they are not package main: a composition root that builds
// the application, or a package of test helpers that wires several modules.
// A path also covers the packages below it.
func WithHostPackages(importPaths ...string) ImportOption {
	return func(c *importConfig) { c.hosts = append(c.hosts, importPaths...) }
}

// WithModuleRoots names, by import path, modules that have no contract
// sub-package (they expose no service and no event), so that importing them
// from another module is reported too. Modules with a contract sub-package are
// found without it.
func WithModuleRoots(importPaths ...string) ImportOption {
	return func(c *importConfig) { c.moduleRoots = append(c.moduleRoots, importPaths...) }
}

// WithNonContractPackages names, by import path, packages that are called
// "contract" but are not the contract package of a module: a domain package
// about customer contracts ("app/legal/contract"), or a helper package inside
// a module ("app/order/internal/contract"). Such a directory then does not
// make its parent a module, and the package (with the packages below it) is
// not held to the contract import rule; when it lies in a module for another
// reason (the parent has a real contract package elsewhere or is named with
// WithModuleRoots, or the directory is below a real contract package), it is
// an ordinary package of that module. Every path must end in "/contract".
func WithNonContractPackages(importPaths ...string) ImportOption {
	return func(c *importConfig) {
		for _, importPath := range importPaths {
			c.nonContracts = append(c.nonContracts, strings.TrimSuffix(importPath, "/"))
		}
	}
}

// WithAllowedContractImports lets contract packages import more than the
// standard library, mono's pkg/types and pkg/helper and other contract
// packages: for example the runtime of a serialization format the DTOs are
// generated for ("google.golang.org/protobuf"), or a shared value type. A path
// also covers the packages below it. A module's own or another module's
// non-contract packages are best left out: allowing them defeats the rule.
func WithAllowedContractImports(importPaths ...string) ImportOption {
	return func(c *importConfig) { c.contractAllowed = append(c.contractAllowed, importPaths...) }
}

// CheckImports verifies the contract import rule of the Ports and Adapters
// layout on the packages matched by "./..." (see WithImportPatterns), and
// reports every import that breaks it as a separate test error:
//
//   - a contract package (<module>/contract) imports only the standard
//     library, mono's pkg/types and pkg/helper, and other contract packages
//     (ImportRuleContract);
//   - a package outside a module imports only that module's contract package
//     (ImportRuleModule). Package main and the packages named with
//     WithHostPackages are exempt: the host program imports module packages to
//     construct and register them.
//
// A module is a package with a contract sub-package (or one named with
// WithModuleRoots), together with every package below it; packages of the same
// module may import each other freely. A directory named contract that is not
// a module's contract package is excluded with WithNonContractPackages.
// Imports of test files are not checked, so an integration test may wire real
// modules together.
//
// The runtime checks (the boundary report, strict mode, CheckModule) see what a
// module does through the container and the bus it is handed; they cannot see
// a module that imports another module's package and calls it directly. This
// check closes that gap. One test for the whole application is enough, in the
// root directory of the application, so that "./..." covers every module:
//
//	func TestImportRules(t *testing.T) {
//	    monotest.CheckImports(t)
//	}
//
// A test in another directory (for example beside a cmd/<app>/main.go) points
// the check at the root with WithImportDir. The check fails, rather than
// passing with nothing checked, when the patterns match no package or only
// exempt ones, when the application has no module at all, and when "./..."
// matches no package of a module (a directory beside the modules instead of
// above them; WithImportPatterns says that such packages are meant).
//
// It runs "go list", so the go command must be on PATH (it is under "go
// test"). Packages are listed for the default build context: files behind
// build tags or for another operating system are not checked. In a Go
// workspace every workspace module counts as part of the application, so a
// run with GOWORK=off can see fewer modules than one with a go.work file.
func CheckImports(tb testing.TB, opts ...ImportOption) {
	tb.Helper()
	violations, err := ImportViolations(tb.Context(), opts...)
	if err != nil {
		tb.Fatalf("monotest.CheckImports: %v", err)
		return
	}
	for _, v := range violations {
		tb.Errorf("%s", v)
	}
}

// ImportViolations is CheckImports without a test: it returns the imports that
// break the contract import rule, sorted by package and import, for use in a
// command or a CI step. ctx bounds the "go list" runs (two minutes at most).
//
// The error reports that the check could not be done, never a violation: an
// option is invalid, the packages could not be listed, ctx ended, no package
// was checked (the patterns matched nothing, or only package main and host
// packages), the application has no module (no contract package and no
// WithModuleRoots), so that no import could break a rule, or the default
// "./..." matched no package of a module.
func ImportViolations(ctx context.Context, opts ...ImportOption) ([]ImportViolation, error) {
	var cfg importConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	for _, importPath := range cfg.nonContracts {
		if path.Base(importPath) != contractSegment || importPath == contractSegment {
			return nil, fmt.Errorf("monotest.WithNonContractPackages: %q does not end in \"/%s\": "+
				"the option names a package called %s that is not the contract package of a module", importPath, contractSegment, contractSegment)
		}
	}
	// Without WithImportPatterns the packages checked were not chosen, so a
	// set that touches no module is a test in the wrong directory.
	defaultPatterns := len(cfg.patterns) == 0
	if defaultPatterns {
		cfg.patterns = []string{defaultImportPattern}
	}
	ctx, cancel := context.WithTimeout(ctx, importListTimeout)
	defer cancel()

	// The matched packages and their dependencies do not show a module nobody
	// in that set imports the contract of, so every package of the
	// application is listed too, for its path only. The two listings do not
	// depend on each other and run side by side.
	var (
		wg         sync.WaitGroup
		siblings   []listedPackage
		siblingErr error
	)
	wg.Go(func() { siblings, siblingErr = listApplicationPackages(ctx, cfg.dir, cfg.patterns) })
	pkgs, err := listPackages(ctx, cfg.dir, cfg.patterns)
	if err != nil {
		// Its error is the one reported: the other listing need not finish.
		cancel()
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	if siblingErr != nil {
		return nil, siblingErr
	}
	found := findImportViolations(append(pkgs, siblings...), cfg)
	dir := cfg.dir
	if dir == "" {
		dir = "the current directory"
	}
	if found.checked == 0 {
		return nil, fmt.Errorf("no package was checked: the patterns %q in %s match no package, or only package main and host packages, which are exempt; "+
			"run the check in the root directory of the application or point it there with monotest.WithImportDir", cfg.patterns, dir)
	}
	if len(found.modules) == 0 {
		// Without a module no import can break a rule: the check would pass
		// whatever the packages import.
		return nil, fmt.Errorf("no module was found: no package of the application around %s has a %s sub-package, so there is nothing the import rules protect; "+
			"run the check in the root directory of the application or point it there with monotest.WithImportDir, "+
			"and name modules that have no %s package with monotest.WithModuleRoots", dir, contractSegment, contractSegment)
	}
	if defaultPatterns && found.inModules == 0 {
		// Beside a cmd/<app>/main.go that has helper packages of its own, for
		// example: they import no module, so the check would pass whatever
		// the modules import.
		return nil, fmt.Errorf("no package of a module was checked: %q in %s matches %d package(s) to check, none of them in a module (%s); "+
			"run the check in the root directory of the application or point it there with monotest.WithImportDir, "+
			"or name the packages with monotest.WithImportPatterns if they are the ones to check",
			defaultImportPattern, dir, found.checked, describeModules(found.modules))
	}
	return found.violations, nil
}

// describeModules names up to three of the modules found, for an error.
func describeModules(modules []string) string {
	const shown = 3
	if len(modules) <= shown {
		return "the modules found: " + strings.Join(modules, ", ")
	}
	return fmt.Sprintf("the modules found: %s and %d more", strings.Join(modules[:shown], ", "), len(modules)-shown)
}

// listedPackage is the part of "go list -json" output the check needs.
type listedPackage struct {
	ImportPath string
	Name       string
	Standard   bool
	DepOnly    bool
	Imports    []string
	Module     *listedModule
}

// listedModule is the part of a Go module description the check needs, from
// "go list -json" (Module field) and "go list -m -json".
type listedModule struct {
	Path string
	Main bool
	Dir  string
}

// listPackages runs "go list -deps" on patterns in dir and returns the matched
// packages together with all their dependencies (marked DepOnly), which is
// what tells a standard library import from any other.
func listPackages(ctx context.Context, dir string, patterns []string) ([]listedPackage, error) {
	args := append([]string{"list", "-deps", "-json=ImportPath,Name,Standard,DepOnly,Imports,Module"}, patterns...)
	return goList[listedPackage](ctx, dir, args...)
}

// listApplicationPackages returns every package of the application that the
// listing of patterns in dir (listPackages) does not already contain: the
// packages of the main Go module dir is in, or of every module of the
// workspace. They carry the import path and the module only, and are marked
// DepOnly: they are there so that every contract package, and with it every
// module, is known, not to have their imports checked.
//
// A Go module whose root is dir is skipped when the patterns include "./...":
// that pattern, resolved there, matches every package of the module, so the
// other listing has them all. This is the documented setup (one test in the
// root directory of the application), which then costs one package listing
// instead of two.
func listApplicationPackages(ctx context.Context, dir string, patterns []string) ([]listedPackage, error) {
	modules, err := goList[listedModule](ctx, dir, "list", "-m", "-json")
	if err != nil {
		return nil, err
	}
	var all []listedPackage
	for _, module := range modules {
		if !module.Main || module.Dir == "" {
			continue
		}
		if patternsCoverModule(dir, patterns, module.Dir) {
			continue
		}
		// -find skips resolving imports and -e tolerates a package that does
		// not build: only the paths are needed here.
		pkgs, err := goList[listedPackage](ctx, module.Dir, "list", "-e", "-find", "-json=ImportPath,Standard,Module", "./...")
		if err != nil {
			return nil, err
		}
		for i := range pkgs {
			pkgs[i].DepOnly = true
		}
		all = append(all, pkgs...)
	}
	return all, nil
}

// patternsCoverModule reports whether listing patterns in dir matches every
// package of the Go module rooted at moduleDir: the patterns include "./..."
// and dir (the current directory when empty) is that root. A directory that
// cannot be resolved is not covered, so the module is listed.
func patternsCoverModule(dir string, patterns []string, moduleDir string) bool {
	if !slices.Contains(patterns, defaultImportPattern) {
		return false
	}
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return false
		}
		dir = wd
	}
	resolvedDir, err := resolveDir(dir)
	if err != nil {
		return false
	}
	resolvedModule, err := resolveDir(moduleDir)
	if err != nil {
		return false
	}
	return resolvedDir == resolvedModule
}

// resolveDir returns dir as an absolute path without symbolic links, so that
// two names of one directory compare equal.
func resolveDir(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolve directory %s: %w", dir, err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("resolve directory %s: %w", dir, err)
	}
	return resolved, nil
}

// goList runs the go command with args in dir and decodes its output, a
// stream of JSON objects, into values of type T.
func goList[T any](ctx context.Context, dir string, args ...string) ([]T, error) {
	cmd := exec.CommandContext(ctx, "go", args...) //nolint:gosec // G204: the go command with package patterns the test author passes, no shell involved
	cmd.Dir = dir
	// Do not wait forever for the output pipes of a go command that was
	// killed because ctx ended.
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		command := "go " + strings.Join(args, " ")
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("%s: stopped before it finished: %w", command, ctxErr)
		}
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = "no output"
		}
		return nil, fmt.Errorf("%s: %w: %s", command, err, detail)
	}

	var values []T
	decoder := json.NewDecoder(&stdout)
	for {
		var value T
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode go %s output: %w", strings.Join(args, " "), err)
		}
		values = append(values, value)
	}
	return values, nil
}

// importFindings is what findImportViolations returns: the violations and
// what the check covered, which tells a clean application from a check that
// looked at nothing.
type importFindings struct {
	// violations are sorted by package and import.
	violations []ImportViolation
	// checked is the number of packages a rule was applied to. Package main
	// and host packages are exempt and not counted.
	checked int
	// inModules is how many of the checked packages belong to a module.
	inModules int
	// modules are the module roots with at least one package of the
	// application in them, sorted, so that a mistyped WithModuleRoots path is
	// not taken for a module.
	modules []string
}

// findImportViolations applies both rules to the listed packages. Module roots
// are taken from every listed package of the application (dependencies
// included) and from WithModuleRoots; imports are checked for the matched
// packages only, those not marked DepOnly.
func findImportViolations(pkgs []listedPackage, cfg importConfig) importFindings {
	var (
		violations []ImportViolation
		found      importFindings
	)
	standard := make(map[string]bool, len(pkgs))
	rootSet := make(map[string]struct{})
	for _, root := range cfg.moduleRoots {
		if root = strings.TrimSuffix(root, "/"); root != "" {
			rootSet[root] = struct{}{}
		}
	}
	for _, pkg := range pkgs {
		if pkg.Standard {
			standard[pkg.ImportPath] = true
		}
		if pkg.Standard || pkg.Module == nil || !pkg.Module.Main {
			continue
		}
		if root, ok := contractRoot(pkg.ImportPath, pkg.Module.Path, cfg.nonContracts); ok {
			rootSet[root] = struct{}{}
		}
	}
	// Longest first, so that the innermost module owning a package is found
	// first when modules are nested.
	roots := make([]string, 0, len(rootSet))
	for root := range rootSet {
		roots = append(roots, root)
	}
	sort.Slice(roots, func(i, j int) bool {
		if len(roots[i]) != len(roots[j]) {
			return len(roots[i]) > len(roots[j])
		}
		return roots[i] < roots[j]
	})

	for _, pkg := range pkgs {
		if pkg.DepOnly || pkg.Standard {
			continue
		}
		owner, owned := owningModule(pkg.ImportPath, roots)
		if owned && isContractPackage(pkg.ImportPath, owner, cfg.nonContracts) {
			found.checked++
			found.inModules++
			violations = append(violations, contractViolations(pkg, owner, roots, standard, cfg)...)
			continue
		}
		if pkg.Name == "main" || matchesAny(pkg.ImportPath, cfg.hosts) {
			continue
		}
		found.checked++
		if owned {
			found.inModules++
		}
		for _, imp := range pkg.Imports {
			target, ok := owningModule(imp, roots)
			if !ok || isContractPackage(imp, target, cfg.nonContracts) || inTree(pkg.ImportPath, target) {
				continue
			}
			violations = append(violations, ImportViolation{
				Rule:    ImportRuleModule,
				Package: pkg.ImportPath,
				Import:  imp,
				Module:  target,
				Reason: fmt.Sprintf("it is not part of the contract of module %s; import %s/%s and call the module through its contracts "+
					"(a composition root that builds the application can be exempted with monotest.WithHostPackages)",
					target, target, contractSegment),
			})
		}
	}

	sort.Slice(violations, func(i, j int) bool {
		a, b := violations[i], violations[j]
		if a.Package != b.Package {
			return a.Package < b.Package
		}
		return a.Import < b.Import
	})
	found.violations = violations
	// A module counts when a package of the application is in it, so that a
	// mistyped WithModuleRoots path is not taken for a module.
	for _, root := range roots {
		for _, pkg := range pkgs {
			if !pkg.Standard && pkg.Module != nil && pkg.Module.Main && inTree(pkg.ImportPath, root) {
				found.modules = append(found.modules, root)
				break
			}
		}
	}
	sort.Strings(found.modules)
	return found
}

// contractViolations checks the imports of one contract package of module
// owner against ImportRuleContract.
func contractViolations(pkg listedPackage, owner string, roots []string, standard map[string]bool, cfg importConfig) []ImportViolation {
	var violations []ImportViolation
	for _, imp := range pkg.Imports {
		// "C" is the cgo pseudo-package, not an import of Go code.
		if imp == "C" || standard[imp] || imp == monoTypesPackage || imp == monoHelperPackage || matchesAny(imp, cfg.contractAllowed) {
			continue
		}
		target, owned := owningModule(imp, roots)
		if owned && isContractPackage(imp, target, cfg.nonContracts) {
			continue
		}
		var reason string
		switch {
		case owned && target == owner:
			reason = fmt.Sprintf("a contract package must not import its own module (%s): move what the contract needs into the contract package", owner)
		case owned:
			reason = fmt.Sprintf("a contract package must not import a non-contract package of another module (%s); import %s/%s instead", target, target, contractSegment)
		default:
			reason = "a contract package imports only the standard library, mono's pkg/types and pkg/helper and other contract packages " +
				"(allow a serialization runtime or a shared value type with monotest.WithAllowedContractImports)"
		}
		violations = append(violations, ImportViolation{Rule: ImportRuleContract, Package: pkg.ImportPath, Import: imp, Module: owner, Reason: reason})
	}
	return violations
}

// contractRoot returns the module a contract package belongs to:
// "app/order" for "app/order/contract" and for "app/order/contract/v2". The
// last "contract" path element that is not one of nonContracts
// (WithNonContractPackages) decides. Only the elements below goModule, the
// path of the Go module the package is in, are looked at, so a Go module that
// happens to be named ".../contract" does not turn every package into a
// contract package; a contract directory at the root of the Go module makes
// the whole Go module one module.
func contractRoot(importPath, goModule string, nonContracts []string) (string, bool) {
	if !inTree(importPath, goModule) || importPath == goModule {
		return "", false
	}
	segments := strings.Split(strings.TrimPrefix(importPath, goModule+"/"), "/")
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i] != contractSegment {
			continue
		}
		root := strings.Join(append([]string{goModule}, segments[:i]...), "/")
		if isNonContract(root, nonContracts) {
			continue
		}
		return root, true
	}
	return "", false
}

// isNonContract reports whether the contract directory of root was named
// with WithNonContractPackages.
func isNonContract(root string, nonContracts []string) bool {
	dir := root + "/" + contractSegment
	for _, excluded := range nonContracts {
		if excluded == dir {
			return true
		}
	}
	return false
}

// owningModule returns the innermost module root whose tree contains
// importPath. roots must be sorted longest first.
func owningModule(importPath string, roots []string) (string, bool) {
	for _, root := range roots {
		if inTree(importPath, root) {
			return root, true
		}
	}
	return "", false
}

// isContractPackage reports whether importPath is the contract package of
// module root or a package below it. A directory named with
// WithNonContractPackages, whether it is the contract directory of root or a
// directory below it, is an ordinary package of the module, with the packages
// below it.
func isContractPackage(importPath, root string, nonContracts []string) bool {
	dir := root + "/" + contractSegment
	if !inTree(importPath, dir) {
		return false
	}
	for _, excluded := range nonContracts {
		if inTree(excluded, dir) && inTree(importPath, excluded) {
			return false
		}
	}
	return true
}

// inTree reports whether importPath is root or a package below it.
func inTree(importPath, root string) bool {
	return importPath == root || strings.HasPrefix(importPath, root+"/")
}

// matchesAny reports whether importPath is one of paths or below one of them.
func matchesAny(importPath string, paths []string) bool {
	for _, path := range paths {
		if path = strings.TrimSuffix(path, "/"); path != "" && inTree(importPath, path) {
			return true
		}
	}
	return false
}
