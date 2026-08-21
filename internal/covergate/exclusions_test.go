package covergate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Paths checked by this package, relative to the repository root.
const (
	gateScriptPath    = "scripts/coverage-gate.sh"
	codecovConfigPath = "codecov.yml"
	goModPath         = "go.mod"
)

// excludeRePattern pulls the two interesting pieces out of the gate script's
// EXCLUDE_RE assignment:
//
//	EXCLUDE_RE='^github\.com/go-monolith/mono/(examples|bench|test)/'
//
// Submatch 1 is the module prefix as written (dots still backslash-escaped for
// the shell's grep -E), submatch 2 the pipe-separated directory alternation.
//
// Matching the exact shape is the point rather than an inconvenience: if
// someone rewrites the exclusion into a form this does not recognise, the
// parse fails and says so. Silently falling back to "no exclusions parsed,
// therefore nothing to compare" is the one outcome that would make this
// package useless.
var excludeRePattern = regexp.MustCompile(`^EXCLUDE_RE='\^(.+?)/\((.+?)\)/'$`)

// excludeReUsePattern matches the point where the gate script actually applies
// EXCLUDE_RE. Without this check the variable could be assigned correctly and
// never used, leaving the gate excluding nothing while both drift tests pass.
var excludeReUsePattern = regexp.MustCompile(`grep\s+-Ev\s+"\$EXCLUDE_RE"`)

// fixesPattern matches the codecov.yml fixes entry that strips the module
// import path from profile lines, e.g.
//
//   - "github.com/go-monolith/mono/::"
var fixesPattern = regexp.MustCompile(`^\s*-\s*"(.+?)/::"\s*$`)

// ignoreGlobPattern matches one entry of the codecov.yml ignore list and
// captures the directory it names, e.g. `- "examples/**"` yields "examples".
var ignoreGlobPattern = regexp.MustCompile(`^\s*-\s*"(.+?)/\*\*"\s*$`)

// modulePattern matches the go.mod module directive.
var modulePattern = regexp.MustCompile(`^module\s+(\S+)$`)

// TestCoverageExclusionsAgree fails when the gate script and codecov.yml
// exclude different sets of directories.
func TestCoverageExclusionsAgree(t *testing.T) {
	root := repoRoot(t)

	_, gateDirs := mustParseGateScript(t, root)
	codecovDirs := mustParseCodecovIgnores(t, root)

	if diff := diffSets(gateDirs, codecovDirs); diff != "" {
		t.Errorf("coverage exclusions have drifted apart:\n%s\n"+
			"EXCLUDE_RE in %s and the ignore globs in %s must name the same directories.\n"+
			"When they differ, the enforced floor and the Codecov badge measure different\n"+
			"sets of packages and report different numbers for the same commit.",
			diff, gateScriptPath, codecovConfigPath)
	}
}

// TestExcludedDirectoriesExist fails when the exclusions name a directory that
// is no longer in the repository.
//
// Both files can agree perfectly on a stale name, in which case the exclusions
// quietly become no-ops and neither drift test notices. Renaming an excluded
// tree is exactly the kind of change that would leave them behind.
func TestExcludedDirectoriesExist(t *testing.T) {
	root := repoRoot(t)

	_, dirs := mustParseGateScript(t, root)
	for _, dir := range dirs {
		info, err := os.Stat(filepath.Join(root, dir))
		if err != nil {
			t.Errorf("the coverage exclusions name %q, which does not exist in the repository: %v\n"+
				"A stale exclusion silently matches nothing. Update %s and %s together.",
				dir, err, gateScriptPath, codecovConfigPath)
			continue
		}
		if !info.IsDir() {
			t.Errorf("the coverage exclusions name %q, which exists but is not a directory", dir)
		}
	}
}

// TestGateScriptAppliesItsExclusions fails when EXCLUDE_RE is assigned but
// never used.
//
// The two drift tests compare the assignment against codecov.yml, so a script
// that computes the right pattern and then forgets to filter with it would
// still pass them while excluding nothing at all.
func TestGateScriptAppliesItsExclusions(t *testing.T) {
	root := repoRoot(t)
	data := readFile(t, filepath.Join(root, gateScriptPath))

	if !excludeReUsePattern.MatchString(data) {
		t.Errorf("%s assigns EXCLUDE_RE but never applies it via a %s filter.\n"+
			"The pattern would be computed and discarded, so every package would count "+
			"toward the floor regardless of the exclusion list.",
			gateScriptPath, excludeReUsePattern)
	}
}

// TestCoverageConfigUsesTheRealModulePath fails when either file hardcodes a
// module path that go.mod no longer declares.
//
// This is the same class of silent drift as the exclusion lists. A Go coverage
// profile names files by import path, so both the gate's EXCLUDE_RE and
// codecov.yml's fixes entry have the module path baked into them. Rename the
// module and neither one errors: the gate simply stops excluding anything and
// Codecov's root-anchored ignore globs stop matching, both reporting a
// plausible-looking but wrong figure.
func TestCoverageConfigUsesTheRealModulePath(t *testing.T) {
	root := repoRoot(t)

	module, err := parseModulePath(readFile(t, filepath.Join(root, goModPath)))
	if err != nil {
		t.Fatalf("cannot read the module path from %s: %v", goModPath, err)
	}

	gateModule, _ := mustParseGateScript(t, root)
	if gateModule != module {
		t.Errorf("EXCLUDE_RE in %s is anchored to module %q, but go.mod declares %q.\n"+
			"The exclusions match nothing until these agree, so the gate would measure every package.",
			gateScriptPath, gateModule, module)
	}

	fixesModule, err := parseCodecovFixes(readFile(t, filepath.Join(root, codecovConfigPath)))
	if err != nil {
		t.Fatalf("cannot read the fixes entry from %s: %v", codecovConfigPath, err)
	}
	if fixesModule != module {
		t.Errorf("the fixes: entry in %s strips prefix %q, but go.mod declares %q.\n"+
			"Codecov's ignore globs are root-anchored, so they match nothing until these agree.",
			codecovConfigPath, fixesModule, module)
	}
}

// The parsers below are pure functions over file contents so that their own
// behaviour can be tested directly; see parsers_test.go. The mustParse
// wrappers read the real files and turn a parse error into a fatal test
// failure.

// parseGateScript returns the module prefix and the excluded directories named
// by EXCLUDE_RE in the coverage gate script.
//
// Exactly one assignment must be present. Bash uses the last assignment that
// executes, so accepting several and reading the first would let this package
// validate a value the script never applies.
func parseGateScript(content string) (module string, dirs []string, err error) {
	var matches [][]string
	for line := range strings.SplitSeq(content, "\n") {
		if m := excludeRePattern.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			matches = append(matches, m)
		}
	}

	switch len(matches) {
	case 1:
		m := matches[0]
		// The shell side needs `\.` so grep -E treats the dots literally;
		// compare against go.mod without them.
		return strings.ReplaceAll(m[1], `\.`, "."), strings.Split(m[2], "|"), nil
	case 0:
		return "", nil, fmt.Errorf("no EXCLUDE_RE assignment matching %s found; "+
			"if the exclusion list moved or changed shape, update this package to match "+
			"rather than deleting it, or the two exclusion lists go back to being unenforced",
			excludeRePattern)
	default:
		return "", nil, fmt.Errorf("found %d EXCLUDE_RE assignments, expected exactly 1; "+
			"the shell applies whichever executes last, so this package cannot tell which one is live",
			len(matches))
	}
}

// parseCodecovIgnores returns the directories named by the ignore list in
// codecov.yml.
//
// The block is found by scanning for a top-level ignore key and collecting the
// list entries that follow it. That is enough structure for a file of this
// size and shape, and avoids taking on a YAML dependency for one assertion —
// the foundation spec caps this module's dependencies at NATS and the standard
// library. Exactly one such block must be present, since a duplicate key later
// in the file is what Codecov would actually use.
func parseCodecovIgnores(content string) ([]string, error) {
	var (
		dirs     []string
		inIgnore bool
		blocks   int
	)

	for line := range strings.SplitSeq(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// A line at column zero starts a new top-level key. List entries are
		// exempt so that an unindented ignore list still parses.
		if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "-") {
			inIgnore = trimmed == "ignore:"
			if inIgnore {
				blocks++
			}
			continue
		}

		if !inIgnore {
			continue
		}

		m := ignoreGlobPattern.FindStringSubmatch(line)
		if m == nil {
			return nil, fmt.Errorf("unrecognised entry in the ignore list: %q; "+
				"entries are expected to look like `- \"examples/**\"`, so update this "+
				"package if the convention changed rather than leaving the lists unguarded",
				trimmed)
		}
		dirs = append(dirs, m[1])
	}

	switch {
	case blocks == 0:
		return nil, fmt.Errorf("no top-level ignore: key found")
	case blocks > 1:
		return nil, fmt.Errorf("found %d top-level ignore: keys, expected exactly 1; "+
			"Codecov uses the last one, so this package cannot tell which is live", blocks)
	case len(dirs) == 0:
		return nil, fmt.Errorf("the ignore: list is empty; the gate script excludes packages, so this cannot be right")
	}
	return dirs, nil
}

// parseCodecovFixes returns the module prefix stripped by the codecov.yml
// fixes entry. Exactly one entry must be present.
func parseCodecovFixes(content string) (string, error) {
	var found []string
	for line := range strings.SplitSeq(content, "\n") {
		if m := fixesPattern.FindStringSubmatch(line); m != nil {
			found = append(found, m[1])
		}
	}

	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return "", fmt.Errorf("no fixes: entry of the form `- \"<module>/::\"` found; " +
			"without it Codecov's root-anchored ignore globs never match the import paths " +
			"a Go profile contains, and every exclusion silently becomes a no-op")
	default:
		return "", fmt.Errorf("found %d fixes: prefix entries, expected exactly 1", len(found))
	}
}

// parseModulePath returns the module path declared by go.mod.
func parseModulePath(content string) (string, error) {
	for line := range strings.SplitSeq(content, "\n") {
		if m := modulePattern.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			return m[1], nil
		}
	}
	return "", fmt.Errorf("no module directive found")
}

// mustParseGateScript reads the real gate script and fails the test if it
// cannot be parsed.
func mustParseGateScript(t *testing.T, root string) (module string, dirs []string) {
	t.Helper()

	module, dirs, err := parseGateScript(readFile(t, filepath.Join(root, gateScriptPath)))
	if err != nil {
		t.Fatalf("cannot read the exclusions from %s: %v", gateScriptPath, err)
	}
	return module, dirs
}

// mustParseCodecovIgnores reads the real Codecov config and fails the test if
// its ignore list cannot be parsed.
func mustParseCodecovIgnores(t *testing.T, root string) []string {
	t.Helper()

	dirs, err := parseCodecovIgnores(readFile(t, filepath.Join(root, codecovConfigPath)))
	if err != nil {
		t.Fatalf("cannot read the ignore list from %s: %v", codecovConfigPath, err)
	}
	return dirs
}

// repoRoot walks up from the test's working directory to the directory holding
// go.mod, so the checks work regardless of where the test binary is run from.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("cannot determine the working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, goModPath)); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no %s found in any parent of the working directory", goModPath)
		}
		dir = parent
	}
}

// readFile returns the contents of path, failing the test if it cannot be read.
func readFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s: %v", path, err)
	}
	return string(data)
}

// diffSets reports the directories present in one list but not the other,
// returning "" when the two name the same set. Duplicates are collapsed: what
// matters is which directories are excluded, not how many times each is listed.
func diffSets(gate, codecov []string) string {
	inGate := toSet(gate)
	inCodecov := toSet(codecov)

	missing := difference(inGate, inCodecov)
	extra := difference(inCodecov, inGate)
	if len(missing) == 0 && len(extra) == 0 {
		return ""
	}

	var b strings.Builder
	if len(missing) > 0 {
		fmt.Fprintf(&b, "  excluded by %s but not by %s: %s\n",
			gateScriptPath, codecovConfigPath, strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		fmt.Fprintf(&b, "  excluded by %s but not by %s: %s\n",
			codecovConfigPath, gateScriptPath, strings.Join(extra, ", "))
	}
	return strings.TrimRight(b.String(), "\n")
}

func toSet(items []string) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		if trimmed := strings.Trim(strings.TrimSpace(item), "/"); trimmed != "" {
			set[trimmed] = struct{}{}
		}
	}
	return set
}

func difference(a, b map[string]struct{}) []string {
	var only []string
	for item := range a {
		if _, ok := b[item]; !ok {
			only = append(only, item)
		}
	}
	sort.Strings(only)
	return only
}
