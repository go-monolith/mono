package covergate

import (
	"strings"
	"testing"
)

// The tests in this file exercise the parsers against inputs the repository
// does not currently contain. Without them the parsers would only ever see the
// one shape that exists today, and a regression that made a malformed file
// parse as "no exclusions" instead of failing would ship undetected — the
// precise no-op this package exists to prevent.

func TestParseGateScript(t *testing.T) {
	const valid = `EXCLUDE_RE='^github\.com/go-monolith/mono/(examples|bench|test)/'`

	tests := []struct {
		name       string
		content    string
		wantModule string
		wantDirs   []string
		wantErr    string
	}{
		{
			name:       "canonical assignment",
			content:    "#!/usr/bin/env bash\n" + valid + "\n",
			wantModule: "github.com/go-monolith/mono",
			wantDirs:   []string{"examples", "bench", "test"},
		},
		{
			name:       "carriage returns are tolerated",
			content:    "#!/usr/bin/env bash\r\n" + valid + "\r\n",
			wantModule: "github.com/go-monolith/mono",
			wantDirs:   []string{"examples", "bench", "test"},
		},
		{
			name:       "single excluded directory",
			content:    `EXCLUDE_RE='^example\.com/m/(only)/'`,
			wantModule: "example.com/m",
			wantDirs:   []string{"only"},
		},
		{
			name:    "missing assignment",
			content: "#!/usr/bin/env bash\necho hello\n",
			wantErr: "no EXCLUDE_RE assignment",
		},
		{
			name:    "renamed variable",
			content: `EXCLUDED_DIRS='^github\.com/go-monolith/mono/(examples|bench)/'`,
			wantErr: "no EXCLUDE_RE assignment",
		},
		{
			name:    "double quotes instead of single",
			content: `EXCLUDE_RE="^github\.com/go-monolith/mono/(examples)/"`,
			wantErr: "no EXCLUDE_RE assignment",
		},
		{
			name:    "no alternation group",
			content: `EXCLUDE_RE='^github\.com/go-monolith/mono/examples/'`,
			wantErr: "no EXCLUDE_RE assignment",
		},
		{
			name:    "duplicate assignments are ambiguous",
			content: valid + "\n" + `EXCLUDE_RE='^github\.com/go-monolith/mono/(bench)/'` + "\n",
			wantErr: "found 2 EXCLUDE_RE assignments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			module, dirs, err := parseGateScript(tt.content)
			checkErr(t, err, tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			if module != tt.wantModule {
				t.Errorf("module = %q, want %q", module, tt.wantModule)
			}
			if !equalStrings(dirs, tt.wantDirs) {
				t.Errorf("dirs = %v, want %v", dirs, tt.wantDirs)
			}
		})
	}
}

func TestParseCodecovIgnores(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantDirs []string
		wantErr  string
	}{
		{
			name:     "canonical block",
			content:  "ignore:\n  - \"examples/**\"\n  - \"bench/**\"\n",
			wantDirs: []string{"examples", "bench"},
		},
		{
			name:     "comments and blank lines do not end the block",
			content:  "# leading comment\n\nignore:\n  # why examples are excluded\n  - \"examples/**\"\n\n  - \"bench/**\"\n",
			wantDirs: []string{"examples", "bench"},
		},
		{
			name:     "carriage returns are tolerated",
			content:  "ignore:\r\n  - \"examples/**\"\r\n  - \"bench/**\"\r\n",
			wantDirs: []string{"examples", "bench"},
		},
		{
			name:     "unindented list entries",
			content:  "ignore:\n- \"examples/**\"\n- \"bench/**\"\n",
			wantDirs: []string{"examples", "bench"},
		},
		{
			name:     "a later top-level key ends the block",
			content:  "ignore:\n  - \"examples/**\"\ncomment:\n  require_changes: true\n",
			wantDirs: []string{"examples"},
		},
		{
			name:     "nested paths are preserved",
			content:  "ignore:\n  - \"internal/vendor/**\"\n",
			wantDirs: []string{"internal/vendor"},
		},
		{
			name:    "no ignore key at all",
			content: "coverage:\n  status:\n    project:\n      default:\n        target: 80%\n",
			wantErr: "no top-level ignore: key",
		},
		{
			name:    "ignore nested under another key is not top level",
			content: "coverage:\n  ignore:\n    - \"examples/**\"\n",
			wantErr: "no top-level ignore: key",
		},
		{
			name:    "duplicate top-level ignore keys are ambiguous",
			content: "ignore:\n  - \"examples/**\"\ncomment:\n  require_changes: true\nignore:\n  - \"bench/**\"\n",
			wantErr: "found 2 top-level ignore: keys",
		},
		{
			name:    "flow style is not silently accepted",
			content: "ignore: [\"examples/**\", \"bench/**\"]\n",
			wantErr: "no top-level ignore: key",
		},
		{
			name:    "unquoted entry is rejected rather than skipped",
			content: "ignore:\n  - examples/**\n",
			wantErr: "unrecognised entry",
		},
		{
			name:    "entry without the glob suffix is rejected",
			content: "ignore:\n  - \"examples\"\n",
			wantErr: "unrecognised entry",
		},
		{
			name:    "empty block",
			content: "ignore:\ncomment:\n  require_changes: true\n",
			wantErr: "ignore: list is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirs, err := parseCodecovIgnores(tt.content)
			checkErr(t, err, tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			if !equalStrings(dirs, tt.wantDirs) {
				t.Errorf("dirs = %v, want %v", dirs, tt.wantDirs)
			}
		})
	}
}

func TestParseCodecovFixes(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
		wantErr string
	}{
		{
			name:    "canonical entry",
			content: "fixes:\n  - \"github.com/go-monolith/mono/::\"\n",
			want:    "github.com/go-monolith/mono",
		},
		{
			name:    "carriage returns are tolerated",
			content: "fixes:\r\n  - \"github.com/go-monolith/mono/::\"\r\n",
			want:    "github.com/go-monolith/mono",
		},
		{
			name:    "missing entry",
			content: "ignore:\n  - \"examples/**\"\n",
			wantErr: "no fixes: entry",
		},
		{
			name:    "duplicate entries are ambiguous",
			content: "fixes:\n  - \"a/b/::\"\n  - \"c/d/::\"\n",
			wantErr: "found 2 fixes: prefix entries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCodecovFixes(tt.content)
			checkErr(t, err, tt.wantErr)
			if tt.wantErr == "" && got != tt.want {
				t.Errorf("prefix = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseModulePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
		wantErr string
	}{
		{
			name:    "canonical go.mod",
			content: "module github.com/go-monolith/mono\n\ngo 1.25.0\n",
			want:    "github.com/go-monolith/mono",
		},
		{
			name:    "carriage returns are tolerated",
			content: "module github.com/go-monolith/mono\r\n\r\ngo 1.25.0\r\n",
			want:    "github.com/go-monolith/mono",
		},
		{
			name:    "a require line naming a module is not the directive",
			content: "go 1.25.0\n\nrequire (\n\tgithub.com/google/uuid v1.6.0\n)\n",
			wantErr: "no module directive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseModulePath(tt.content)
			checkErr(t, err, tt.wantErr)
			if tt.wantErr == "" && got != tt.want {
				t.Errorf("module = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiffSets(t *testing.T) {
	tests := []struct {
		name     string
		gate     []string
		codecov  []string
		wantDiff bool
		contains string
	}{
		{
			name:    "identical sets agree",
			gate:    []string{"examples", "bench", "test"},
			codecov: []string{"examples", "bench", "test"},
		},
		{
			name:    "order does not matter",
			gate:    []string{"test", "examples", "bench"},
			codecov: []string{"examples", "bench", "test"},
		},
		{
			name:    "trailing slashes are normalised away",
			gate:    []string{"examples/", "bench"},
			codecov: []string{"examples", "bench/"},
		},
		{
			name:     "directory only in the gate script",
			gate:     []string{"examples", "bench"},
			codecov:  []string{"examples"},
			wantDiff: true,
			contains: "but not by " + codecovConfigPath + ": bench",
		},
		{
			name:     "directory only in codecov.yml",
			gate:     []string{"examples"},
			codecov:  []string{"examples", "docs"},
			wantDiff: true,
			contains: "but not by " + gateScriptPath + ": docs",
		},
		{
			name:     "disjoint sets report both directions",
			gate:     []string{"a"},
			codecov:  []string{"b"},
			wantDiff: true,
			contains: "excluded by",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff := diffSets(tt.gate, tt.codecov)
			if tt.wantDiff && diff == "" {
				t.Fatal("expected a reported difference, got none")
			}
			if !tt.wantDiff && diff != "" {
				t.Fatalf("expected no difference, got:\n%s", diff)
			}
			if tt.contains != "" && !strings.Contains(diff, tt.contains) {
				t.Errorf("diff %q does not mention %q", diff, tt.contains)
			}
		})
	}
}

// TestExcludeReUsePatternMatchesRealUsage keeps the "is EXCLUDE_RE actually
// applied" check honest: the pattern must match the form the script uses and
// must not match a call that filters on something else.
func TestExcludeReUsePattern(t *testing.T) {
	if !excludeReUsePattern.MatchString(`grep -Ev "$EXCLUDE_RE" <(tail -n +2 "$PROFILE")`) {
		t.Error("pattern does not match the form the gate script uses")
	}
	if excludeReUsePattern.MatchString(`grep -Ev "$SOMETHING_ELSE" file`) {
		t.Error("pattern matches a filter on an unrelated variable")
	}
}

func checkErr(t *testing.T, err error, want string) {
	t.Helper()

	switch {
	case want == "" && err != nil:
		t.Fatalf("unexpected error: %v", err)
	case want != "" && err == nil:
		t.Fatalf("expected an error containing %q, got none", want)
	case want != "" && !strings.Contains(err.Error(), want):
		t.Fatalf("error %q does not contain %q", err, want)
	}
}

func equalStrings(got, want []string) bool {
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
