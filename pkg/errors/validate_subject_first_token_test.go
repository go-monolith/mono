package errors

import (
	"strings"
	"testing"
)

// The first token of a subject with a custom prefix was never looked at, so
// a wildcard there passed with wildcards not allowed.
func TestValidateSubject_FirstToken(t *testing.T) {
	for _, tc := range []struct {
		name          string
		subject       string
		allowWildcard bool
		errContains   string
	}{
		{"wildcard first, wildcards not allowed", "*.auth.login", false, "not allowed to contain wildcards"},
		{"full wildcard first, wildcards not allowed", ">.auth.login", false, "not allowed to contain wildcards"},
		{"full wildcard first is never the last token", ">.auth.login", true, "must be the last token"},
		{"empty first token", ".auth.login", false, "empty token"},
		{"wildcard first, wildcards allowed", "*.auth.login", true, ""},
		{"custom prefix", "requests.auth.login", false, ""},
		{"custom prefix that is not kebab-case", "$KV.auth.login", false, ""},
		{"custom prefix in capitals", "Orders.auth.login", false, ""},
		{"custom prefix with a wildcard later", "requests.*.login", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subjectType, err := ValidateSubject(tc.subject, tc.allowWildcard)
			if tc.errContains == "" {
				if err != nil || subjectType != SubjectTypeUnknown {
					t.Fatalf("ValidateSubject(%q) = %v, %v; want unknown type and no error", tc.subject, subjectType, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("ValidateSubject(%q) = %v, want an error containing %q", tc.subject, err, tc.errContains)
			}
		})
	}
}

// The wrappers the framework uses were not affected (they require the
// "services" or "events" prefix), and still reject these.
func TestValidateSubject_WrappersRejectWildcardFirst(t *testing.T) {
	for name, validate := range map[string]func(string) error{
		"service":          ValidateServiceSubject,
		"event subscriber": ValidateEventSubscriberSubject,
		"event definition": ValidateEventDefinitionSubject,
	} {
		if err := validate("*.auth.login"); err == nil {
			t.Errorf("%s: *.auth.login was accepted", name)
		}
	}
}
