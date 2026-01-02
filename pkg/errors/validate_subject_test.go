package errors

import (
	"strings"
	"testing"
)

func TestValidateSubject(t *testing.T) {
	tests := []struct {
		name          string
		subject       string
		allowWildcard bool
		wantType      SubjectType
		wantErr       bool
		errContains   string
	}{
		// Valid service subjects
		{
			name:          "valid service subject",
			subject:       "services.auth.login",
			allowWildcard: false,
			wantType:      SubjectTypeService,
			wantErr:       false,
		},
		{
			name:          "valid service subject with hyphens",
			subject:       "services.user-management.create-user",
			allowWildcard: false,
			wantType:      SubjectTypeService,
			wantErr:       false,
		},
		{
			name:          "valid service subject with numbers",
			subject:       "services.api-v2.get-item",
			allowWildcard: false,
			wantType:      SubjectTypeService,
			wantErr:       false,
		},

		// Valid event subjects
		{
			name:          "valid event subject",
			subject:       "events.user.created",
			allowWildcard: false,
			wantType:      SubjectTypeEvent,
			wantErr:       false,
		},
		{
			name:          "valid event subject with hyphens",
			subject:       "events.order-processing.order-completed",
			allowWildcard: false,
			wantType:      SubjectTypeEvent,
			wantErr:       false,
		},
		{
			name:          "valid event subject with wildcard allowed",
			subject:       "events.user.*",
			allowWildcard: true,
			wantType:      SubjectTypeEvent,
			wantErr:       false,
		},
		{
			name:          "valid event subject with multi-wildcard allowed",
			subject:       "events.user.>",
			allowWildcard: true,
			wantType:      SubjectTypeEvent,
			wantErr:       false,
		},
		{
			name:          "valid event subject with wildcard in middle position",
			subject:       "events.*.user.created",
			allowWildcard: true,
			wantType:      SubjectTypeEvent,
			wantErr:       false,
		},
		{
			name:          "valid event subject with multiple wildcards",
			subject:       "events.*.*.created",
			allowWildcard: true,
			wantType:      SubjectTypeEvent,
			wantErr:       false,
		},

		// Invalid - empty subject
		{
			name:          "empty subject",
			subject:       "",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "cannot be empty",
		},

		// Invalid - reserved prefix
		{
			name:          "reserved prefix",
			subject:       "_mono.internal.event",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "reserved prefix",
		},

		// Invalid - too few tokens
		{
			name:          "too few tokens",
			subject:       "services.auth",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "at least 3 tokens",
		},

		// Custom prefix - allowed (returns SubjectTypeUnknown without error)
		{
			name:          "custom prefix allowed",
			subject:       "requests.auth.login",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       false,
		},

		// Invalid - not kebab-case (uppercase)
		{
			name:          "uppercase in token",
			subject:       "services.Auth.login",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "must be kebab-case",
		},
		{
			name:          "camelCase in token",
			subject:       "services.auth.loginUser",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "must be kebab-case",
		},
		{
			name:          "underscore in token",
			subject:       "services.auth.login_user",
			allowWildcard: false,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "must be kebab-case",
		},

		// Invalid - wildcard not allowed
		{
			name:          "wildcard in service subject not allowed",
			subject:       "services.auth.*",
			allowWildcard: false,
			wantType:      SubjectTypeService,
			wantErr:       true,
			errContains:   "not allowed to contain wildcards",
		},
		{
			name:          "wildcard in event subject not allowed",
			subject:       "events.user.*",
			allowWildcard: false,
			wantType:      SubjectTypeEvent,
			wantErr:       true,
			errContains:   "not allowed to contain wildcards",
		},

		// Invalid - service subject with too many tokens
		{
			name:          "service subject too many tokens",
			subject:       "services.auth.users.login",
			allowWildcard: false,
			wantType:      SubjectTypeService,
			wantErr:       true,
			errContains:   "exactly 3 tokens",
		},

		// Invalid - '>' wildcard not at the end
		{
			name:          "multi-wildcard not at end",
			subject:       "events.>.user.created",
			allowWildcard: true,
			wantType:      SubjectTypeUnknown,
			wantErr:       true,
			errContains:   "must be the last token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, err := ValidateSubject(tt.subject, tt.allowWildcard)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateSubject() expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateSubject() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateSubject() unexpected error = %v", err)
					return
				}
			}

			if gotType != tt.wantType {
				t.Errorf("ValidateSubject() type = %v, want %v", gotType, tt.wantType)
			}
		})
	}
}

func TestValidateServiceSubject(t *testing.T) {
	tests := []struct {
		name        string
		subject     string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid service subject",
			subject: "services.auth.login",
			wantErr: false,
		},
		{
			name:    "valid service subject with hyphens and numbers",
			subject: "services.api-v2.get-user-123",
			wantErr: false,
		},
		{
			name:        "event subject rejected",
			subject:     "events.user.created",
			wantErr:     true,
			errContains: "not a valid service subject",
		},
		{
			name:        "wildcard rejected",
			subject:     "services.auth.*",
			wantErr:     true,
			errContains: "not allowed to contain wildcards",
		},
		{
			name:        "invalid format",
			subject:     "services.Auth.Login",
			wantErr:     true,
			errContains: "kebab-case",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateServiceSubject(tt.subject)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateServiceSubject() expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateServiceSubject() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateServiceSubject() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateEventSubscriberSubject(t *testing.T) {
	tests := []struct {
		name        string
		subject     string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid event subject",
			subject: "events.user.created",
			wantErr: false,
		},
		{
			name:    "valid event subject with wildcard",
			subject: "events.user.*",
			wantErr: false,
		},
		{
			name:    "valid event subject with multi-wildcard",
			subject: "events.order.>",
			wantErr: false,
		},
		{
			name:    "valid event subject with multiple tokens",
			subject: "events.payment.order.completed",
			wantErr: false,
		},
		{
			name:        "service subject rejected",
			subject:     "services.auth.login",
			wantErr:     true,
			errContains: "not a valid event subject",
		},
		{
			name:        "invalid format",
			subject:     "events.User.Created",
			wantErr:     true,
			errContains: "kebab-case",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEventSubscriberSubject(tt.subject)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateEventSubscriberSubject() expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateEventSubscriberSubject() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateEventSubscriberSubject() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateEventDefinitionSubject(t *testing.T) {
	tests := []struct {
		name        string
		subject     string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid event subject",
			subject: "events.user.created",
			wantErr: false,
		},
		{
			name:    "valid event subject with multiple tokens",
			subject: "events.payment.order.completed",
			wantErr: false,
		},
		{
			name:    "valid event subject with version",
			subject: "events.orders.v1.created",
			wantErr: false,
		},
		{
			name:    "valid event subject with hyphens",
			subject: "events.order-processing.order-completed",
			wantErr: false,
		},
		{
			name:        "wildcard not allowed",
			subject:     "events.user.*",
			wantErr:     true,
			errContains: "not allowed to contain wildcards",
		},
		{
			name:        "multi-wildcard not allowed",
			subject:     "events.order.>",
			wantErr:     true,
			errContains: "not allowed to contain wildcards",
		},
		{
			name:        "wildcard in middle not allowed",
			subject:     "events.*.user.created",
			wantErr:     true,
			errContains: "not allowed to contain wildcards",
		},
		{
			name:        "service subject rejected",
			subject:     "services.auth.login",
			wantErr:     true,
			errContains: "not a valid event subject",
		},
		{
			name:        "invalid format",
			subject:     "events.User.Created",
			wantErr:     true,
			errContains: "kebab-case",
		},
		{
			name:        "empty subject",
			subject:     "",
			wantErr:     true,
			errContains: "cannot be empty",
		},
		{
			name:        "reserved prefix",
			subject:     "_mono.events.created",
			wantErr:     true,
			errContains: "reserved prefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEventDefinitionSubject(tt.subject)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateEventDefinitionSubject() expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateEventDefinitionSubject() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateEventDefinitionSubject() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestIsKebabCase(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  bool
	}{
		{"lowercase only", "auth", true},
		{"with hyphens", "user-management", true},
		{"with numbers", "api-v2", true},
		{"numbers only", "123", true},
		{"multiple hyphens", "user-profile-settings", true},
		{"uppercase", "Auth", false},
		{"camelCase", "authService", false},
		{"with underscore", "auth_service", false},
		{"with space", "auth service", false},
		{"starting with hyphen", "-auth", false},
		{"ending with hyphen", "auth-", false},
		{"double hyphen", "auth--service", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsKebabCase(tt.token); got != tt.want {
				t.Errorf("IsKebabCase(%v) = %v, want %v", tt.token, got, tt.want)
			}
		})
	}
}

func TestCheckSubjectConflict(t *testing.T) {
	tests := []struct {
		name            string
		serviceSubjects []string
		eventSubjects   []string
		wantConflicts   int
	}{
		{
			name:            "no conflicts",
			serviceSubjects: []string{"services.auth.login", "services.user.create"},
			eventSubjects:   []string{"events.order.created", "events.payment.completed"},
			wantConflicts:   0,
		},
		{
			name:            "one conflict",
			serviceSubjects: []string{"services.user.create"},
			eventSubjects:   []string{"events.user.create"},
			wantConflicts:   1,
		},
		{
			name:            "multiple conflicts",
			serviceSubjects: []string{"services.user.create", "services.order.update"},
			eventSubjects:   []string{"events.user.create", "events.order.update"},
			wantConflicts:   2,
		},
		{
			name:            "partial overlap no conflict",
			serviceSubjects: []string{"services.user.create"},
			eventSubjects:   []string{"events.user.deleted"},
			wantConflicts:   0,
		},
		{
			name:            "empty lists",
			serviceSubjects: []string{},
			eventSubjects:   []string{},
			wantConflicts:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conflicts := CheckSubjectConflict(tt.serviceSubjects, tt.eventSubjects)
			if len(conflicts) != tt.wantConflicts {
				t.Errorf("CheckSubjectConflict() found %d conflicts, want %d. Conflicts: %v",
					len(conflicts), tt.wantConflicts, conflicts)
			}
		})
	}
}

// TestCheckSubjectConflictEdgeCases tests edge cases for CheckSubjectConflict
func TestCheckSubjectConflictEdgeCases(t *testing.T) {
	t.Run("invalid service subject with less than 3 tokens", func(t *testing.T) {
		serviceSubjects := []string{"services.auth"} // Only 2 tokens
		eventSubjects := []string{"events.auth.v1.login"}
		conflicts := CheckSubjectConflict(serviceSubjects, eventSubjects)
		if len(conflicts) != 0 {
			t.Errorf("expected 0 conflicts for invalid service subject, got %d", len(conflicts))
		}
	})

	t.Run("invalid event subject with less than 3 tokens", func(t *testing.T) {
		serviceSubjects := []string{"services.auth.login"}
		eventSubjects := []string{"events.auth"} // Only 2 tokens
		conflicts := CheckSubjectConflict(serviceSubjects, eventSubjects)
		if len(conflicts) != 0 {
			t.Errorf("expected 0 conflicts for invalid event subject, got %d", len(conflicts))
		}
	})

	t.Run("both invalid subjects", func(t *testing.T) {
		serviceSubjects := []string{"services.auth", "invalid"}
		eventSubjects := []string{"events", "also.invalid"}
		conflicts := CheckSubjectConflict(serviceSubjects, eventSubjects)
		if len(conflicts) != 0 {
			t.Errorf("expected 0 conflicts for invalid subjects, got %d", len(conflicts))
		}
	})

	t.Run("mixed valid and invalid subjects", func(t *testing.T) {
		serviceSubjects := []string{"services.auth.login", "invalid.subject"}
		eventSubjects := []string{"events.auth.login", "events"}
		conflicts := CheckSubjectConflict(serviceSubjects, eventSubjects)
		if len(conflicts) != 1 {
			t.Errorf("expected 1 conflict from valid subjects, got %d", len(conflicts))
		}
	})
}
