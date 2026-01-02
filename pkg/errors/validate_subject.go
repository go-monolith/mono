package errors

import (
	"fmt"
	"regexp"
	"strings"
)

// Subject Validation

// Subject naming patterns
const (
	// ServiceSubjectPattern defines the pattern for service subjects
	ServiceSubjectPattern = "services.<module>.<service>"

	// EventSubjectPattern defines the pattern for event subjects
	EventSubjectPattern = "events.[<module>.]<domain>.[<sub-domain>].<event-type>"

	// ReservedPrefix is the reserved subject prefix for framework internal use
	ReservedPrefix = "_mono."
)

// SubjectType represents the type of NATS subject
type SubjectType int

const (
	// SubjectTypeService indicates a service subject
	SubjectTypeService SubjectType = iota

	// SubjectTypeEvent indicates an event subject
	SubjectTypeEvent

	// SubjectTypeUnknown indicates an unknown subject type
	SubjectTypeUnknown
)

// kebabCaseRegex matches valid kebab-case tokens (lowercase letters, numbers, hyphens)
var kebabCaseRegex = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ValidateSubject validates a NATS subject against naming conventions.
//
// Subject naming rules:
//   - Service subjects: services.<module>.<service>
//   - Event subjects: events.<domain>.<event-type>
//   - All tokens must be kebab-case (lowercase, numbers, hyphens)
//   - Reserved prefix "_mono." is not allowed for user subjects
//   - Wildcards (*,>) only allowed in event subjects for subscriptions
//
// Returns the subject type and an error if validation fails.
func ValidateSubject(subject string, allowWildcard bool) (SubjectType, error) {
	if subject == "" {
		return SubjectTypeUnknown, fmt.Errorf("subject cannot be empty")
	}

	// Split subject into tokens
	tokens := strings.Split(subject, ".")
	if len(tokens) < 3 {
		return SubjectTypeUnknown, fmt.Errorf("subject '%s' must have at least 3 tokens", subject)
	}

	// Determine subject type based on first token
	var subjectType SubjectType
	switch tokens[0] {
	case "services":
		subjectType = SubjectTypeService
		// Service subjects must contain exactly 3 tokens
		if len(tokens) != 3 {
			return subjectType, fmt.Errorf("service subject '%s' must have exactly 3 tokens (expected %s)", subject, ServiceSubjectPattern)
		}
	case "events":
		subjectType = SubjectTypeEvent
		// Event subjects can contain 3 or more tokens
		if len(tokens) < 3 {
			return subjectType, fmt.Errorf("event subject '%s' must have at least 3 tokens (expected %s)", subject, EventSubjectPattern)
		}
	default:
		// Check for reserved prefix
		if strings.HasPrefix(subject, ReservedPrefix) {
			return SubjectTypeUnknown, fmt.Errorf("subject '%s' uses forbidden reserved prefix '%s'", subject, ReservedPrefix)
		}
		// We do allow a custom prefix which is other than "services" and "events" as long as it is not the reserved prefix
		subjectType = SubjectTypeUnknown
	}

	// Validate each token is kebab-case (excluding wildcards for event subjects)
	for i, token := range tokens {
		// Skip first token (services/events) as it's already validated
		if i == 0 {
			continue
		}

		// Detect wildcards and handle accordingly
		if !allowWildcard && (token == "*" || token == ">") {
			return subjectType, fmt.Errorf("subject '%s' is not allowed to contain wildcards", subject)
		}

		// '>' wildcard must be the last token
		if token == ">" && i != len(tokens)-1 {
			return SubjectTypeUnknown, fmt.Errorf("subject '%s' wildcard '>' must be the last token", subject)
		}

		// Validate kebab-case for non-wildcard tokens
		if token != "*" && token != ">" && !isKebabCase(token) {
			return SubjectTypeUnknown, fmt.Errorf("subject token '%s' in '%s' must be kebab-case (lowercase, numbers, hyphens)", token, subject)
		}
	}

	return subjectType, nil
}

// ValidateServiceSubject validates a service subject.
//
// Service subjects must match: services.<module>.<service>
// All tokens must be kebab-case with no wildcards.
func ValidateServiceSubject(subject string) error {
	subjectType, err := ValidateSubject(subject, false)
	if err != nil {
		return err
	}

	if subjectType != SubjectTypeService {
		return fmt.Errorf("subject '%s' is not a valid service subject (expected %s)", subject, ServiceSubjectPattern)
	}

	return nil
}

// ValidateEventSubscriberSubject validates an event subject to be used by subscribers.
//
// Event subjects must match: events.[<module>.]<domain>.[<sub-domain>].<event-type>
// All tokens must be kebab-case. Wildcards (*,>) allowed for subscriptions.
func ValidateEventSubscriberSubject(subject string) error {
	subjectType, err := ValidateSubject(subject, true)
	if err != nil {
		return err
	}

	if subjectType != SubjectTypeEvent {
		return fmt.Errorf("subject '%s' is not a valid event subject (expected events.<domain>.<event-type> or events.<module>.<domain>.<event-type>)", subject)
	}

	return nil
}

// ValidateEventDefinitionSubject validates an event subject for event definitions.
//
// Event subjects must match: events.[<module>.]<domain>.[<sub-domain>].<event-type>
// All tokens must be kebab-case. Wildcards (*,>) are NOT allowed for event definitions.
func ValidateEventDefinitionSubject(subject string) error {
	subjectType, err := ValidateSubject(subject, false)
	if err != nil {
		return err
	}

	if subjectType != SubjectTypeEvent {
		return fmt.Errorf("subject '%s' is not a valid event subject (expected events.<domain>.<event-type> or events.<module>.<domain>.<event-type>)", subject)
	}

	return nil
}

// isKebabCase checks if a token follows kebab-case convention.
// Valid: lowercase letters, numbers, hyphens (but not starting/ending with hyphen)
func isKebabCase(token string) bool {
	return kebabCaseRegex.MatchString(token)
}

// IsKebabCase is the exported version of isKebabCase for testing.
// It checks if a token follows kebab-case convention.
func IsKebabCase(token string) bool {
	return isKebabCase(token)
}

// CheckSubjectConflict checks if a subject might conflict between services and events.
//
// This function logs warnings for subjects that could cause routing confusion.
// For example, "services.user.created" vs "events.user.created" might be confusing.
//
// Returns true if a potential conflict is detected (same module/domain and service/event names).
func CheckSubjectConflict(serviceSubjects, eventSubjects []string) []string {
	conflicts := []string{}

	for _, svc := range serviceSubjects {
		svcTokens := strings.Split(svc, ".")
		if len(svcTokens) != 3 {
			continue
		}

		svcModule := svcTokens[1]
		svcName := svcTokens[2]

		for _, evt := range eventSubjects {
			evtTokens := strings.Split(evt, ".")
			if len(evtTokens) < 3 {
				continue
			}

			evtDomain := evtTokens[1]
			evtType := evtTokens[2]

			// Check if module/domain and service/event names overlap
			if svcModule == evtDomain && svcName == evtType {
				conflicts = append(conflicts,
					fmt.Sprintf("potential conflict: service '%s' and event '%s' share similar naming", svc, evt))
			}
		}
	}

	return conflicts
}
