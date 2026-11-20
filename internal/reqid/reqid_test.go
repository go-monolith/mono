package reqid

import (
	"strings"
	"testing"
)

func TestSanitize(t *testing.T) {
	long := strings.Repeat("a", 900_000)
	tests := []struct {
		name, in, want string
	}{
		{"empty", "", ""},
		{"uuid", "3f2c8a1e-9b7d-4c1a-8e2f-0a1b2c3d4e5f", "3f2c8a1e-9b7d-4c1a-8e2f-0a1b2c3d4e5f"},
		{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		{"base64 and prefixes", "req_AbC+/=@tenant/1:2~x.y", "req_AbC+/=@tenant/1:2~x.y"},
		{"forged fields", "x module=billing service=refund status=success", "x_module=billing_service=refund_status=success"},
		{"control characters", "a\x1b[2Jb\r\nc\x00d\te", "a__2Jb__c_d_e"},
		{"quotes and braces", `a"b'c{d}e\f`, "a_b_c_d_e_f"},
		{"non-ASCII", "id-é-日", "id-__-___"},
		{"too long", long, long[:MaxLen]},
		{"too long and dirty", strings.Repeat(" ", MaxLen+10), strings.Repeat("_", MaxLen)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sanitize(tt.in)
			if got != tt.want {
				t.Fatalf("Sanitize() = %q, want %q", got, tt.want)
			}
			if again := Sanitize(got); again != got {
				t.Fatalf("not idempotent: %q then %q", got, again)
			}
		})
	}
}

func TestSanitize_CleanIDDoesNotAllocate(t *testing.T) {
	id := "3f2c8a1e-9b7d-4c1a-8e2f-0a1b2c3d4e5f"
	if n := testing.AllocsPerRun(100, func() { _ = Sanitize(id) }); n != 0 {
		t.Fatalf("Sanitize allocates %v times for a clean ID", n)
	}
}
