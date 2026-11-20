package accesslog

import (
	"context"
	"strings"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestForeignRequestIDCannotForgeALine: the request ID header comes from the
// sender. In text format a value with spaces used to add fields to the line,
// and a long one made the line as long.
func TestForeignRequestIDCannotForgeALine(t *testing.T) {
	forged := "x module=billing service=refund status=success duration_ms=1 \x1b[2J\r\nts=2020-01-01T00:00:00Z" +
		strings.Repeat("A", 900_000)
	for name, format := range map[string]Format{"text": FormatText, "json": FormatJSON} {
		t.Run(name, func(t *testing.T) {
			buf := &syncWriter{}
			m, err := New(WithOutput(buf), WithFormat(format))
			if err != nil {
				t.Fatal(err)
			}
			h := m.wrapRequestReplyHandler(func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }, "orders", "place")
			if _, err := h(context.Background(), &types.Msg{Header: types.Header{"X-Request-ID": {forged}}}); err != nil {
				t.Fatal(err)
			}
			lines := buf.Lines()
			if len(lines) != 1 {
				t.Fatalf("%d lines written, want 1: %q", len(lines), lines)
			}
			line := lines[0]
			if len(line) > 1024 {
				t.Fatalf("line of %d bytes", len(line))
			}
			if strings.ContainsAny(line, "\x1b\r") {
				t.Fatalf("control characters in the line: %q", line)
			}
			if format == FormatText {
				if n := strings.Count(line, " module="); n != 1 || !strings.Contains(line, " module=orders ") {
					t.Fatalf("forged or missing module field (%d): %q", n, line)
				}
				if n := strings.Count(line, " status="); n != 1 {
					t.Fatalf("%d status fields: %q", n, line)
				}
				if !strings.Contains(line, "request_id=x_module=billing_service=refund_status=success_") {
					t.Fatalf("request ID not sanitised in place: %q", line)
				}
			}
		})
	}
}
