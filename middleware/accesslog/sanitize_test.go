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

// TestPanickingHandlerLeavesALine: a request whose handler panics is the one
// an operator looks for. Each wrapper writes its line with status error, and
// the panic goes on to the framework unchanged.
func TestPanickingHandlerLeavesALine(t *testing.T) {
	msg := &types.Msg{Data: []byte("abc")}
	wrappers := map[string]func(m *AccessLogModule){
		"request_reply": func(m *AccessLogModule) {
			_, _ = m.wrapRequestReplyHandler(func(context.Context, *types.Msg) ([]byte, error) { panic("boom") }, "orders", "place")(context.Background(), msg)
		},
		"queue_group": func(m *AccessLogModule) {
			_ = m.wrapQueueGroupHandler(func(context.Context, *types.Msg) error { panic("boom") }, "orders", "place")(context.Background(), msg)
		},
		"event_consumer": func(m *AccessLogModule) {
			_ = m.wrapEventConsumerHandler(func(context.Context, *types.Msg) error { panic("boom") }, "orders", "place")(context.Background(), msg)
		},
		"stream_consumer": func(m *AccessLogModule) {
			_ = m.wrapStreamConsumerHandler(func(context.Context, []*types.Msg) error { panic("boom") }, "orders", "place")(context.Background(), []*types.Msg{msg})
		},
		"event_stream_consumer": func(m *AccessLogModule) {
			_ = m.wrapEventStreamConsumerHandler(func(context.Context, []*types.Msg) error { panic("boom") }, "orders", "place")(context.Background(), []*types.Msg{msg})
		},
	}
	for serviceType, call := range wrappers {
		t.Run(serviceType, func(t *testing.T) {
			buf := &syncWriter{}
			m, err := New(WithOutput(buf))
			if err != nil {
				t.Fatal(err)
			}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				call(m)
			}()
			if recovered != "boom" {
				t.Fatalf("the panic did not reach the caller of the wrapper unchanged: %v", recovered)
			}
			lines := buf.Lines()
			if len(lines) != 1 {
				t.Fatalf("%d lines written for a handler that panicked, want 1", len(lines))
			}
			for _, want := range []string{"service_type=" + serviceType, "status=error", "request_size=3"} {
				if !strings.Contains(lines[0], want) {
					t.Errorf("line %q lacks %q", lines[0], want)
				}
			}
		})
	}
}
