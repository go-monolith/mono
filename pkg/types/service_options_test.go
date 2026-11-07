package types_test

import (
	"runtime"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

func TestApplyServiceOptions(t *testing.T) {
	t.Run("no options keeps zero values", func(t *testing.T) {
		got := types.ApplyServiceOptions()
		if got != (types.ServiceOptions{}) {
			t.Fatalf("got %+v, want zero value", got)
		}
	})

	t.Run("options set their fields", func(t *testing.T) {
		got := types.ApplyServiceOptions(types.WithMaxConcurrency(8), types.WithMaxPending(50))
		if got.MaxConcurrency != 8 || got.MaxPending != 50 {
			t.Fatalf("got %+v, want {8 50}", got)
		}
	})

	t.Run("last option wins and nil options are skipped", func(t *testing.T) {
		got := types.ApplyServiceOptions(types.WithMaxConcurrency(8), nil, types.WithMaxConcurrency(2))
		if got.MaxConcurrency != 2 {
			t.Fatalf("MaxConcurrency = %d, want 2", got.MaxConcurrency)
		}
	})
}

func TestServiceOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    types.ServiceOptions
		wantErr bool
	}{
		{name: "zero values are valid", opts: types.ServiceOptions{}},
		{name: "positive values are valid", opts: types.ServiceOptions{MaxConcurrency: 1, MaxPending: 1}},
		{name: "negative concurrency", opts: types.ServiceOptions{MaxConcurrency: -1}, wantErr: true},
		{name: "negative pending", opts: types.ServiceOptions{MaxPending: -1}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServiceOptions_Resolve(t *testing.T) {
	builtIn := runtime.GOMAXPROCS(0) * 4

	tests := []struct {
		name            string
		opts            types.ServiceOptions
		defaultConc     int
		wantConcurrency int
		wantPending     int
	}{
		{
			name:            "zero values use built-in defaults",
			wantConcurrency: builtIn,
			wantPending:     types.DefaultMaxPending(builtIn),
		},
		{
			name:            "framework default replaces zero concurrency",
			defaultConc:     3,
			wantConcurrency: 3,
			wantPending:     types.DefaultMaxPendingFloor, // 3*16 = 48 < floor
		},
		{
			name:            "explicit concurrency beats framework default",
			opts:            types.ServiceOptions{MaxConcurrency: 20},
			defaultConc:     3,
			wantConcurrency: 20,
			wantPending:     20 * types.DefaultMaxPendingFactor, // 320 > floor
		},
		{
			name:            "explicit pending is kept",
			opts:            types.ServiceOptions{MaxConcurrency: 1, MaxPending: 2},
			wantConcurrency: 1,
			wantPending:     2,
		},
		{
			name:            "negative framework default is ignored",
			defaultConc:     -4,
			wantConcurrency: builtIn,
			wantPending:     types.DefaultMaxPending(builtIn),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opts.Resolve(tt.defaultConc)
			if got.MaxConcurrency != tt.wantConcurrency || got.MaxPending != tt.wantPending {
				t.Fatalf("Resolve(%d) = %+v, want {%d %d}", tt.defaultConc, got, tt.wantConcurrency, tt.wantPending)
			}
		})
	}
}

func TestServiceOptions_ResolveSerial(t *testing.T) {
	got := (types.ServiceOptions{}).ResolveSerial()
	if got.MaxConcurrency != 1 || got.MaxPending != types.DefaultMaxPendingFloor {
		t.Fatalf("ResolveSerial() = %+v, want {1 %d}", got, types.DefaultMaxPendingFloor)
	}
	got = (types.ServiceOptions{MaxConcurrency: 4, MaxPending: 9}).ResolveSerial()
	if got.MaxConcurrency != 4 || got.MaxPending != 9 {
		t.Fatalf("ResolveSerial() with explicit values = %+v, want {4 9}", got)
	}
	if got := (types.ServiceOptions{}).ResolveWithDefault(-3); got.MaxConcurrency != 1 {
		t.Fatalf("ResolveWithDefault(-3).MaxConcurrency = %d, want 1", got.MaxConcurrency)
	}
}

func TestDefaultMaxConcurrency(t *testing.T) {
	if got, want := types.DefaultMaxConcurrency(), runtime.GOMAXPROCS(0)*4; got != want {
		t.Fatalf("DefaultMaxConcurrency() = %d, want %d", got, want)
	}
}

func TestDefaultMaxPending(t *testing.T) {
	tests := []struct {
		concurrency int
		want        int
	}{
		{concurrency: 0, want: types.DefaultMaxPendingFloor},
		{concurrency: 1, want: types.DefaultMaxPendingFloor},
		{concurrency: 16, want: types.DefaultMaxPendingFloor},
		{concurrency: 17, want: 272},
		{concurrency: 32, want: 512},
	}
	for _, tt := range tests {
		if got := types.DefaultMaxPending(tt.concurrency); got != tt.want {
			t.Errorf("DefaultMaxPending(%d) = %d, want %d", tt.concurrency, got, tt.want)
		}
	}
}

func TestServiceOptions_PendingLimits(t *testing.T) {
	t.Run("request-reply defaults", func(t *testing.T) {
		msgs, bytes, apply := (types.ServiceOptions{}).PendingLimits(true)
		if !apply || msgs != types.DefaultRequestReplyPendingMsgs || bytes != types.DefaultRequestReplyPendingBytes {
			t.Fatalf("got %d/%d apply=%v", msgs, bytes, apply)
		}
	})
	t.Run("other kinds apply nothing by default", func(t *testing.T) {
		if _, _, apply := (types.ServiceOptions{}).PendingLimits(false); apply {
			t.Fatal("zero options must not apply limits for queue groups/events")
		}
	})
	t.Run("explicit values and unlimited", func(t *testing.T) {
		msgs, bytes, apply := (types.ServiceOptions{PendingMsgs: 100, PendingBytes: -1}).PendingLimits(false)
		if !apply || msgs != 100 || bytes != -1 {
			t.Fatalf("got %d/%d apply=%v", msgs, bytes, apply)
		}
		msgs, bytes, _ = (types.ServiceOptions{PendingMsgs: 100}).PendingLimits(true)
		if msgs != 100 || bytes != types.DefaultRequestReplyPendingBytes {
			t.Fatalf("request-reply with explicit msgs: got %d/%d", msgs, bytes)
		}
		// Queue group / event with one dimension: the other is never 0.
		msgs, bytes, apply = (types.ServiceOptions{PendingBytes: 1 << 20}).PendingLimits(false)
		if !apply || msgs != nats.DefaultSubPendingMsgsLimit || bytes != 1<<20 {
			t.Fatalf("single dimension: got %d/%d apply=%v", msgs, bytes, apply)
		}
	})
	t.Run("validation and option", func(t *testing.T) {
		if err := (types.ServiceOptions{PendingMsgs: -2}).Validate(); err == nil {
			t.Fatal("PendingMsgs -2 must be rejected")
		}
		if err := (types.ServiceOptions{PendingBytes: -2}).Validate(); err == nil {
			t.Fatal("PendingBytes -2 must be rejected")
		}
		if err := (types.ServiceOptions{PendingMsgs: -1, PendingBytes: -1}).Validate(); err != nil {
			t.Fatalf("-1 (unlimited) must be valid: %v", err)
		}
		got := types.ApplyServiceOptions(types.WithPendingLimits(5, 6))
		if got.PendingMsgs != 5 || got.PendingBytes != 6 {
			t.Fatalf("WithPendingLimits = %+v", got)
		}
	})
}

// TestServiceOptions_PendingLimits_SingleDimension checks the single-dimension
// cases: the dimension left at zero takes nats.go's default, never 0 (which
// nats.go rejects).
func TestServiceOptions_PendingLimits_SingleDimension(t *testing.T) {
	msgs, bytes, apply := types.ServiceOptions{PendingBytes: 4096}.PendingLimits(false)
	if !apply || msgs != nats.DefaultSubPendingMsgsLimit || bytes != 4096 {
		t.Fatalf("bytes-only PendingLimits = %d/%d/%v, want %d/4096/true", msgs, bytes, apply, nats.DefaultSubPendingMsgsLimit)
	}
	msgs, bytes, apply = types.ServiceOptions{PendingMsgs: 1000}.PendingLimits(false)
	if !apply || msgs != 1000 || bytes != nats.DefaultSubPendingBytesLimit {
		t.Fatalf("msgs-only PendingLimits = %d/%d/%v, want 1000/%d/true", msgs, bytes, apply, nats.DefaultSubPendingBytesLimit)
	}
	msgs, bytes, apply = types.ServiceOptions{PendingBytes: -1}.PendingLimits(true)
	if !apply || msgs != types.DefaultRequestReplyPendingMsgs || bytes != -1 {
		t.Fatalf("request-reply PendingLimits = %d/%d/%v, want %d/-1/true", msgs, bytes, apply, types.DefaultRequestReplyPendingMsgs)
	}
}
