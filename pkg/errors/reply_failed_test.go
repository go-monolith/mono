package errors_test

import (
	"errors"
	"strings"
	"testing"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func TestWrapReplyFailed(t *testing.T) {
	err := monoerrors.WrapReplyFailed("place", "orders", types.ServiceTypeRequestReply, "nats: maximum payload exceeded")
	if !errors.Is(err, monoerrors.ErrReplyFailed) || !strings.Contains(err.Error(), "maximum payload exceeded") {
		t.Fatalf("err = %v, want ErrReplyFailed with its reason", err)
	}
	var serviceErr *monoerrors.ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.ServiceName != "place" || serviceErr.ModuleName != "orders" {
		t.Fatalf("err = %#v, want a ServiceError of the service", err)
	}
	if bare := monoerrors.WrapReplyFailed("place", "orders", types.ServiceTypeRequestReply, ""); !errors.Is(bare, monoerrors.ErrReplyFailed) {
		t.Fatalf("err = %v, want ErrReplyFailed", bare)
	}
}
