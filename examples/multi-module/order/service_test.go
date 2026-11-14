package order_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-monolith/mono/examples/multi-module/order"
)

// These tests run the order domain with fake ports. They import nothing but
// the order package and the standard library: no framework, no NATS, no other
// module, no serialization.

// fakes implements the four ports of the order module and records the calls.
type fakes struct {
	available    bool
	inventoryErr error
	approved     bool
	paymentErr   error
	notifyErr    error
	announceErr  error

	stockChecks []string // productID
	charges     []string // currency
	notified    []order.PlacedOrder
	announced   []order.PlacedOrder
}

func (f *fakes) Available(_ context.Context, productID string, _ int) (bool, error) {
	f.stockChecks = append(f.stockChecks, productID)
	return f.available, f.inventoryErr
}

func (f *fakes) Charge(_ context.Context, _ string, _ float64, currency string) (bool, error) {
	f.charges = append(f.charges, currency)
	return f.approved, f.paymentErr
}

// notifier and announcer split the two ports that share a method name.
type notifier struct{ *fakes }

func (n notifier) OrderPlaced(_ context.Context, placed order.PlacedOrder) error {
	n.notified = append(n.notified, placed)
	return n.notifyErr
}

type announcer struct{ *fakes }

func (a announcer) OrderPlaced(_ context.Context, placed order.PlacedOrder) error {
	a.announced = append(a.announced, placed)
	return a.announceErr
}

func newService(t *testing.T, f *fakes) *order.Service {
	t.Helper()
	service, err := order.NewService(f, f, notifier{f}, announcer{f})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

var laptop = order.NewOrder{ProductID: "laptop", Quantity: 1, Amount: 999.99}

func TestServicePlaceOrder(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name          string
		fakes         fakes
		order         order.NewOrder
		wantStatus    order.Status
		wantErr       error
		wantCharges   int
		wantAnnounced int
	}{
		{name: "placed", fakes: fakes{available: true, approved: true}, order: laptop,
			wantStatus: order.StatusSuccess, wantCharges: 1, wantAnnounced: 1},
		{name: "out of stock: no charge, nothing announced", fakes: fakes{available: false, approved: true}, order: laptop,
			wantStatus: order.StatusOutOfStock},
		{name: "payment declined: nothing announced", fakes: fakes{available: true, approved: false}, order: laptop,
			wantStatus: order.StatusPaymentFailed, wantCharges: 1},
		{name: "inventory failure fails the order", fakes: fakes{inventoryErr: boom}, order: laptop,
			wantErr: boom},
		{name: "payment failure fails the order", fakes: fakes{available: true, paymentErr: boom}, order: laptop,
			wantErr: boom, wantCharges: 1},
		{name: "a failed notification does not fail the order", fakes: fakes{available: true, approved: true, notifyErr: boom}, order: laptop,
			wantStatus: order.StatusSuccess, wantCharges: 1, wantAnnounced: 1},
		{name: "a failed announcement does not fail the order", fakes: fakes{available: true, approved: true, announceErr: boom}, order: laptop,
			wantStatus: order.StatusSuccess, wantCharges: 1, wantAnnounced: 1},
		{name: "missing product", fakes: fakes{available: true, approved: true}, order: order.NewOrder{Quantity: 1, Amount: 1},
			wantErr: order.ErrInvalidOrder},
		{name: "non-positive quantity", fakes: fakes{available: true, approved: true}, order: order.NewOrder{ProductID: "laptop", Amount: 1},
			wantErr: order.ErrInvalidOrder},
		{name: "non-positive amount", fakes: fakes{available: true, approved: true}, order: order.NewOrder{ProductID: "laptop", Quantity: 1},
			wantErr: order.ErrInvalidOrder},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.fakes
			result, err := newService(t, &f).PlaceOrder(context.Background(), tc.order)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if result.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", result.Status, tc.wantStatus)
			}
			if tc.wantErr == nil && result.OrderID == "" {
				t.Error("an order that was processed must have an id")
			}
			if len(f.charges) != tc.wantCharges {
				t.Errorf("charges = %d, want %d", len(f.charges), tc.wantCharges)
			}
			if len(f.announced) != tc.wantAnnounced || len(f.notified) != tc.wantAnnounced {
				t.Errorf("notified = %d, announced = %d, want %d each", len(f.notified), len(f.announced), tc.wantAnnounced)
			}
			if errors.Is(tc.wantErr, order.ErrInvalidOrder) && len(f.stockChecks) != 0 {
				t.Error("an invalid order must not reach the inventory")
			}
		})
	}
}

func TestServicePlaceOrderTellsThePortsWhatWasPlaced(t *testing.T) {
	f := &fakes{available: true, approved: true}
	result, err := newService(t, f).PlaceOrder(context.Background(), laptop)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if len(f.charges) != 1 || f.charges[0] != "USD" {
		t.Errorf("an order without a currency is charged in USD, got %v", f.charges)
	}
	if len(f.notified) != 1 || len(f.announced) != 1 {
		t.Fatalf("notified = %v, announced = %v", f.notified, f.announced)
	}
	placed := f.announced[0]
	if placed != f.notified[0] {
		t.Errorf("both ports must be told about the same order: %+v vs %+v", f.notified[0], placed)
	}
	if placed.OrderID != result.OrderID || placed.ProductID != "laptop" || placed.Amount != 999.99 || placed.PlacedAt.IsZero() {
		t.Errorf("placed order = %+v, result = %+v", placed, result)
	}
}

func TestNewServiceRequiresEveryPort(t *testing.T) {
	f := &fakes{}
	if _, err := order.NewService(nil, f, notifier{f}, announcer{f}); err == nil {
		t.Error("expected an error for a missing inventory port")
	}
	if _, err := order.NewService(f, nil, notifier{f}, announcer{f}); err == nil {
		t.Error("expected an error for a missing payment port")
	}
	if _, err := order.NewService(f, f, nil, announcer{f}); err == nil {
		t.Error("expected an error for a missing notification port")
	}
	if _, err := order.NewService(f, f, notifier{f}, nil); err == nil {
		t.Error("expected an error for a missing events port")
	}
}
