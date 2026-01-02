package notification

import (
	"context"
	"time"
)

// NotificationAdapterPort provides a type-safe interface for sending notifications
type NotificationAdapterPort interface {
	SendOnOrderCreatedNotification(ctx context.Context, notification *OrderCreatedNotification) error
}

// OrderCreatedNotification represents a notification about an order
type OrderCreatedNotification struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// NotificationLog represents a logged notification
type NotificationLog struct {
	NotificationID string
	Channel        string // "service" or "event"
	Message        string
	Timestamp      time.Time
}
