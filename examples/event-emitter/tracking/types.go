package tracking

// OrderCreatedEvent is the event payload emitted when an order is created
type OrderCreatedEvent struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	ProductID  string  `json:"product_id"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
}

// OrderShippedEvent is the event payload emitted when an order is shipped
type OrderShippedEvent struct {
	OrderID     string `json:"order_id"`
	TrackingNum string `json:"tracking_num"`
	Carrier     string `json:"carrier"`
}
