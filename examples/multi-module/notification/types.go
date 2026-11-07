package notification

import "time"

// NotificationLog represents a logged notification. It is internal to the
// module: the types other modules exchange with it live in the contract
// sub-package.
type NotificationLog struct {
	NotificationID string
	Channel        string // "service" or "event"
	Message        string
	Timestamp      time.Time
}
