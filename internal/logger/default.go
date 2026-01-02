package logger

import (
	"log/slog"
	"os"

	"github.com/go-monolith/mono/pkg/types"
)

// NewDefaultLogger creates a default logger using slog.TextHandler.
//
// The default logger:
// - Uses text format
// - Writes to stdout
// - Log level: Info
// - No source location
func NewDefaultLogger() types.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	return &slogLogger{
		logger: slog.New(handler),
		output: os.Stdout,
	}
}
