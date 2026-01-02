package logger

import (
	"log/slog"

	"github.com/go-monolith/mono/pkg/types"
)

// toSlogLevel converts types.LogLevel to slog.Level.
func toSlogLevel(level types.LogLevel) slog.Level {
	switch level {
	case types.LogLevelDebug:
		return slog.LevelDebug
	case types.LogLevelInfo:
		return slog.LevelInfo
	case types.LogLevelWarn:
		return slog.LevelWarn
	case types.LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo // Default to Info for unknown levels
	}
}

// fromSlogLevel converts slog.Level to types.LogLevel.
func fromSlogLevel(level slog.Level) types.LogLevel {
	switch level {
	case slog.LevelDebug:
		return types.LogLevelDebug
	case slog.LevelInfo:
		return types.LogLevelInfo
	case slog.LevelWarn:
		return types.LogLevelWarn
	case slog.LevelError:
		return types.LogLevelError
	default:
		return types.LogLevelInfo
	}
}
