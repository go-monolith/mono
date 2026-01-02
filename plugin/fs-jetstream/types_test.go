package fsjetstream

import (
	"testing"
)

func TestObjectNotFoundError(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantMsg string
	}{
		{
			name:    "simple key",
			key:     "test-object",
			wantMsg: "object not found: test-object",
		},
		{
			name:    "key with path",
			key:     "folder/subfolder/file.txt",
			wantMsg: "object not found: folder/subfolder/file.txt",
		},
		{
			name:    "empty key",
			key:     "",
			wantMsg: "object not found: ",
		},
		{
			name:    "key with special characters",
			key:     "key-with_special.chars:2024",
			wantMsg: "object not found: key-with_special.chars:2024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := objectNotFoundError(tt.key)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantMsg {
				t.Errorf("objectNotFoundError() = %q, want %q", err.Error(), tt.wantMsg)
			}
		})
	}
}
