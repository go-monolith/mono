package container

import (
	"github.com/go-monolith/mono/pkg/types"
)

// New creates a new ServiceContainer instance.
// This is the public factory function for creating containers.
//
// Example:
//
//	logger := types.NewLogger(os.Stdout)
//	container := container.New(logger)
//	container.BindModule(myModule)
func New(logger types.Logger) types.ServiceContainer {
	return NewServiceContainer(logger)
}
