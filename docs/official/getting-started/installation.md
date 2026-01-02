# Installation

This guide walks you through installing and setting up the Monolith Framework in your Go project.

## Prerequisites

- **Go 1.25 or higher** - [Download Go](https://golang.org/dl/)
- **A Go project** or a new directory to create one

## Step 1: Create a New Go Project (Optional)

If you don't have a Go project yet, create one:

```bash
mkdir my-mono-app
cd my-mono-app
go mod init github.com/yourname/my-mono-app
```

## Step 2: Install the Framework

Add the Monolith Framework to your project:

```bash
go get github.com/go-monolith/mono/v1
```

This command:
- Downloads the framework and its dependencies
- Updates your `go.mod` and `go.sum` files
- Installs NATS.io (embedded NATS server)
- Installs other required dependencies

## Step 3: Verify Installation

Create a simple `main.go` file to verify the installation:

```go
package main

import (
    "context"
    "log"

    "github.com/go-monolith/mono/v1"
)

type HelloModule struct{}

func (m *HelloModule) Name() string                 { return "hello" }
func (m *HelloModule) Start(context.Context) error { return nil }
func (m *HelloModule) Stop(context.Context) error  { return nil }

func main() {
    app, err := mono.NewMonoApplication()
    if err != nil {
        log.Fatal(err)
    }

    app.Register(&HelloModule{})

    if err := app.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    app.Stop(context.Background())
}
```

Run it:

```bash
go run main.go
```

You should see:

```
INFO [main] app/framework.go:XXX framework starting
INFO [main] app/nats.go:XXX NATS server started
...
INFO [main] app/framework.go:XXX framework started
INFO [main] app/framework.go:XXX framework stopping
```

## What's Installed?

The framework installation includes:

| Component | Purpose |
|-----------|---------|
| **Monolith Framework Core** | Module management, lifecycle, DI container |
| **NATS Server** | Embedded message broker for inter-module communication |
| **JetStream** | Optional persistence layer for messages |
| **Built-in Middleware** | Access logging, audit trails, request IDs |
| **Plugins** | File storage (fs-jetstream), Key-value storage (kv-jetstream) |

## Module Import

Import the framework using:

```go
import "github.com/go-monolith/mono/v1"
```

The import path follows Go's versioning convention. The `/v1` suffix ensures you get version 1.x releases.

## Troubleshooting

### Port Conflict: "address already in use"

The NATS server listens on port 4222 by default. If you get an "address already in use" error:

**Option 1:** Stop the service using that port
```bash
# macOS/Linux: Find process using port 4222
lsof -i :4222
# Kill the process
kill -9 <PID>
```

**Option 2:** Use a different port in your code
```go
app, err := mono.NewMonoApplication(
    mono.WithNATSPort(4223),
)
```

### Module Not Recognized

Make sure you implement the `Module` interface correctly:

```go
// Must implement all three methods
func (m *MyModule) Name() string                 { return "my-module" }
func (m *MyModule) Start(context.Context) error { return nil }
func (m *MyModule) Stop(context.Context) error  { return nil }
```

### Import Errors

If you see import errors, ensure you're using the correct import path:

```go
// Correct
import "github.com/go-monolith/mono/v1"

// Wrong (missing /v1)
import "github.com/go-monolith/mono"
```

## Next Steps

Now that you have the framework installed, try:

1. **[Quick Start Tutorial](quickstart.md)** - Build your first module with logging
2. **[Project Structure](project-structure.md)** - Learn how to organize your code
3. **[Core Concepts](../core-concepts/README.md)** - Understand modules and communication patterns

## Getting Help

- Review [examples](../../../examples/basic/README.md) for working code
- Check the [API reference](https://pkg.go.dev/github.com/go-monolith/mono/v1)
- Visit [GitHub](https://github.com/go-monolith/mono) for issues and discussions
