# Best Practices for Go Framework Documentation on GitBook

A **distributed monolith** framework requires documentation that bridges two worlds: monolithic simplicity for developers getting started and distributed systems depth for production deployment. This guide synthesizes patterns from GoFiber, Gin, Echo, and industry-leading documentation frameworks to create a complete scaffold for your GitBook-based documentation.

## Complete folder structure for GitBook

The following structure adapts GoFiber's proven organization pattern to GitBook's requirements, optimized for a distributed monolith Go framework:

```
your-framework-official-docs/
├── README.md                        # 👋 Welcome (homepage)
├── SUMMARY.md                       # Navigation structure
├── .gitbook/
│   └── assets/                      # GitBook-managed images
├── getting-started/
│   ├── README.md                    # 🚀 Getting Started overview
│   ├── installation.md              # 📦 Installation
│   ├── quickstart.md                # ⚡ Quick Start
│   └── project-structure.md         # 📁 Project Structure
├── core-concepts/
│   ├── README.md                    # 🧠 Core Concepts overview
│   ├── distributed-monolith.md      # 🏗️ What is a Distributed Monolith
│   ├── architecture.md              # 📐 Architecture Overview
│   ├── modules.md                   # 📦 Modules & Boundaries
│   └── inter-module-communication.md             # 🔗 Inter-Module Communication
├── guide/
│   ├── README.md                    # 📖 Guide overview
│   ├── configuration.md             # ⚙️ Configuration
│   ├── routing.md                   # 🔌 Routing
│   ├── error-handling.md            # 🐛 Error Handling
│   ├── testing.md                   # 🧪 Testing
│   ├── logging.md                   # 📃 Logging
│   └── deployment.md                # 🚢 Deployment
├── api/
│   ├── README.md                    # 📚 API Reference overview
│   ├── app.md                       # 🚀 App
│   ├── context.md                   # 🧠 Context
│   ├── config.md                    # ⚙️ Config
│   ├── client.md                    # 🌎 Client
│   └── middleware/
│       ├── README.md                # 🧬 Middleware overview
│       ├── auth.md                  # Authentication
│       ├── cors.md                  # CORS
│       ├── logging.md               # Request Logging
│       ├── recovery.md              # Panic Recovery
│       ├── tracing.md               # Distributed Tracing
│       └── custom.md                # Creating Custom Middleware
├── distributed/
│   ├── README.md                    # 🌐 Distributed Features overview
│   ├── service-discovery.md         # 🔍 Service Discovery
│   ├── load-balancing.md            # ⚖️ Load Balancing
│   ├── circuit-breaker.md           # 🔌 Circuit Breaker
│   ├── saga.md                      # 🔄 Saga Patterns
│   └── observability.md             # 📊 Observability
├── recipes/
│   ├── README.md                    # 🍳 Recipes overview
│   ├── rest-api.md                  # Building a REST API
│   ├── grpc-service.md              # gRPC Service Integration
│   ├── event-driven.md              # Event-Driven Architecture
│   └── microservices-migration.md   # Migrating from Microservices
├── extra/
│   ├── README.md                    # 📎 Extra Resources
│   ├── faq.md                       # 🤔 FAQ
│   ├── benchmarks.md                # 📊 Benchmarks
│   ├── comparison.md                # ⚖️ Framework Comparison
│   └── contributing.md              # 🤝 Contributing
├── changelog.md                     # 📋 Changelog
└── assets/
    └── images/                      # Custom images
```

## GitBook SUMMARY.md navigation template

This structure follows GitBook's syntax with GoFiber-style emoji conventions:

```markdown
# Summary

## Getting Started
* [👋 Welcome](README.md)
* [🚀 Getting Started](getting-started/README.md)
    * [📦 Installation](getting-started/installation.md)
    * [⚡ Quick Start](getting-started/quickstart.md)
    * [📁 Project Structure](getting-started/project-structure.md)

## Core Concepts
* [🧠 Core Concepts](core-concepts/README.md)
    * [🏗️ Distributed Monolith](core-concepts/distributed-monolith.md)
    * [📐 Architecture](core-concepts/architecture.md)
    * [📦 Modules & Boundaries](core-concepts/modules.md)
    * [🔗 Communication](core-concepts/inter-module-communication.md)

## Guide
* [📖 Developer Guide](guide/README.md)
    * [⚙️ Configuration](guide/configuration.md)
    * [🔌 Routing](guide/routing.md)
    * [🐛 Error Handling](guide/error-handling.md)
    * [🧪 Testing](guide/testing.md)
    * [📃 Logging](guide/logging.md)
    * [🚢 Deployment](guide/deployment.md)

## API Reference
* [📚 API Reference](api/README.md)
    * [🚀 App](api/app.md)
    * [🧠 Context](api/context.md)
    * [⚙️ Config](api/config.md)
    * [🌎 Client](api/client.md)
    * [🧬 Middleware](api/middleware/README.md)
        * [Authentication](api/middleware/auth.md)
        * [CORS](api/middleware/cors.md)
        * [Request Logging](api/middleware/logging.md)
        * [Panic Recovery](api/middleware/recovery.md)
        * [Distributed Tracing](api/middleware/tracing.md)
        * [Custom Middleware](api/middleware/custom.md)

## Distributed Features
* [🌐 Distributed Features](distributed/README.md)
    * [🔍 Service Discovery](distributed/service-discovery.md)
    * [⚖️ Load Balancing](distributed/load-balancing.md)
    * [🔌 Circuit Breaker](distributed/circuit-breaker.md)
    * [🔄 Saga Patterns](distributed/saga.md)
    * [📊 Observability](distributed/observability.md)

## Recipes
* [🍳 Recipes](recipes/README.md)
    * [Building a REST API](recipes/rest-api.md)
    * [gRPC Service Integration](recipes/grpc-service.md)
    * [Event-Driven Architecture](recipes/event-driven.md)
    * [Migrating from Microservices](recipes/microservices-migration.md)

---

## Extra
* [🤔 FAQ](extra/faq.md)
* [📊 Benchmarks](extra/benchmarks.md)
* [⚖️ Comparison](extra/comparison.md)
* [🤝 Contributing](extra/contributing.md)
* [📋 Changelog](changelog.md)
```

## GitBook configuration file

Create `.gitbook.yaml` at your repository root:

```yaml
root: ./path/to/your-framework-official-docs/

structure:
  readme: README.md
  summary: SUMMARY.md

redirects:
  getting-started: getting-started/README.md
  api: api/README.md
  middleware: api/middleware/README.md
```

## Template files with placeholder content

### Welcome page (README.md)

```markdown
# 👋 Welcome

**YourFramework** is a Go framework for building distributed monolith applications that scale from startup to enterprise without the operational complexity of microservices.

{% hint style="info" %}
These docs are for **YourFramework v1.x**. For older versions, use the version selector.
{% endhint %}

## Why distributed monolith?

Traditional monoliths don't scale. Microservices are operationally expensive. A **distributed monolith** gives you the best of both worlds: modular boundaries with simplified deployment, strong typing across module boundaries, and the ability to scale individual components when needed.

## Installation

```bash
go get github.com/yourorg/yourframework
```

**Requirements:** Go 1.21 or higher

## Quick example

```go
package main

import "github.com/yourorg/yourframework"

func main() {
    app := yourframework.New()
    
    app.Module("users", func(m *yourframework.Module) {
        m.GET("/", listUsers)
        m.POST("/", createUser)
    })
    
    app.Run(":3000")
}
```

## Next steps

| If you want to...                    | Start here                                           |
| ------------------------------------ | ---------------------------------------------------- |
| Get up and running quickly           | [⚡ Quick Start](getting-started/quickstart.md)      |
| Understand the architecture          | [🏗️ Core Concepts](core-concepts/README.md)          |
| See working examples                 | [🍳 Recipes](recipes/README.md)                      |
| Look up API details                  | [📚 API Reference](api/README.md)                    |
```

### Quick Start template (getting-started/quickstart.md)

```markdown
# ⚡ Quick Start

Build your first distributed monolith application in under 5 minutes.

**Prerequisites:**
- Go 1.21 or higher installed
- Basic familiarity with Go

## Create a new project

```bash
mkdir myapp && cd myapp
go mod init myapp
go get github.com/yourorg/yourframework
```

## Write your first application

Create `main.go`:

```go
package main

import (
    "net/http"
    
    "github.com/yourorg/yourframework"
)

func main() {
    // Create a new application instance
    app := yourframework.New()
    
    // Define a module with its routes
    app.Module("api", func(m *yourframework.Module) {
        m.GET("/health", func(ctx *yourframework.Context) error {
            return ctx.JSON(http.StatusOK, map[string]string{
                "status": "healthy",
            })
        })
    })
    
    // Start the server
    app.Run(":3000")
}
```

## Run your application

```bash
go run main.go
```

Test it:

```bash
curl http://localhost:3000/api/health
# {"status":"healthy"}
```

{% hint style="success" %}
**Congratulations!** You've built your first distributed monolith application.
{% endhint %}

## What's next?

- Learn about [📦 Modules & Boundaries](../core-concepts/modules.md) to structure your application
- Add [🧬 Middleware](../api/middleware/README.md) for authentication and logging
- Explore [🌐 Distributed Features](../distributed/README.md) for scaling
```

### API reference template (api/app.md)

```markdown
# 🚀 App

The `App` struct is the main entry point for your application. It manages modules, middleware, and the application lifecycle.

## New

Creates a new application instance.

**Signature**

```go
func New(config ...Config) *App
```

**Example**

```go
// Default configuration
app := yourframework.New()

// Custom configuration
app := yourframework.New(yourframework.Config{
    AppName:      "MyApp",
    ReadTimeout:  10 * time.Second,
    WriteTimeout: 10 * time.Second,
})
```

## Module

Registers a new module with the application.

**Signature**

```go
func (app *App) Module(name string, fn func(*Module)) *Module
```

**Example**

```go
app.Module("users", func(m *yourframework.Module) {
    m.GET("/", listUsers)
    m.GET("/:id", getUser)
    m.POST("/", createUser)
})
```

## Use

Registers global middleware that runs on every request.

**Signature**

```go
func (app *App) Use(handlers ...Handler) *App
```

**Example**

```go
// Single middleware
app.Use(middleware.Logger())

// Multiple middleware
app.Use(
    middleware.Recover(),
    middleware.CORS(),
    middleware.Logger(),
)
```

## Run

Starts the HTTP server on the specified address.

**Signature**

```go
func (app *App) Run(addr string) error
```

**Example**

```go
// Listen on port 3000
app.Run(":3000")

// Listen on specific interface
app.Run("127.0.0.1:8080")
```

## Config

Configuration options for the application.

```go
type Config struct {
    // AppName is the application name used in logs and tracing.
    // Optional. Default: ""
    AppName string `json:"app_name"`
    
    // ReadTimeout is the maximum duration for reading the request.
    // Optional. Default: unlimited
    ReadTimeout time.Duration `json:"read_timeout"`
    
    // WriteTimeout is the maximum duration for writing the response.
    // Optional. Default: unlimited
    WriteTimeout time.Duration `json:"write_timeout"`
    
    // IdleTimeout is the maximum duration to wait for the next request.
    // Optional. Default: unlimited
    IdleTimeout time.Duration `json:"idle_timeout"`
    
    // DisableStartupMessage disables the startup banner.
    // Optional. Default: false
    DisableStartupMessage bool `json:"disable_startup_message"`
}
```

## Default Config

```go
var ConfigDefault = Config{
    AppName:               "",
    ReadTimeout:           0,
    WriteTimeout:          0,
    IdleTimeout:           0,
    DisableStartupMessage: false,
}
```
```

### Middleware template (api/middleware/cors.md)

```markdown
# CORS

Cross-Origin Resource Sharing (CORS) middleware for handling cross-origin requests.

## Signatures

```go
func New(config ...Config) yourframework.Handler
```

## Examples

```go
import "github.com/yourorg/yourframework/middleware/cors"

// Initialize with default config
app.Use(cors.New())

// Custom configuration
app.Use(cors.New(cors.Config{
    AllowOrigins:     []string{"https://example.com"},
    AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
    AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
    AllowCredentials: true,
    MaxAge:           86400,
}))
```

## Config

```go
type Config struct {
    // Next defines a function to skip this middleware when returned true.
    // Optional. Default: nil
    Next func(ctx *yourframework.Context) bool

    // AllowOrigins defines a list of origins that may access the resource.
    // Optional. Default: ["*"]
    AllowOrigins []string `json:"allow_origins"`

    // AllowMethods defines a list of methods allowed for cross-origin requests.
    // Optional. Default: ["GET", "POST", "HEAD", "PUT", "DELETE", "PATCH"]
    AllowMethods []string `json:"allow_methods"`

    // AllowHeaders defines a list of request headers that can be used.
    // Optional. Default: []
    AllowHeaders []string `json:"allow_headers"`

    // AllowCredentials indicates whether credentials are supported.
    // Optional. Default: false
    AllowCredentials bool `json:"allow_credentials"`

    // ExposeHeaders defines headers clients are allowed to access.
    // Optional. Default: []
    ExposeHeaders []string `json:"expose_headers"`

    // MaxAge indicates how long preflight results can be cached.
    // Optional. Default: 0
    MaxAge int `json:"max_age"`
}
```

## Default Config

```go
var ConfigDefault = Config{
    Next:             nil,
    AllowOrigins:     []string{"*"},
    AllowMethods:     []string{"GET", "POST", "HEAD", "PUT", "DELETE", "PATCH"},
    AllowHeaders:     []string{},
    AllowCredentials: false,
    ExposeHeaders:    []string{},
    MaxAge:           0,
}
```
```

### Recipe template (recipes/rest-api.md)

```markdown
# Building a REST API

Learn how to build a production-ready REST API with authentication, validation, and proper error handling.

**Time to complete:** 15 minutes

**What you'll learn:**
- Structuring modules for a REST API
- Adding authentication middleware
- Input validation
- Consistent error responses

## Project structure

```
myapi/
├── main.go
├── modules/
│   ├── users/
│   │   ├── module.go
│   │   ├── handlers.go
│   │   └── models.go
│   └── products/
│       ├── module.go
│       ├── handlers.go
│       └── models.go
└── middleware/
    └── auth.go
```

## Define your modules

Create `modules/users/module.go`:

```go
package users

import "github.com/yourorg/yourframework"

func Register(app *yourframework.App) {
    app.Module("users", func(m *yourframework.Module) {
        // Public routes
        m.POST("/login", Login)
        m.POST("/register", Register)
        
        // Protected routes
        protected := m.Group("/", middleware.Auth())
        protected.GET("/me", GetCurrentUser)
        protected.PUT("/me", UpdateCurrentUser)
    })
}
```

## Implement handlers

Create `modules/users/handlers.go`:

```go
package users

import (
    "net/http"
    
    "github.com/yourorg/yourframework"
)

type LoginRequest struct {
    Email    string `json:"email" validate:"required,email"`
    Password string `json:"password" validate:"required,min=8"`
}

func Login(ctx *yourframework.Context) error {
    var req LoginRequest
    if err := ctx.BodyParser(&req); err != nil {
        return ctx.Status(http.StatusBadRequest).JSON(ErrorResponse{
            Error: "Invalid request body",
        })
    }
    
    if err := ctx.Validate(req); err != nil {
        return ctx.Status(http.StatusBadRequest).JSON(ErrorResponse{
            Error:  "Validation failed",
            Fields: err.Fields(),
        })
    }
    
    // Authentication logic here...
    
    return ctx.JSON(http.StatusOK, TokenResponse{
        Token: token,
    })
}
```

## Wire it together

Update `main.go`:

```go
package main

import (
    "github.com/yourorg/yourframework"
    "github.com/yourorg/yourframework/middleware"
    
    "myapi/modules/users"
    "myapi/modules/products"
)

func main() {
    app := yourframework.New(yourframework.Config{
        AppName: "MyAPI",
    })
    
    // Global middleware
    app.Use(
        middleware.Recover(),
        middleware.Logger(),
        middleware.CORS(),
    )
    
    // Register modules
    users.Register(app)
    products.Register(app)
    
    app.Run(":3000")
}
```

## Test your API

```bash
# Register a user
curl -X POST http://localhost:3000/users/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"securepassword"}'

# Login
curl -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"securepassword"}'
```

{% hint style="success" %}
You now have a structured REST API with modular organization, authentication, and validation.
{% endhint %}

## Next steps

- Add [Distributed Tracing](../distributed/observability.md) for debugging
- Implement [Circuit Breaker](../distributed/circuit-breaker.md) for resilience
- Set up [Deployment](../guide/deployment.md) for production
```

## Emoji usage conventions

Following GoFiber's established patterns, apply emojis consistently:

| Emoji | Category | Usage |
|-------|----------|-------|
| 👋 | Welcome | Landing pages, introductions |
| 🚀 | Getting Started / App | Application-level content, launches |
| 📦 | Package / Installation | Installation, modules, bundled items |
| 🧠 | Context / Concepts | Context objects, core concepts, understanding |
| ⚡ | Quick Start / Performance | Speed, optimization, quick actions |
| 📖 | Guide | Developer guides, tutorials |
| 📚 | API Reference | API documentation, comprehensive references |
| 🧬 | Middleware | Middleware, extensions, plugins |
| ⚙️ | Configuration | Settings, config, options |
| 🔌 | Routing / Integration | Routes, connections, integrations |
| 🐛 | Error Handling | Debugging, errors, troubleshooting |
| 🧪 | Testing | Tests, validation, quality |
| 📃 | Logging | Logs, output, records |
| 🚢 | Deployment | Deploy, ship, release |
| 🌐 | Distributed | Distributed features, networking |
| 🔍 | Discovery / Search | Service discovery, finding things |
| ⚖️ | Balance / Comparison | Load balancing, comparisons |
| 🔄 | Patterns / Sync | Saga patterns, synchronization |
| 📊 | Metrics / Benchmarks | Observability, performance data |
| 🍳 | Recipes | Practical examples, cookbook |
| 🤔 | FAQ | Questions, help |
| 🤝 | Contributing | Community, collaboration |
| 📋 | Changelog | Version history, updates |
| 🌎 | Client | HTTP clients, external calls |
| 📁 | Structure | Project structure, folders |
| 🏗️ | Architecture | System design, building |
| 🔗 | Communication | Links, inter-module calls |

**Consistency rules from GoFiber:**
- Page titles in navigation **always** include emojis
- Category headers in sidebar include emojis
- Individual middleware pages typically **omit** emojis (use plain names)
- Section headers within pages typically **omit** emojis
- Use emojis to create visual hierarchy and quick scanning

## Writing style guide

### Voice and tone

Write documentation that is **conversational but professional**. Address the reader directly as "you" and explain not just *what* to do but *why*.

**Do:**
- "Configure the timeout to prevent hanging requests."
- "You can customize middleware order to match your authentication flow."

**Don't:**
- "The timeout should be configured." (passive)
- "One may wish to customize..." (impersonal)

### Code examples follow Go conventions

Every code example should be **runnable** and follow standard Go formatting:

```go
// Standard library imports first, blank line, then third-party
import (
    "net/http"
    "time"
    
    "github.com/yourorg/yourframework"
)

// Comments explain why, not what
// Configure aggressive timeouts for microservice communication
app := yourframework.New(yourframework.Config{
    ReadTimeout:  5 * time.Second,
    WriteTimeout: 10 * time.Second,
})
```

### Structural patterns for different page types

| Page Type | Structure | Emoji in Title |
|-----------|-----------|----------------|
| **Tutorial** | Overview → Prerequisites → Steps → Verify → Next | Yes |
| **How-to Guide** | Goal → Steps → Troubleshooting | Yes |
| **API Reference** | Signature → Example → Config → Defaults | Yes (category only) |
| **Concept** | Overview → Background → How it Works → When to Use | Yes |
| **Middleware** | Signatures → Examples → Config → Default Config | No |

### GitBook-specific markdown to use

**Hints for callouts:**
```markdown
{% hint style="info" %}
General information or tips.
{% endhint %}

{% hint style="warning" %}
Important warnings or gotchas.
{% endhint %}

{% hint style="danger" %}
Breaking changes or critical warnings.
{% endhint %}
```

**Tabs for platform-specific content:**
```markdown
{% tabs %}
{% tab title="Linux/macOS" %}
```bash
export APP_ENV=production
./myapp
```
{% endtab %}
{% tab title="Windows" %}
```powershell
$env:APP_ENV="production"
.\myapp.exe
```
{% endtab %}
{% endtabs %}
```

### Progressive disclosure strategy

Structure content using the **Diátaxis framework** with three complexity layers:

1. **Layer 1 (Getting Started)**: Minimum viable understanding—installation, hello world, basic concepts
2. **Layer 2 (Guide)**: Common workflows—configuration, routing, middleware, testing
3. **Layer 3 (Distributed/Advanced)**: Expert content—saga patterns, circuit breakers, custom tracing

Connect layers with clear "Next steps" sections and use expandable sections for advanced options within beginner content.

## godoc integration patterns

Ensure your framework's Go code includes proper godoc comments that complement your GitBook documentation:

```go
// Package yourframework provides a framework for building distributed
// monolith applications in Go.
//
// A distributed monolith combines modular boundaries with simplified
// deployment, allowing teams to scale components independently while
// maintaining strong typing across module boundaries.
//
// # Getting Started
//
// Create a new application and register modules:
//
//     app := yourframework.New()
//     app.Module("users", func(m *yourframework.Module) {
//         m.GET("/", listUsers)
//     })
//     app.Run(":3000")
//
// For complete documentation, see https://docs.yourframework.io
package yourframework
```

Link from your GitBook documentation to pkg.go.dev:
```markdown
For complete API details, see the [Go package documentation](https://pkg.go.dev/github.com/yourorg/yourframework).
```

## Changelog format

Use Keep a Changelog format in `changelog.md`:

```markdown
# 📋 Changelog

All notable changes to this project are documented here.

## [1.2.0] - 2025-01-15

### Added
- Circuit breaker middleware with configurable thresholds
- Support for gRPC inter-module communication

### Changed
- **Breaking:** `Module.Call()` now returns `(Response, error)` instead of `Response`

### Fixed
- Race condition in service discovery refresh

## [1.1.0] - 2024-12-01

_If upgrading from 1.0.x: see [Migration Guide](guide/migration-1.1.md)_

### Added
- Distributed tracing with OpenTelemetry support
- Health check endpoints for all modules

### Deprecated
- `app.Legacy()` method, use `app.Module()` instead
```

## SEO and discoverability checklist

Apply these practices for better search visibility:

- **URL structure**: Use lowercase, hyphenated paths (`/getting-started/quick-start`)
- **Titles**: Include primary keywords, keep under 60 characters
- **Headings**: Use descriptive H2-H4 structure, include keywords naturally
- **Internal linking**: Link concepts on first mention, use descriptive anchor text
- **Meta descriptions**: Write unique 160-character descriptions per page
- **FAQs**: Include FAQ sections for common questions (helps AI-powered search)

## Key implementation recommendations

This documentation structure follows proven patterns that work well for Go frameworks on GitBook. **Start with the getting-started section and one complete module example**—developers judge documentation quality within the first 5 minutes. The distributed features section differentiates your framework from standard web frameworks and should clearly explain the "why" behind the distributed monolith approach.

For maintenance, keep documentation close to code by using the same repository or a dedicated `docs/` folder with CI that validates links and builds on every PR. Run `godoc` locally to ensure your code comments and GitBook documentation tell a consistent story.