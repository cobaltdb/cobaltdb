package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)


// generateRandomPassword generates a secure random password
func generateRandomPassword(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		// Fallback to time-based if crypto/rand fails
		for i := range b {
			b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		}
	} else {
		for i := range b {
			b[i] = charset[int(b[i])%len(charset)]
		}
	}
	return string(b)
}

func main() {
	var (
		dataDir      = flag.String("data", "./data", "data directory")
		address      = flag.String("addr", ":4200", "wire protocol address")
		mysqlAddr    = flag.String("mysql-addr", ":3307", "MySQL protocol address")
		enableMySQL  = flag.Bool("mysql", true, "enable MySQL protocol")
		inMemory     = flag.Bool("memory", false, "use in-memory storage")
		cacheSize    = flag.Int("cache", 1024, "cache size in pages")
		authEnabled  = flag.Bool("auth", false, "enable authentication")
		adminUser    = flag.String("admin-user", "admin", "default admin username")
		adminPass    = flag.String("admin-pass", "", "admin password (required if auth enabled, random generated if not set)")
		tlsEnabled   = flag.Bool("tls", false, "enable TLS")
		tlsCert      = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey       = flag.String("tls-key", "", "TLS key file")
		tlsGenCert   = flag.Bool("tls-gen-cert", false, "auto-generate self-signed TLS certificate")

		// Production features
		healthAddr           = flag.String("health-addr", ":8420", "health check HTTP address")
		enableHealthServer   = flag.Bool("health-server", true, "enable health check HTTP server")
		enableCircuitBreaker = flag.Bool("circuit-breaker", true, "enable circuit breaker")
		enableRetry          = flag.Bool("retry", true, "enable retry logic")
		shutdownTimeout      = flag.Duration("shutdown-timeout", 30*time.Second, "graceful shutdown timeout")
		drainTimeout         = flag.Duration("drain-timeout", 10*time.Second, "connection drain timeout")
	)
	flag.Parse()

	// FIX-007: Check for default admin credentials when auth is enabled
	if *authEnabled {
		if *adminPass == "" {
			log.Fatal("ERROR: Admin password must be set when auth is enabled (use -admin-pass flag or COBALTDB_ADMIN_PASSWORD env var)")
		}
	} else if *adminPass == "" {
		// Generate random password when auth is disabled and no password set
		*adminPass = generateRandomPassword(16)
		log.Printf("[INFO] Generated random admin password: %s", *adminPass)
	}

	// Open database
	opts := &engine.Options{
		CacheSize:  *cacheSize,
		InMemory:   *inMemory,
		WALEnabled: !*inMemory,
	}

	var dbPath string
	if *inMemory {
		dbPath = ":memory:"
	} else {
		// Ensure data directory exists
		if err := os.MkdirAll(*dataDir, 0755); err != nil {
			log.Fatalf("Failed to create data directory: %v", err)
		}
		dbPath = fmt.Sprintf("%s/cobalt.cb", *dataDir)
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	log.Printf("CobaltDB v2.2.0 Production Server starting...")
	if !*inMemory {
		log.Printf("Data directory: %s", *dataDir)
	} else {
		log.Printf("Mode: in-memory")
	}

	// Setup TLS configuration
	var tlsConfig *server.TLSConfig
	if *tlsEnabled {
		tlsConfig = &server.TLSConfig{
			Enabled:            true,
			CertFile:           *tlsCert,
			KeyFile:            *tlsKey,
			GenerateSelfSigned: *tlsGenCert,
		}
		if *tlsGenCert {
			log.Println("TLS: Auto-generating self-signed certificate")
		}
	}

	// Create production server
	prodConfig := &server.ProductionConfig{
		Lifecycle: &server.LifecycleConfig{
			ShutdownTimeout:      *shutdownTimeout,
			DrainTimeout:         *drainTimeout,
			HealthCheckInterval:  5 * time.Second,
			StartupTimeout:       60 * time.Second,
			EnableSignalHandling: true,
		},
		CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
		Retry:                engine.DefaultRetryConfig(),
		HealthAddr:           *healthAddr,
		EnableCircuitBreaker: *enableCircuitBreaker,
		EnableRetry:          *enableRetry,
		EnableHealthServer:   *enableHealthServer,
	}

	prodServer := server.NewProductionServer(db, prodConfig)

	// Override admin credentials from environment variables if set
	finalAdminUser := *adminUser
	finalAdminPass := *adminPass
	if envUser := os.Getenv("COBALTDB_ADMIN_USER"); envUser != "" {
		finalAdminUser = envUser
	}
	if envPass := os.Getenv("COBALTDB_ADMIN_PASSWORD"); envPass != "" {
		finalAdminPass = envPass
	}

	// Warn if using default credentials with auth enabled
	if *authEnabled && finalAdminPass == "admin" {
		log.Println("WARNING: Using default admin credentials is insecure. Set COBALTDB_ADMIN_PASSWORD environment variable.")
	}

	// Create wire protocol server
	srv, err := server.New(db, &server.Config{
		Address:          *address,
		AuthEnabled:      *authEnabled,
		RequireAuth:      *authEnabled,
		DefaultAdminUser: finalAdminUser,
		DefaultAdminPass: finalAdminPass,
		TLS:              tlsConfig,
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Register wire server as a lifecycle component
	wireComponent := &WireServerComponent{
		server: srv,
		addr:   *address,
		tls:    tlsConfig,
	}
	prodServer.Lifecycle.RegisterComponent(wireComponent)

	// Start MySQL protocol server if enabled
	var mysqlComponent *MySQLServerComponent
	if *enableMySQL {
		mysqlSrv := protocol.NewMySQLServer(db, "5.7.0-CobaltDB")
		// Share the wire server's authenticator so both protocols use the same user store
		if *authEnabled {
			mysqlSrv.SetAuthenticator(srv.GetAuthenticator())
		}
		mysqlComponent = &MySQLServerComponent{
			server: mysqlSrv,
			addr:   *mysqlAddr,
		}
		prodServer.Lifecycle.RegisterComponent(mysqlComponent)
	}

	// Start production server
	if err := prodServer.Start(); err != nil {
		log.Fatalf("Failed to start production server: %v", err)
	}

	// Print startup information
	if tlsConfig != nil && tlsConfig.Enabled {
		log.Printf("Wire protocol listening on: %s (TLS enabled)", *address)
	} else {
		log.Printf("Wire protocol listening on: %s", *address)
	}
	if *enableMySQL {
		log.Printf("MySQL protocol listening on: %s", *mysqlAddr)
	}
	if *enableHealthServer {
		log.Printf("Health server listening on: %s", *healthAddr)
		log.Printf("Health endpoints: /health, /ready, /healthz")
	}
	log.Printf("Server is ready. Press Ctrl+C to shutdown gracefully.")

	// Wait for shutdown signal
	prodServer.Wait()

	// Close database engine (flushes WAL, checkpoints, releases resources)
	if err := db.Close(); err != nil {
		log.Printf("Warning: database close error: %v", err)
	}

	log.Println("Server stopped.")
}

// WireServerComponent wraps the wire protocol server as a lifecycle component
type WireServerComponent struct {
	server *server.Server
	addr   string
	tls    *server.TLSConfig
}

func (w *WireServerComponent) Name() string {
	return "wire-server"
}

func (w *WireServerComponent) Start(ctx context.Context) error {
	// Start in a goroutine since Listen blocks
	go func() {
		if err := w.server.Listen(w.addr, w.tls); err != nil {
			log.Printf("Wire server error: %v", err)
		}
	}()
	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)
	return nil
}

func (w *WireServerComponent) Stop(ctx context.Context) error {
	return w.server.Close()
}

func (w *WireServerComponent) Health() server.HealthStatus {
	// Wire server is healthy if it has clients or just started
	return server.HealthStatus{
		Healthy: true,
		Message: "wire server running",
	}
}

// MySQLServerComponent wraps the MySQL protocol server as a lifecycle component
type MySQLServerComponent struct {
	server *protocol.MySQLServer
	addr   string
}

func (m *MySQLServerComponent) Name() string {
	return "mysql-server"
}

func (m *MySQLServerComponent) Start(ctx context.Context) error {
	if err := m.server.Listen(m.addr); err != nil {
		return err
	}
	return nil
}

func (m *MySQLServerComponent) Stop(ctx context.Context) error {
	m.server.Close()
	return nil
}

func (m *MySQLServerComponent) Health() server.HealthStatus {
	return server.HealthStatus{
		Healthy: true,
		Message: "MySQL protocol server running",
	}
}

