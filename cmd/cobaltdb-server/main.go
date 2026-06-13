package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	cblogger "github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)

// generateRandomPassword generates a secure random password
func generateRandomPassword(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate secure random password: %w", err)
	}
	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b), nil
}

var version = "dev"

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		dataDir     = flag.String("data", "./data", "data directory")
		address     = flag.String("addr", "127.0.0.1:4200", "wire protocol address")
		mysqlAddr   = flag.String("mysql-addr", "127.0.0.1:3307", "MySQL protocol address")
		enableMySQL = flag.Bool("mysql", true, "enable MySQL protocol")
		inMemory    = flag.Bool("memory", false, "use in-memory storage")
		cacheSize   = flag.Int("cache", 1024, "cache size in pages")
		authEnabled = flag.Bool("auth", true, "enable authentication")
		adminUser   = flag.String("admin-user", "admin", "default admin username")
		adminPass   = flag.String("admin-pass", "", "admin password (generated securely if not set)")
		tlsEnabled  = flag.Bool("tls", false, "enable TLS")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey      = flag.String("tls-key", "", "TLS key file")
		tlsGenCert  = flag.Bool("tls-gen-cert", false, "auto-generate self-signed TLS certificate")

		// Production features
		healthAddr           = flag.String("health-addr", "127.0.0.1:8420", "health check HTTP address")
		enableHealthServer   = flag.Bool("health-server", true, "enable health check HTTP server")
		enableCircuitBreaker = flag.Bool("circuit-breaker", true, "enable circuit breaker")
		enableRetry          = flag.Bool("retry", true, "enable retry logic")
		allowRemoteMetrics   = flag.Bool("remote-metrics", false, "allow Prometheus metrics endpoint from non-loopback clients")
		adminToken           = flag.String("admin-token", "", "admin API bearer token for protected health server endpoints")
		allowCleartextAuth   = flag.Bool("allow-cleartext-auth", false, "allow authenticated non-loopback listeners without encrypted transport")
		shutdownTimeout      = flag.Duration("shutdown-timeout", 30*time.Second, "graceful shutdown timeout")
		drainTimeout         = flag.Duration("drain-timeout", 10*time.Second, "connection drain timeout")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("CobaltDB Server %s\n", version)
		os.Exit(0)
	}

	if err := applyEnvOverrides(
		dataDir,
		address,
		mysqlAddr,
		enableMySQL,
		inMemory,
		cacheSize,
		authEnabled,
		tlsEnabled,
		tlsCert,
		tlsKey,
		tlsGenCert,
		healthAddr,
		enableHealthServer,
		enableCircuitBreaker,
		enableRetry,
		allowRemoteMetrics,
		adminToken,
		allowCleartextAuth,
		shutdownTimeout,
		drainTimeout,
	); err != nil {
		log.Fatalf("Invalid environment configuration: %v", err)
	}

	// Override admin credentials from environment variables if set.
	if envUser := os.Getenv("COBALTDB_ADMIN_USER"); envUser != "" {
		*adminUser = envUser
	}
	if envPass := os.Getenv("COBALTDB_ADMIN_PASSWORD"); envPass != "" {
		*adminPass = envPass
	}

	// Secure-by-default authentication behavior.
	if *authEnabled {
		if *adminPass == "" {
			generated, err := generateRandomPassword(20)
			if err != nil {
				log.Fatalf("Failed to generate random admin password: %v", err)
			}
			*adminPass = generated
			log.Printf("[SECURITY] Auth enabled. Admin user: %s (password generated securely, not displayed)", *adminUser)
			log.Printf("[SECURITY] Set -admin-pass or COBALTDB_ADMIN_PASSWORD to use a fixed secret.")
		}
	} else {
		log.Printf("[SECURITY WARNING] Authentication is disabled (-auth=false). Do not expose this server publicly.")
	}

	if err := validateAdminCredentials(*authEnabled, *adminUser, *adminPass); err != nil {
		log.Fatalf("Invalid security configuration: %v", err)
	}
	if err := validateAuthTransport(*address, *mysqlAddr, *authEnabled, *tlsEnabled, *enableMySQL, *allowCleartextAuth); err != nil {
		log.Fatalf("Invalid security configuration: %v", err)
	}
	if *authEnabled && *allowCleartextAuth {
		log.Printf("[SECURITY WARNING] Cleartext authentication was explicitly allowed. Use this only for local development or trusted private networks.")
	}
	serverLogger := cblogger.New(cblogger.InfoLevel, os.Stderr)

	// Open database
	opts := &engine.Options{
		CoreStorage: engine.CoreStorage{
			CacheSize:  *cacheSize,
			InMemory:   *inMemory,
			WALEnabled: engine.BoolPtr(!*inMemory),
		},
	}

	var dbPath string
	if *inMemory {
		dbPath = ":memory:"
	} else {
		cleanDataDir, err := prepareDataDir(*dataDir)
		if err != nil {
			log.Fatalf("Failed to create data directory: %v", err)
		}
		*dataDir = cleanDataDir
		dbPath = filepath.Join(cleanDataDir, "cobalt.cb")
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	log.Printf("CobaltDB %s Production Server starting...", version)
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
			Logger:               serverLogger,
		},
		CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
		Retry:                engine.DefaultRetryConfig(),
		HealthAddr:           *healthAddr,
		EnableCircuitBreaker: *enableCircuitBreaker,
		EnableRetry:          *enableRetry,
		EnableHealthServer:   *enableHealthServer,
		AllowRemoteMetrics:   *allowRemoteMetrics,
		AdminToken:           *adminToken,
		Logger:               serverLogger,
	}

	prodServer := server.NewProductionServer(db, prodConfig)

	finalAdminUser := *adminUser
	finalAdminPass := *adminPass

	// Create wire protocol server
	srv, err := server.New(db, &server.Config{
		Address:            *address,
		AuthEnabled:        *authEnabled,
		RequireAuth:        *authEnabled,
		DefaultAdminUser:   finalAdminUser,
		DefaultAdminPass:   finalAdminPass,
		TLS:                tlsConfig,
		AllowCleartextAuth: *allowCleartextAuth,
		Logger:             serverLogger,
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
			mysqlSrv.SetAllowCleartextAuth(*allowCleartextAuth)
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

func applyEnvOverrides(
	dataDir *string,
	address *string,
	mysqlAddr *string,
	enableMySQL *bool,
	inMemory *bool,
	cacheSize *int,
	authEnabled *bool,
	tlsEnabled *bool,
	tlsCert *string,
	tlsKey *string,
	tlsGenCert *bool,
	healthAddr *string,
	enableHealthServer *bool,
	enableCircuitBreaker *bool,
	enableRetry *bool,
	allowRemoteMetrics *bool,
	adminToken *string,
	allowCleartextAuth *bool,
	shutdownTimeout *time.Duration,
	drainTimeout *time.Duration,
) error {
	envString("COBALTDB_DATA_DIR", dataDir)
	envString("COBALTDB_STORAGE_DATA_DIR", dataDir)
	envString("COBALTDB_ADDR", address)
	envString("COBALTDB_SERVER_ADDR", address)
	envString("COBALTDB_MYSQL_ADDR", mysqlAddr)
	envString("COBALTDB_HEALTH_ADDR", healthAddr)
	envString("COBALTDB_TLS_CERT_FILE", tlsCert)
	envString("COBALTDB_TLS_KEY_FILE", tlsKey)
	envString("COBALTDB_ADMIN_TOKEN", adminToken)

	if err := envBool("COBALTDB_MYSQL_ENABLED", enableMySQL); err != nil {
		return err
	}
	if err := envBool("COBALTDB_IN_MEMORY", inMemory); err != nil {
		return err
	}
	if err := envInt("COBALTDB_CACHE_SIZE", cacheSize); err != nil {
		return err
	}
	if err := envBool("COBALTDB_AUTH_ENABLED", authEnabled); err != nil {
		return err
	}
	if err := envBool("COBALTDB_SECURITY_AUTH_ENABLED", authEnabled); err != nil {
		return err
	}
	if err := envBool("COBALTDB_TLS_ENABLED", tlsEnabled); err != nil {
		return err
	}
	if err := envBool("COBALTDB_TLS_GEN_CERT", tlsGenCert); err != nil {
		return err
	}
	if err := envBool("COBALTDB_HEALTH_SERVER_ENABLED", enableHealthServer); err != nil {
		return err
	}
	if err := envBool("COBALTDB_CIRCUIT_BREAKER_ENABLED", enableCircuitBreaker); err != nil {
		return err
	}
	if err := envBool("COBALTDB_RETRY_ENABLED", enableRetry); err != nil {
		return err
	}
	if err := envBool("COBALTDB_REMOTE_METRICS_ENABLED", allowRemoteMetrics); err != nil {
		return err
	}
	if err := envBool("COBALTDB_ALLOW_CLEARTEXT_AUTH", allowCleartextAuth); err != nil {
		return err
	}
	if err := envDuration("COBALTDB_SHUTDOWN_TIMEOUT", shutdownTimeout); err != nil {
		return err
	}
	if err := envDuration("COBALTDB_DRAIN_TIMEOUT", drainTimeout); err != nil {
		return err
	}

	return nil
}

func prepareDataDir(path string) (string, error) {
	cleanPath := filepath.Clean(strings.TrimSpace(path))
	if cleanPath == "" || cleanPath == "." {
		return "", fmt.Errorf("data directory must be explicit")
	}
	if err := rejectDataDirSymlinkPathComponents(cleanPath); err != nil {
		return "", err
	}

	info, statErr := os.Lstat(cleanPath)
	preexisting := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return "", fmt.Errorf("failed to stat data directory: %w", statErr)
	}
	if preexisting {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("data directory must not be a symlink: %s", cleanPath)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("data path must be a directory: %s", cleanPath)
		}
	}

	if err := os.MkdirAll(cleanPath, 0750); err != nil {
		return "", err
	}
	if err := rejectDataDirSymlinkPathComponents(cleanPath); err != nil {
		return "", err
	}
	if err := os.Chmod(cleanPath, 0750); err != nil {
		return "", fmt.Errorf("failed to set data directory permissions: %w", err)
	}

	openedInfo, err := os.Stat(cleanPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat data directory after create: %w", err)
	}
	if !openedInfo.IsDir() {
		return "", fmt.Errorf("data path must be a directory: %s", cleanPath)
	}
	if preexisting && !os.SameFile(info, openedInfo) {
		return "", fmt.Errorf("data directory changed while opening: %s", cleanPath)
	}

	return cleanPath, nil
}

func rejectDataDirSymlinkPathComponents(path string) error {
	path = filepath.Clean(path)
	if path == "." || path == string(os.PathSeparator) {
		return nil
	}

	current := "."
	if filepath.IsAbs(path) {
		current = string(os.PathSeparator)
		path = strings.TrimPrefix(path, string(os.PathSeparator))
	}

	for _, part := range strings.Split(path, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to stat data directory component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("data directory component must not be a symlink: %s", current)
		}
	}
	return nil
}

func validateAuthTransport(wireAddr, mysqlAddr string, authEnabled, tlsEnabled, mysqlEnabled, allowCleartextAuth bool) error {
	if !authEnabled || allowCleartextAuth {
		return nil
	}
	if !tlsEnabled && !isLoopbackListenAddress(wireAddr) {
		return fmt.Errorf("authentication without TLS is not allowed on non-loopback wire address %q; enable TLS, bind to loopback, or set -allow-cleartext-auth for development", wireAddr)
	}
	if mysqlEnabled && !isLoopbackListenAddress(mysqlAddr) {
		return fmt.Errorf("MySQL authentication on non-loopback address %q is cleartext; bind MySQL to loopback, disable MySQL, or set -allow-cleartext-auth for development", mysqlAddr)
	}
	return nil
}

func validateAdminCredentials(authEnabled bool, adminUser, adminPass string) error {
	if !authEnabled {
		return nil
	}
	if strings.TrimSpace(adminUser) == "" {
		return fmt.Errorf("admin username cannot be empty when authentication is enabled")
	}
	if adminPass == "" {
		return fmt.Errorf("admin password cannot be empty when authentication is enabled")
	}
	if adminUser == "admin" && adminPass == "admin" {
		return fmt.Errorf("default admin credentials are not allowed; set -admin-pass or COBALTDB_ADMIN_PASSWORD to a unique secret")
	}
	return nil
}

func isLoopbackListenAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func envString(name string, target *string) {
	if value := os.Getenv(name); value != "" {
		*target = value
	}
}

func envBool(name string, target *bool) error {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	*target = parsed
	return nil
}

func envInt(name string, target *int) error {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s must be an integer: %w", name, err)
	}
	if parsed <= 0 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	*target = parsed
	return nil
}

func envDuration(name string, target *time.Duration) error {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s must be a duration: %w", name, err)
	}
	if parsed <= 0 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	*target = parsed
	return nil
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
	if w.server.GetAuthenticator().IsEnabled() && (w.tls == nil || !w.tls.Enabled) {
		log.Println("WARNING: Authentication is enabled but TLS is disabled. Passwords will be sent in cleartext.")
	}

	listener, err := net.Listen("tcp", w.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.addr, err)
	}

	if w.tls != nil && w.tls.Enabled {
		tlsConf, err := server.LoadTLSConfig(w.tls)
		if err != nil {
			if closeErr := listener.Close(); closeErr != nil {
				return fmt.Errorf("failed to load TLS config: %w; listener close failed: %v", err, closeErr)
			}
			return fmt.Errorf("failed to load TLS config: %w", err)
		}
		listener = server.GetTLSListener(listener, tlsConf)
	}

	go func() {
		if err := w.server.ListenOnListener(listener); err != nil {
			log.Printf("Wire server error: %v", err)
		}
	}()
	return nil
}

func (w *WireServerComponent) Stop(ctx context.Context) error {
	return w.server.Close()
}

func (w *WireServerComponent) Health() server.HealthStatus {
	if w.server == nil {
		return server.HealthStatus{Healthy: false, Message: "server not initialized"}
	}
	return server.HealthStatus{
		Healthy: true,
		Message: fmt.Sprintf("wire server running, %d clients", w.server.ClientCount()),
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
	return m.server.Close()
}

func (m *MySQLServerComponent) Health() server.HealthStatus {
	return server.HealthStatus{
		Healthy: true,
		Message: "MySQL protocol server running",
	}
}
