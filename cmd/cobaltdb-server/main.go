package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)

func main() {
	var (
		dataDir     = flag.String("data", "./data", "data directory")
		address     = flag.String("addr", ":4200", "wire protocol address")
		mysqlAddr   = flag.String("mysql-addr", ":3307", "MySQL protocol address")
		enableMySQL = flag.Bool("mysql", true, "enable MySQL protocol")
		inMemory    = flag.Bool("memory", false, "use in-memory storage")
		cacheSize   = flag.Int("cache", 1024, "cache size in pages")
		authEnabled = flag.Bool("auth", false, "enable authentication")
		adminUser   = flag.String("admin-user", "admin", "default admin username")
		adminPass   = flag.String("admin-pass", "admin", "default admin password")
		tlsEnabled  = flag.Bool("tls", false, "enable TLS")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey      = flag.String("tls-key", "", "TLS key file")
		tlsGenCert  = flag.Bool("tls-gen-cert", false, "auto-generate self-signed TLS certificate")
	)
	flag.Parse()

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
	defer db.Close()

	log.Printf("CobaltDB v2.0 server starting...")
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

	// Create wire protocol server
	srv, err := server.New(db, &server.Config{
		Address:          *address,
		AuthEnabled:      *authEnabled,
		RequireAuth:      *authEnabled,
		DefaultAdminUser: *adminUser,
		DefaultAdminPass: *adminPass,
		TLS:              tlsConfig,
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start MySQL protocol server if enabled
	var mysqlSrv *protocol.MySQLServer
	if *enableMySQL {
		mysqlSrv = protocol.NewMySQLServer(db, "5.7.0-CobaltDB")
		if err := mysqlSrv.Listen(*mysqlAddr); err != nil {
			log.Fatalf("Failed to start MySQL protocol: %v", err)
		}
		log.Printf("MySQL protocol listening on: %s", *mysqlAddr)
	}

	if tlsConfig != nil && tlsConfig.Enabled {
		log.Printf("Wire protocol listening on: %s (TLS enabled)", *address)
	} else {
		log.Printf("Wire protocol listening on: %s", *address)
	}

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		if mysqlSrv != nil {
			mysqlSrv.Close()
		}
		srv.Close()
	}()

	// Start wire protocol server (blocks)
	if err := srv.Listen(*address, tlsConfig); err != nil {
		log.Printf("Server error: %v", err)
	}

	log.Println("Server stopped.")
}
