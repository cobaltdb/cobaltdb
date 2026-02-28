package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)

func main() {
	var (
		dataDir = flag.String("data", "./data", "data directory")
		address = flag.String("addr", ":4200", "server address")
		inMemory = flag.Bool("memory", false, "use in-memory storage")
		cacheSize = flag.Int("cache", 1024, "cache size in pages")
	)
	flag.Parse()

	// Open database
	opts := &engine.Options{
		CacheSize: *cacheSize,
		InMemory:  *inMemory,
		WALEnabled: !*inMemory,
	}

	var dbPath string
	if *inMemory {
		dbPath = ":memory:"
	} else {
		dbPath = fmt.Sprintf("%s/cobalt.cb", *dataDir)
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	log.Printf("CobaltDB server starting...")
	log.Printf("Data directory: %s", *dataDir)
	log.Printf("Listening on: %s", *address)

	// Create server
	srv, err := server.New(db, &server.Config{
		Address: *address,
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		srv.Close()
	}()

	// Start server
	if err := srv.Listen(*address); err != nil {
		log.Printf("Server error: %v", err)
	}
}
