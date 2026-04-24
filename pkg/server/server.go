package server

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

const (
	// maxPayloadSize is the maximum allowed message payload size (16 MB)
	maxPayloadSize uint32 = 16 * 1024 * 1024
)

var (
	ErrServerClosed = errors.New("server is closed")
)

// Server represents a CobaltDB server
type Server struct {
	listener       net.Listener
	db             *engine.DB
	clients        map[uint64]*ClientConn
	nextID         uint64
	mu             sync.RWMutex
	closed         bool
	auth           *auth.Authenticator
	maxConnections int
	readTimeout    time.Duration
	writeTimeout   time.Duration
	sqlProtector   *SQLProtector  // Optional SQL injection protection
	clientWg       sync.WaitGroup // Tracks active client handler goroutines
}

// Config contains server configuration
type Config struct {
	Address          string
	AuthEnabled      bool
	RequireAuth      bool
	DefaultAdminUser string
	DefaultAdminPass string
	MaxConnections   int        // Maximum concurrent connections (0 = unlimited)
	ReadTimeout      int        // Read timeout in seconds (0 = 300s default)
	WriteTimeout     int        // Write timeout in seconds (0 = 60s default)
	TLS              *TLSConfig // TLS configuration (nil = disabled)
}

// generateRandomPassword generates a 16-character random alphanumeric password
// using crypto/rand for secure random generation.
func generateRandomPassword() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback should never happen with crypto/rand, but be safe
		panic("crypto/rand failed: " + err.Error())
	}
	for i := range b {
		b[i] = charset[b[i]%byte(len(charset))]
	}
	return string(b)
}

// DefaultConfig returns the default server configuration
func DefaultConfig() *Config {
	pass := generateRandomPassword()
	return &Config{
		Address:          ":4200",
		AuthEnabled:      false,
		RequireAuth:      false,
		DefaultAdminUser: "admin",
		DefaultAdminPass: pass,
	}
}

// New creates a new server
func New(db *engine.DB, config *Config) (*Server, error) {
	if config == nil {
		config = DefaultConfig()
	}

	authenticator := auth.NewAuthenticator()

	// Enable authentication if configured
	if config.AuthEnabled {
		authenticator.Enable()

		// Create default admin user if specified
		if config.DefaultAdminUser != "" {
			if err := authenticator.CreateUser(config.DefaultAdminUser, config.DefaultAdminPass, true); err != nil {
				return nil, fmt.Errorf("failed to create default admin user: %w", err)
			}
		}
	}

	readTimeout := time.Duration(config.ReadTimeout) * time.Second
	if readTimeout == 0 {
		readTimeout = 300 * time.Second // 5 minutes default
	}
	writeTimeout := time.Duration(config.WriteTimeout) * time.Second
	if writeTimeout == 0 {
		writeTimeout = 60 * time.Second // 1 minute default
	}

	return &Server{
		db:             db,
		clients:        make(map[uint64]*ClientConn),
		auth:           authenticator,
		maxConnections: config.MaxConnections,
		readTimeout:    readTimeout,
		writeTimeout:   writeTimeout,
	}, nil
}

// GetAuthenticator returns the server's authenticator instance.
func (s *Server) GetAuthenticator() *auth.Authenticator {
	return s.auth
}

// SetSQLProtector sets the SQL injection protector for the server
func (s *Server) SetSQLProtector(sp *SQLProtector) {
	s.sqlProtector = sp
}

// Listen starts the server on the given address
func (s *Server) Listen(address string, tlsConfig *TLSConfig) error {
	if s.auth.IsEnabled() && (tlsConfig == nil || !tlsConfig.Enabled) {
		fmt.Println("WARNING: Authentication is enabled but TLS is disabled. Passwords will be sent in cleartext.")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Wrap with TLS if configured
	if tlsConfig != nil && tlsConfig.Enabled {
		tlsConf, err := LoadTLSConfig(tlsConfig)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("failed to load TLS config: %w", err)
		}
		listener = GetTLSListener(listener, tlsConf)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return s.acceptLoop()
}

// ListenOnListener starts the server using an existing listener
func (s *Server) ListenOnListener(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return s.acceptLoop()
}

// acceptLoop accepts incoming connections.
//
// Register the loop itself in s.clientWg *before* accepting any connections.
// This gives Close()'s s.clientWg.Wait() a non-zero counter to synchronize
// against, so later per-connection s.clientWg.Add(1) calls can't race with
// Wait (the classic "positive-delta-while-zero-concurrent-with-Wait"
// WaitGroup race). Snapshot the listener while we hold the lock.
func (s *Server) acceptLoop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	listener := s.listener
	s.clientWg.Add(1)
	s.mu.Unlock()
	defer s.clientWg.Done()

	if listener == nil {
		return nil
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.RLock()
			closed := s.closed
			s.mu.RUnlock()
			if closed {
				return nil
			}
			return err
		}

		s.mu.Lock()
		// Check max connections
		if s.maxConnections > 0 && len(s.clients) >= s.maxConnections {
			s.mu.Unlock()
			// Best-effort: notify client why the connection is being refused.
			errMsg := wire.NewErrorMessage(10, "max connections reached")
			if payload, encErr := wire.Encode(errMsg); encErr == nil {
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				length := uint32(1 + len(payload))
				buf := make([]byte, 4+1+len(payload))
				buf[0] = byte(length)
				buf[1] = byte(length >> 8)
				buf[2] = byte(length >> 16)
				buf[3] = byte(length >> 24)
				buf[4] = byte(wire.MsgError)
				copy(buf[5:], payload)
				_, _ = conn.Write(buf)
			}
			_ = conn.Close()
			continue
		}
		s.nextID++
		clientID := s.nextID
		client := &ClientConn{
			ID:     clientID,
			Conn:   conn,
			Server: s,
			reader: bufio.NewReader(conn),
			authed: !s.auth.IsEnabled(), // Auto-authenticate if auth is disabled
		}
		s.clients[clientID] = client
		s.mu.Unlock()

		// Set TCP keepalive
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				fmt.Printf("failed to enable keepalive: %v\n", err)
			}
			if err := tcpConn.SetKeepAlivePeriod(60 * time.Second); err != nil {
				fmt.Printf("failed to set keepalive period: %v\n", err)
			}
		}

		s.clientWg.Add(1)
		go func() {
			defer s.clientWg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("[PANIC] client handler recovered: %v\n", r)
				}
			}()
			client.Handle()
		}()
	}
}

// Close closes the server and waits for all client handlers to finish
func (s *Server) Close() error {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return nil
	}

	s.closed = true

	// Close all client connections
	for _, client := range s.clients {
		_ = client.Conn.Close()
	}

	// Close listener
	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.mu.Unlock()

	// Wait for all client handlers to finish (outside lock to avoid deadlock)
	s.clientWg.Wait()

	if s.auth != nil {
		s.auth.Stop()
	}

	return nil
}

// ClientCount returns the current number of connected clients
func (s *Server) ClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// removeClient removes a client connection
func (s *Server) removeClient(id uint64) {
	s.mu.Lock()
	delete(s.clients, id)
	s.mu.Unlock()
}

// ClientConn represents a client connection
type ClientConn struct {
	ID       uint64
	Conn     net.Conn
	Server   *Server
	reader   *bufio.Reader
	username string
	authed   bool
	ctx      context.Context
	cancel   context.CancelFunc
}

// Handle handles client requests
func (c *ClientConn) Handle() {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	defer func() {
		c.cancel() // cancel any in-flight queries on disconnect
		_ = c.Conn.Close()
		c.Server.removeClient(c.ID)
	}()

	for {
		// Set read deadline for idle timeout
		if err := c.Conn.SetReadDeadline(time.Now().Add(c.Server.readTimeout)); err != nil {
			return
		}

		// Read message length (4 bytes)
		var length uint32
		if err := binary.Read(c.reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF || errors.Is(err, net.ErrClosed) {
				return
			}
			// Timeout or connection error - stop handling
			return
		}

		// Read message type (1 byte)
		msgType, err := c.reader.ReadByte()
		if err != nil {
			return // Connection error
		}

		// Read payload
		if length < 1 {
			c.sendError(1, fmt.Sprintf("invalid message length: %d", length))
			return
		}
		if length > maxPayloadSize {
			c.sendError(1, "message too large")
			return // disconnect: payload bytes still on wire would desync the protocol
		}
		payload := make([]byte, length-1)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return // Connection error
		}

		// Handle message
		response := c.handleMessage(wire.MsgType(msgType), payload)

		// Send response
		if err := c.sendMessage(response); err != nil {
			return
		}
	}
}

// handleMessage handles a single message
func (c *ClientConn) handleMessage(msgType wire.MsgType, payload []byte) interface{} {
	ctx := c.ctx

	switch msgType {
	case wire.MsgPing:
		return wire.MsgPong

	case wire.MsgAuth:
		var authMsg wire.AuthMessage
		if err := wire.Decode(payload, &authMsg); err != nil {
			return wire.NewErrorMessage(2, "malformed message")
		}
		return c.handleAuth(&authMsg)

	case wire.MsgQuery:
		if !c.authed {
			return wire.NewErrorMessage(6, "authentication required")
		}

		var query wire.QueryMessage
		if err := wire.Decode(payload, &query); err != nil {
			return wire.NewErrorMessage(2, "malformed message")
		}

		return c.handleQuery(ctx, &query)

	case wire.MsgPrepare:
		if !c.authed {
			return wire.NewErrorMessage(6, "authentication required")
		}

		var prepMsg wire.PrepareMessage
		if err := wire.Decode(payload, &prepMsg); err != nil {
			return wire.NewErrorMessage(2, "malformed message")
		}

		// Prepare is essentially the same as query in our engine (uses cached prepared stmts)
		return wire.NewOKMessage(0, 0)

	case wire.MsgExecute:
		if !c.authed {
			return wire.NewErrorMessage(6, "authentication required")
		}

		var execMsg wire.ExecuteMessage
		if err := wire.Decode(payload, &execMsg); err != nil {
			return wire.NewErrorMessage(2, "malformed message")
		}

		return wire.NewErrorMessage(3, "prepared statement execution not yet supported via wire protocol")

	default:
		return wire.NewErrorMessage(3, fmt.Sprintf("unknown message type: %d", msgType))
	}
}

// handleAuth handles authentication
func (c *ClientConn) handleAuth(authMsg *wire.AuthMessage) interface{} {
	token, err := c.Server.auth.Authenticate(authMsg.Username, authMsg.Password)
	if err != nil {
		return wire.NewErrorMessage(7, "invalid credentials")
	}

	c.username = authMsg.Username
	c.authed = true

	return wire.NewAuthSuccessMessage(token)
}

// checkPermission checks if the authenticated user has permission for the operation
func (c *ClientConn) checkPermission(sql string) bool {
	// If auth is disabled, allow all
	if !c.Server.auth.IsEnabled() {
		return true
	}
	// If user is not authenticated, deny
	if !c.authed {
		return false
	}

	user, err := c.Server.auth.GetUser(c.username)
	if err != nil {
		return false
	}

	if user.IsAdmin {
		return true
	}

	// Extract first SQL keyword to determine required permission
	// Using first word only prevents multi-statement bypass (e.g. "SELECT 1;DROP TABLE")
	sqlTrimmed := strings.TrimSpace(sql)
	firstWord := sqlTrimmed
	if idx := strings.IndexAny(sqlTrimmed, " \t\n\r("); idx > 0 {
		firstWord = sqlTrimmed[:idx]
	}
	action := strings.ToUpper(firstWord)

	switch action {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER":
		// valid action
	default:
		return false // Unknown operations denied by default for safety
	}

	// Check permission (using empty database/table for now - would need proper parsing)
	return c.Server.auth.HasPermission(c.username, "", "", action)
}

// handleQuery handles a query message
func (c *ClientConn) handleQuery(ctx context.Context, query *wire.QueryMessage) interface{} {
	// Check if database is initialized
	if c.Server.db == nil {
		return wire.NewErrorMessage(1, "database not initialized")
	}

	// Check permissions
	if !c.checkPermission(query.SQL) {
		return wire.NewErrorMessage(8, "permission denied")
	}

	// SQL injection protection
	if sp := c.Server.sqlProtector; sp != nil {
		result := sp.CheckSQL(query.SQL)
		if result.Blocked {
			return wire.NewErrorMessage(9, "query blocked by SQL protection")
		}
	}

	// Determine if this is a query (returns rows) or exec (returns affected count)
	// by checking the SQL prefix to avoid parsing twice
	sqlTrimmed := strings.TrimSpace(query.SQL)
	isQuery := len(sqlTrimmed) >= 4 && ((len(sqlTrimmed) >= 6 && strings.EqualFold(sqlTrimmed[:6], "SELECT")) ||
		strings.EqualFold(sqlTrimmed[:4], "WITH") ||
		strings.EqualFold(sqlTrimmed[:4], "SHOW") ||
		(len(sqlTrimmed) >= 7 && strings.EqualFold(sqlTrimmed[:7], "EXPLAIN")) ||
		(len(sqlTrimmed) >= 8 && strings.EqualFold(sqlTrimmed[:8], "DESCRIBE")))

	if isQuery {
		rows, err := c.Server.db.Query(ctx, query.SQL, query.Params...)
		if err != nil {
			return wire.NewErrorMessage(4, sanitizeError(err))
		}
		defer rows.Close()

		columns := rows.Columns()
		var resultRows [][]interface{}

		for rows.Next() {
			row := make([]interface{}, len(columns))
			dest := make([]interface{}, len(columns))
			for i := range dest {
				dest[i] = &row[i]
			}

			if err := rows.Scan(dest...); err != nil {
				return wire.NewErrorMessage(5, sanitizeError(err))
			}

			resultRows = append(resultRows, row)
		}

		return wire.NewResultMessage(columns, resultRows)
	}

	// Non-query statement (INSERT, UPDATE, DELETE, CREATE, etc.)
	result, err := c.Server.db.Exec(ctx, query.SQL, query.Params...)
	if err != nil {
		return wire.NewErrorMessage(4, sanitizeError(err))
	}

	return wire.NewOKMessage(result.LastInsertID, result.RowsAffected)
}

// sendMessage sends a message to the client
func (c *ClientConn) sendMessage(msg interface{}) error {
	var msgType wire.MsgType
	var payload interface{}

	switch m := msg.(type) {
	case wire.MsgType:
		msgType = m
		payload = nil
	case *wire.ResultMessage:
		msgType = wire.MsgResult
		payload = m
	case *wire.OKMessage:
		msgType = wire.MsgOK
		payload = m
	case *wire.ErrorMessage:
		msgType = wire.MsgError
		payload = m
	case *wire.AuthSuccessMessage:
		msgType = wire.MsgAuthSuccess
		payload = m
	default:
		return fmt.Errorf("unknown message type: %T", msg)
	}

	// Encode payload
	var payData []byte
	var err error
	if payload != nil {
		payData, err = wire.Encode(payload)
		if err != nil {
			// Encoding failed — send an error message to the client instead of
			// silently dropping the response (which would leave the client hanging).
			if msgType != wire.MsgError {
				fallbackErr := wire.NewErrorMessage(5, "internal: failed to encode response")
				return c.sendMessage(fallbackErr)
			}
			// If even the error message can't be encoded, give up.
			return fmt.Errorf("failed to encode error message: %w", err)
		}
	}

	// Set write deadline
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.Server.writeTimeout)); err != nil {
		return err
	}

	// Write length
	length := uint32(1 + len(payData))
	if err := binary.Write(c.Conn, binary.LittleEndian, length); err != nil {
		return err
	}

	// Write message type
	if err := binary.Write(c.Conn, binary.LittleEndian, msgType); err != nil {
		return err
	}

	// Write payload
	if len(payData) > 0 {
		if _, err := c.Conn.Write(payData); err != nil {
			return err
		}
	}

	return nil
}

// sendError sends an error message
func (c *ClientConn) sendError(code int, message string) {
	if err := c.sendMessage(wire.NewErrorMessage(code, message)); err != nil {
		_ = err
	}
}

// sanitizeError strips internal details from errors before sending to clients.
// It preserves SQL-level errors (syntax, constraint, etc.) but removes
// file paths, stack traces, and internal component names.
func sanitizeError(err error) string {
	msg := err.Error()
	// Remove file paths (Unix and Windows)
	for _, prefix := range []string{"/", "C:\\", "D:\\"} {
		if idx := strings.Index(msg, prefix); idx >= 0 {
			// Truncate at the path
			msg = msg[:idx] + "(internal error)"
		}
	}
	return msg
}
