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
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

const (
	maxPayloadBytes = 16 * 1024 * 1024
	// maxPayloadSize is the maximum allowed message payload size (16 MB)
	maxPayloadSize             uint32 = maxPayloadBytes
	maxWireSQLBytes                   = 10000
	maxWireInboundPayloadBytes        = 1024 * 1024
	maxWireAuthPayloadBytes           = 4 * 1024
	maxWireResultRows                 = 10000
	maxWireResultValueBytes           = 1024 * 1024
	maxWireParams                     = 1024
	maxWireParamBytes                 = 1024 * 1024
	maxWirePreparedStmts              = 1024
	maxServerTimeoutSeconds           = int64(1<<63-1) / int64(time.Second)
)

var (
	ErrServerClosed = errors.New("server is closed")
)

func messagePacketLength(payloadLen int) (uint32, error) {
	if payloadLen < 0 || payloadLen > maxPayloadBytes-1 {
		return 0, fmt.Errorf("payload too large: %d bytes", payloadLen)
	}
	return uint32(payloadLen + 1), nil // #nosec G115 - range checked above.
}

// Server represents a CobaltDB server
type Server struct {
	listener           net.Listener
	db                 *engine.DB
	clients            map[uint64]*ClientConn
	nextID             uint64
	mu                 sync.RWMutex
	closed             bool
	auth               *auth.Authenticator
	maxConnections     int
	readTimeout        time.Duration
	writeTimeout       time.Duration
	allowCleartextAuth bool
	sqlProtector       *SQLProtector  // Optional SQL injection protection
	clientWg           sync.WaitGroup // Tracks active client handler goroutines
	logger             *logger.Logger
}

// Config contains server configuration
type Config struct {
	Address            string
	AuthEnabled        bool
	RequireAuth        bool
	DefaultAdminUser   string
	DefaultAdminPass   string
	MaxConnections     int        // Maximum concurrent connections (0 = production default)
	ReadTimeout        int        // Read timeout in seconds (0 = 300s default)
	WriteTimeout       int        // Write timeout in seconds (0 = 60s default)
	TLS                *TLSConfig // TLS configuration (nil = disabled)
	AllowCleartextAuth bool       // Allow authenticated non-loopback listeners without TLS (development only)
	Logger             *logger.Logger
}

const defaultMaxConnections = 1000

// generateRandomPassword generates a 16-character random alphanumeric password
// using crypto/rand for secure random generation.
func generateRandomPassword() (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	for i := range b {
		b[i] = charset[b[i]%byte(len(charset))]
	}
	return string(b), nil
}

// DefaultConfig returns the default server configuration
func DefaultConfig() *Config {
	pass, _ := generateRandomPassword()
	return &Config{
		Address:          ":4200",
		AuthEnabled:      false,
		RequireAuth:      false,
		DefaultAdminUser: "admin",
		DefaultAdminPass: pass,
		MaxConnections:   defaultMaxConnections,
	}
}

// New creates a new server
func New(db *engine.DB, config *Config) (*Server, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if err := validateServerConfig(config); err != nil {
		return nil, err
	}

	authenticator := auth.NewAuthenticator()

	authEnabled := config.AuthEnabled || config.RequireAuth

	// Enable authentication if configured. RequireAuth is kept as an explicit
	// production safety switch for callers that distinguish config intent from
	// the lower-level authenticator state.
	if authEnabled {
		authenticator.Enable()

		// Create default admin user if specified
		if config.DefaultAdminUser != "" {
			if config.DefaultAdminPass == "" {
				return nil, fmt.Errorf("default admin password cannot be empty when authentication is enabled")
			}
			if err := authenticator.CreateUser(config.DefaultAdminUser, config.DefaultAdminPass, true); err != nil {
				return nil, fmt.Errorf("failed to create default admin user: %w", err)
			}
		}
	}

	readTimeout := timeoutSecondsOrDefault(config.ReadTimeout, 300*time.Second)
	writeTimeout := timeoutSecondsOrDefault(config.WriteTimeout, 60*time.Second)
	maxConnections := config.MaxConnections
	if maxConnections <= 0 {
		maxConnections = defaultMaxConnections
	}

	return &Server{
		db:                 db,
		clients:            make(map[uint64]*ClientConn),
		auth:               authenticator,
		maxConnections:     maxConnections,
		readTimeout:        readTimeout,
		writeTimeout:       writeTimeout,
		allowCleartextAuth: config.AllowCleartextAuth,
		logger:             config.Logger,
	}, nil
}

func validateServerConfig(config *Config) error {
	if config.MaxConnections < 0 {
		return fmt.Errorf("max connections must be non-negative: %d", config.MaxConnections)
	}
	if config.ReadTimeout < 0 {
		return fmt.Errorf("read timeout must be non-negative: %d", config.ReadTimeout)
	}
	if config.WriteTimeout < 0 {
		return fmt.Errorf("write timeout must be non-negative: %d", config.WriteTimeout)
	}
	if int64(config.ReadTimeout) > maxServerTimeoutSeconds {
		return fmt.Errorf("read timeout too large: %d seconds", config.ReadTimeout)
	}
	if int64(config.WriteTimeout) > maxServerTimeoutSeconds {
		return fmt.Errorf("write timeout too large: %d seconds", config.WriteTimeout)
	}
	return nil
}

func timeoutSecondsOrDefault(seconds int, fallback time.Duration) time.Duration {
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func validateWireSQL(sql string) *wire.ErrorMessage {
	if len(sql) > maxWireSQLBytes {
		return wire.NewErrorMessage(9, "query too large")
	}
	return nil
}

func validateWireParams(params []interface{}) *wire.ErrorMessage {
	if len(params) > maxWireParams {
		return wire.NewErrorMessage(9, "too many parameters")
	}
	for _, param := range params {
		switch v := param.(type) {
		case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			continue
		case string:
			if len(v) > maxWireParamBytes {
				return wire.NewErrorMessage(9, "parameter too large")
			}
		case []byte:
			if len(v) > maxWireParamBytes {
				return wire.NewErrorMessage(9, "parameter too large")
			}
		default:
			return wire.NewErrorMessage(9, "unsupported parameter type")
		}
	}
	return nil
}

func maxWireInboundPayloadFor(msgType wire.MsgType) int {
	switch msgType {
	case wire.MsgPing:
		return 0
	case wire.MsgAuth:
		return maxWireAuthPayloadBytes
	case wire.MsgQuery, wire.MsgPrepare, wire.MsgExecute:
		return maxWireInboundPayloadBytes
	default:
		return 0
	}
}

func (s *Server) logWarnf(format string, args ...interface{}) {
	if s != nil && s.logger != nil {
		s.logger.Warnf(format, args...)
	}
}

func (s *Server) logErrorf(format string, args ...interface{}) {
	if s != nil && s.logger != nil {
		s.logger.Errorf(format, args...)
	}
}

func validateServerAuthTransport(address string, authEnabled, tlsEnabled, allowCleartextAuth bool) error {
	if !authEnabled || tlsEnabled || allowCleartextAuth || isLoopbackServerListenAddress(address) {
		return nil
	}
	return fmt.Errorf("authentication without TLS is not allowed on non-loopback address %q; enable TLS, bind to loopback, or set AllowCleartextAuth for development", address)
}

func isLoopbackServerListenAddress(address string) bool {
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

func isTLSListener(listener net.Listener) bool {
	if listener == nil {
		return false
	}
	return reflect.TypeOf(listener).String() == "*tls.listener"
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
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrServerClosed
	}

	if s.auth.IsEnabled() && (tlsConfig == nil || !tlsConfig.Enabled) {
		s.logWarnf("authentication is enabled but TLS is disabled; passwords will be sent in cleartext")
	}
	if err := validateServerAuthTransport(address, s.auth.IsEnabled(), tlsConfig != nil && tlsConfig.Enabled, s.allowCleartextAuth); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Wrap with TLS if configured
	if tlsConfig != nil && tlsConfig.Enabled {
		tlsConf, err := LoadTLSConfig(tlsConfig)
		if err != nil {
			if closeErr := listener.Close(); closeErr != nil {
				return fmt.Errorf("failed to load TLS config: %w; listener close failed: %v", err, closeErr)
			}
			return fmt.Errorf("failed to load TLS config: %w", err)
		}
		listener = GetTLSListener(listener, tlsConf)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if err := listener.Close(); err != nil && !isBenignNetworkCloseError(err) {
			return fmt.Errorf("%w: listener close failed: %v", ErrServerClosed, err)
		}
		return ErrServerClosed
	}
	s.listener = listener
	s.mu.Unlock()
	return s.acceptLoop()
}

// ListenOnListener starts the server using an existing listener
func (s *Server) ListenOnListener(listener net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if listener != nil {
			if err := listener.Close(); err != nil && !isBenignNetworkCloseError(err) {
				return fmt.Errorf("%w: listener close failed: %v", ErrServerClosed, err)
			}
		}
		return ErrServerClosed
	}
	if listener == nil {
		s.mu.Unlock()
		return fmt.Errorf("listener cannot be nil")
	}
	if err := validateServerAuthTransport(listener.Addr().String(), s.auth.IsEnabled(), isTLSListener(listener), s.allowCleartextAuth); err != nil {
		s.mu.Unlock()
		if closeErr := listener.Close(); closeErr != nil && !isBenignNetworkCloseError(closeErr) {
			return fmt.Errorf("%w; listener close failed: %v", err, closeErr)
		}
		return err
	}
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
				if length, lenErr := messagePacketLength(len(payload)); lenErr == nil {
					buf := make([]byte, 4+1+len(payload))
					binary.LittleEndian.PutUint32(buf[:4], length)
					buf[4] = byte(wire.MsgError)
					copy(buf[5:], payload)
					_, _ = conn.Write(buf)
				}
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
				s.logWarnf("failed to enable keepalive: %v", err)
			}
			if err := tcpConn.SetKeepAlivePeriod(60 * time.Second); err != nil {
				s.logWarnf("failed to set keepalive period: %v", err)
			}
		}

		s.clientWg.Add(1)
		go func() {
			defer s.clientWg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.logErrorf("client handler recovered from panic: %v\n%v", r, debug.Stack())
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
	var closeErrs []error
	for _, client := range s.clients {
		if err := client.Conn.Close(); err != nil && !isBenignNetworkCloseError(err) {
			closeErrs = append(closeErrs, fmt.Errorf("close client %d: %w", client.ID, err))
		}
	}

	// Close listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil && !isBenignNetworkCloseError(err) {
			closeErrs = append(closeErrs, fmt.Errorf("close listener: %w", err))
		}
	}

	s.mu.Unlock()

	// Wait for all client handlers to finish (outside lock to avoid deadlock)
	s.clientWg.Wait()

	if s.auth != nil {
		s.auth.Stop()
	}

	return errors.Join(closeErrs...)
}

func isBenignNetworkCloseError(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
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
// preparedStmt holds a statement prepared via the wire protocol.
type preparedStmt struct {
	sql string
}

type ClientConn struct {
	ID            uint64
	Conn          net.Conn
	Server        *Server
	reader        *bufio.Reader
	username      string
	authed        bool
	ctx           context.Context
	cancel        context.CancelFunc
	preparedStmts map[uint32]*preparedStmt
	nextStmtID    uint32
	stmtMu        sync.Mutex
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

		if length < 1 {
			c.sendError(1, fmt.Sprintf("invalid message length: %d", length))
			return
		}
		if length > maxPayloadSize {
			c.sendError(1, "message too large")
			return // disconnect: payload bytes still on wire would desync the protocol
		}

		// Read message type (1 byte)
		msgType, err := c.reader.ReadByte()
		if err != nil {
			return // Connection error
		}

		payloadLen := int(length - 1)
		if payloadLen > maxWireInboundPayloadFor(wire.MsgType(msgType)) {
			c.sendError(1, "message payload too large")
			return // disconnect: payload bytes still on wire would desync the protocol
		}
		payload := make([]byte, payloadLen)
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
	if ctx == nil {
		ctx = context.Background()
	}

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

		return c.handlePrepare(ctx, &prepMsg)

	case wire.MsgExecute:
		if !c.authed {
			return wire.NewErrorMessage(6, "authentication required")
		}

		var execMsg wire.ExecuteMessage
		if err := wire.Decode(payload, &execMsg); err != nil {
			return wire.NewErrorMessage(2, "malformed message")
		}

		return c.handleExecute(ctx, &execMsg)

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
	// Fast-path: most SQL is already uppercase; avoid ToUpper allocation.
	action := firstWord
	for i := 0; i < len(action); i++ {
		if action[i] >= 'a' && action[i] <= 'z' {
			action = strings.ToUpper(action)
			break
		}
	}

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
	if errMsg := validateWireSQL(query.SQL); errMsg != nil {
		return errMsg
	}
	if errMsg := validateWireParams(query.Params); errMsg != nil {
		return errMsg
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
	// by checking the SQL prefix. Also blocks multi-statement attacks.
	sqlTrimmed := strings.TrimSpace(query.SQL)

	// Block multi-statement attacks (e.g., "SELECT; DROP TABLE users")
	if strings.Contains(sqlTrimmed, ";") {
		return wire.NewErrorMessage(9, "multi-statement queries are not allowed")
	}

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
			if len(resultRows) >= maxWireResultRows {
				return wire.NewErrorMessage(9, "result set too large")
			}
			row := make([]interface{}, len(columns))
			dest := make([]interface{}, len(columns))
			for i := range dest {
				dest[i] = &row[i]
			}

			if err := rows.Scan(dest...); err != nil {
				return wire.NewErrorMessage(5, sanitizeError(err))
			}
			if wireResultRowValueTooLarge(row) {
				return wire.NewErrorMessage(9, "result value too large")
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

// handlePrepare parses and caches a prepared statement, returning a statement ID.
func (c *ClientConn) handlePrepare(ctx context.Context, prep *wire.PrepareMessage) interface{} {
	if c.Server.db == nil {
		return wire.NewErrorMessage(1, "database not initialized")
	}
	if errMsg := validateWireSQL(prep.SQL); errMsg != nil {
		return errMsg
	}
	if !c.checkPermission(prep.SQL) {
		return wire.NewErrorMessage(8, "permission denied")
	}

	if _, err := query.ParseStrict(prep.SQL); err != nil {
		return wire.NewErrorMessage(4, sanitizeError(err))
	}

	c.stmtMu.Lock()
	defer c.stmtMu.Unlock()
	if c.preparedStmts == nil {
		c.preparedStmts = make(map[uint32]*preparedStmt)
	}
	if len(c.preparedStmts) >= maxWirePreparedStmts {
		return wire.NewErrorMessage(9, "too many prepared statements")
	}
	c.nextStmtID++
	stmtID := c.nextStmtID
	c.preparedStmts[stmtID] = &preparedStmt{sql: prep.SQL}

	return &wire.OKMessage{LastInsertID: 0, RowsAffected: 0, StmtID: stmtID}
}

// handleExecute looks up a prepared statement by ID and executes it with bound parameters.
func (c *ClientConn) handleExecute(ctx context.Context, exec *wire.ExecuteMessage) interface{} {
	if c.Server.db == nil {
		return wire.NewErrorMessage(1, "database not initialized")
	}

	c.stmtMu.Lock()
	ps, exists := c.preparedStmts[exec.StmtID]
	c.stmtMu.Unlock()
	if !exists {
		return wire.NewErrorMessage(4, fmt.Sprintf("prepared statement %d not found", exec.StmtID))
	}
	if errMsg := validateWireParams(exec.Params); errMsg != nil {
		return errMsg
	}

	if !c.checkPermission(ps.sql) {
		return wire.NewErrorMessage(8, "permission denied")
	}

	// Reuse handleQuery logic by constructing a QueryMessage
	qm := &wire.QueryMessage{SQL: ps.sql, Params: exec.Params}
	return c.handleQuery(ctx, qm)
}

func wireResultRowValueTooLarge(row []interface{}) bool {
	for _, value := range row {
		if wireResultValueSize(value) > maxWireResultValueBytes {
			return true
		}
	}
	return false
}

func wireResultValueSize(value interface{}) int {
	switch v := value.(type) {
	case nil:
		return 0
	case string:
		return len(v)
	case []byte:
		return len(v)
	case []interface{}:
		total := 0
		for _, item := range v {
			total += wireResultValueSize(item)
			if total > maxWireResultValueBytes {
				return total
			}
		}
		return total
	case map[string]interface{}:
		total := 0
		for key, item := range v {
			total += len(key) + wireResultValueSize(item)
			if total > maxWireResultValueBytes {
				return total
			}
		}
		return total
	case map[interface{}]interface{}:
		total := 0
		for key, item := range v {
			total += wireResultValueSize(key) + wireResultValueSize(item)
			if total > maxWireResultValueBytes {
				return total
			}
		}
		return total
	default:
		return len(fmt.Sprint(v))
	}
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

	length, err := messagePacketLength(len(payData))
	if err != nil {
		return err
	}

	packet := make([]byte, 5+len(payData))
	binary.LittleEndian.PutUint32(packet[:4], length)
	packet[4] = byte(msgType)
	copy(packet[5:], payData)
	if _, err := writeServerFull(c.Conn, packet); err != nil {
		return err
	}

	return nil
}

func writeServerFull(writer io.Writer, data []byte) (int, error) {
	n, err := writer.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, nil
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
