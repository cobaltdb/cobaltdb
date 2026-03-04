package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

var (
	ErrServerClosed = errors.New("server is closed")
)

// Server represents a CobaltDB server
type Server struct {
	listener net.Listener
	db       *engine.DB
	clients  map[uint64]*ClientConn
	nextID   uint64
	mu       sync.RWMutex
	closed   bool
	auth     *auth.Authenticator
}

// Config contains server configuration
type Config struct {
	Address          string
	AuthEnabled      bool
	RequireAuth      bool
	DefaultAdminUser string
	DefaultAdminPass string
}

// DefaultConfig returns the default server configuration
func DefaultConfig() *Config {
	return &Config{
		Address:          ":4200",
		AuthEnabled:      false,
		RequireAuth:      false,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "admin",
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
			authenticator.CreateUser(config.DefaultAdminUser, config.DefaultAdminPass, true)
		}
	}

	return &Server{
		db:      db,
		clients: make(map[uint64]*ClientConn),
		auth:    authenticator,
	}, nil
}

// Listen starts the server
func (s *Server) Listen(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = listener
	return s.acceptLoop()
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed {
				return nil
			}
			return err
		}

		s.mu.Lock()
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

		go client.Handle()
	}
}

// Close closes the server
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	// Close all client connections
	for _, client := range s.clients {
		client.Conn.Close()
	}

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	return nil
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
}

// Handle handles client requests
func (c *ClientConn) Handle() {
	defer func() {
		c.Conn.Close()
		c.Server.removeClient(c.ID)
	}()

	for {
		// Read message length (4 bytes)
		var length uint32
		if err := binary.Read(c.reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				return
			}
			c.sendError(1, err.Error())
			continue
		}

		// Read message type (1 byte)
		msgType, err := c.reader.ReadByte()
		if err != nil {
			c.sendError(1, err.Error())
			continue
		}

		// Read payload
		if length < 1 {
			c.sendError(1, fmt.Sprintf("invalid message length: %d", length))
			continue
		}
		payload := make([]byte, length-1)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			c.sendError(1, err.Error())
			continue
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
	ctx := context.Background()

	switch msgType {
	case wire.MsgPing:
		return wire.MsgPong

	case wire.MsgAuth:
		var authMsg wire.AuthMessage
		if err := wire.Decode(payload, &authMsg); err != nil {
			return wire.NewErrorMessage(2, err.Error())
		}
		return c.handleAuth(&authMsg)

	case wire.MsgQuery:
		// Check if authentication is required
		if !c.authed {
			return wire.NewErrorMessage(6, "authentication required")
		}

		var query wire.QueryMessage
		if err := wire.Decode(payload, &query); err != nil {
			return wire.NewErrorMessage(2, err.Error())
		}

		return c.handleQuery(ctx, &query)

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
	// If auth is disabled or user is admin, allow all
	if !c.Server.auth.IsEnabled() || c.authed == false {
		return true
	}

	user, err := c.Server.auth.GetUser(c.username)
	if err != nil {
		return false
	}

	if user.IsAdmin {
		return true
	}

	// Parse SQL to determine required permission
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	var action string
	switch {
	case strings.HasPrefix(sqlUpper, "SELECT"):
		action = "SELECT"
	case strings.HasPrefix(sqlUpper, "INSERT"):
		action = "INSERT"
	case strings.HasPrefix(sqlUpper, "UPDATE"):
		action = "UPDATE"
	case strings.HasPrefix(sqlUpper, "DELETE"):
		action = "DELETE"
	case strings.HasPrefix(sqlUpper, "CREATE"):
		action = "CREATE"
	case strings.HasPrefix(sqlUpper, "DROP"):
		action = "DROP"
	case strings.HasPrefix(sqlUpper, "ALTER"):
		action = "ALTER"
	default:
		return true // Unknown operations allowed by default
	}

	// Check permission (using empty database/table for now - would need proper parsing)
	return c.Server.auth.HasPermission(c.username, "", "", action)
}

// handleQuery handles a query message
func (c *ClientConn) handleQuery(ctx context.Context, query *wire.QueryMessage) interface{} {
	// Check permissions
	if !c.checkPermission(query.SQL) {
		return wire.NewErrorMessage(8, "permission denied")
	}

	// Try as query first
	rows, err := c.Server.db.Query(ctx, query.SQL, query.Params...)
	if err == nil {
		defer rows.Close()

		// Collect results
		columns := rows.Columns()
		var resultRows [][]interface{}

		for rows.Next() {
			row := make([]interface{}, len(columns))
			dest := make([]interface{}, len(columns))
			for i := range dest {
				dest[i] = &row[i]
			}

			if err := rows.Scan(dest...); err != nil {
				return wire.NewErrorMessage(5, err.Error())
			}

			resultRows = append(resultRows, row)
		}

		return wire.NewResultMessage(columns, resultRows)
	}

	// Try as exec
	result, err := c.Server.db.Exec(ctx, query.SQL, query.Params...)
	if err != nil {
		return wire.NewErrorMessage(4, err.Error())
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
			return err
		}
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
	c.sendMessage(wire.NewErrorMessage(code, message))
}
