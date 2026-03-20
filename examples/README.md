# CobaltDB Examples

Production-ready example applications demonstrating CobaltDB usage patterns.

## 📁 Examples

### 0. MySQL Server (`mysql-server/`) ⭐
**Start CobaltDB as a standalone MySQL-compatible server.** Connect with any MySQL client — mysql CLI, Python, Node.js, Java, Go, or any ORM.

**Run:**
```bash
go run examples/mysql-server/main.go
```

**Connect:**
```bash
mysql -h 127.0.0.1 -P 3307 -u admin
```

```sql
mysql> SELECT * FROM users;
mysql> SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name;
```

---

### 1. REST API (`rest-api/`)
A production-ready HTTP API with CRUD operations.

**Features:**
- Graceful shutdown with signal handling
- Structured logging with slog
- Health check endpoint
- Database connection management
- Input validation
- Error handling
- HTTP timeouts for production safety

**Run:**
```bash
cd rest-api
go run main.go

# With environment variables
COBALTDB_PATH=./data.db go run main.go
```

**API Endpoints:**
- `GET /health` - Health check
- `GET /users` - List all users
- `POST /users` - Create user
- `GET /users/{id}` - Get single user
- `PUT /users/{id}` - Update user
- `DELETE /users/{id}` - Delete user

**Test:**
```bash
# Create user
curl -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","name":"Alice","active":true}'

# List users
curl http://localhost:8080/users

# Get user
curl http://localhost:8080/users/1

# Update user
curl -X PUT http://localhost:8080/users/1 \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice Updated","active":false}'

# Delete user
curl -X DELETE http://localhost:8080/users/1
```

---

### 2. CLI Tool (`cli/`)
Command-line interface for database operations.

**Features:**
- Interactive commands
- SQL query execution
- Table management
- Data import/export

**Run:**
```bash
cd cli
go run main.go
```

---

### 3. Web App (`webapp/`)
HTML-based web application with server-side rendering.

**Features:**
- HTML templates
- Form handling
- Session management
- CSRF protection

**Run:**
```bash
cd webapp
go run main.go
```

---

### 4. Background Worker (`worker/`)
Background job processing with CobaltDB.

**Features:**
- Job queue implementation
- Retry logic
- Scheduled tasks
- Graceful shutdown

**Run:**
```bash
cd worker
go run main.go
```

---

## 🏗️ Production Patterns

### Database Connection
```go
db, err := engine.Open(dbPath, &engine.Options{
    CacheSize:  1024,        // Adjust based on memory
    WALEnabled: true,        // Enable for durability
    InMemory:   false,       // Use disk for persistence
})
```

### Graceful Shutdown
```go
quit := make(chan os.Signal, 1)
signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
<-quit

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

httpServer.Shutdown(ctx)
db.Close()
```

### Transaction Usage
```go
// Begin transaction
result, err := db.Exec(ctx, "BEGIN")
if err != nil {
    return err
}

// Operations...
_, err = db.Exec(ctx, "INSERT INTO users (name) VALUES (?)", name)
if err != nil {
    db.Exec(ctx, "ROLLBACK")
    return err
}

// Commit
_, err = db.Exec(ctx, "COMMIT")
```

### Health Check
```go
func handleHealth(w http.ResponseWriter, r *http.Request) {
    if err := db.Ping(r.Context()); err != nil {
        http.Error(w, "unhealthy", http.StatusServiceUnavailable)
        return
    }
    json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
```

---

## 📊 Performance Tips

1. **Use connection pooling**: Reuse database connections
2. **Enable WAL**: For better concurrent performance
3. **Proper indexing**: Index frequently queried columns
4. **Prepared statements**: For repeated queries
5. **Batch operations**: Insert multiple rows at once

---

## 🔒 Security Best Practices

1. **Input validation**: Always validate user input
2. **Parameterized queries**: Never concatenate SQL strings
3. **Timeouts**: Set read/write timeouts on HTTP server
4. **Graceful shutdown**: Handle shutdown signals properly
5. **Structured logging**: Use structured logging for observability
