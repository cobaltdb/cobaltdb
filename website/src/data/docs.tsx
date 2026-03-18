import { DocsInfo, DocsTip, DocsWarning } from '@components/docs/DocsContent'

export interface DocSection {
  title: string
  description?: string
  headings?: { id: string; title: string }[]
  content: React.ReactNode
}

export const docsData: Record<string, DocSection> = {
  'getting-started': {
    title: 'Introduction',
    description: 'CobaltDB is a high-performance, embeddable SQL database written in Go.',
    headings: [
      { id: 'what-is', title: 'What is CobaltDB?' },
      { id: 'features', title: 'Key Features' },
      { id: 'use-cases', title: 'Use Cases' },
    ],
    content: (
      <>
        <h2 id="what-is">What is CobaltDB?</h2>
        <p>
          CobaltDB is a modern SQL database engine designed for applications that need
          a fast, reliable, and easy-to-use data storage solution. Written entirely in Go,
          it can be embedded directly into your application without any external dependencies.
        </p>

        <h2 id="features">Key Features</h2>
        <ul>
          <li><strong>High Performance:</strong> B-tree storage with buffer pool caching delivers 100K+ QPS</li>
          <li><strong>ACID Transactions:</strong> Full transaction support with proper isolation</li>
          <li><strong>Modern SQL:</strong> CTEs, window functions, JSON operations, triggers, views</li>
          <li><strong>Full-Text Search:</strong> Built-in FTS4/FTS5 compatible search</li>
          <li><strong>WASM Support:</strong> Run in the browser for demos and offline apps</li>
          <li><strong>Security:</strong> Encryption at rest, TLS, audit logging, row-level security</li>
        </ul>

        <h2 id="use-cases">Use Cases</h2>
        <ul>
          <li>Embedded applications needing local data storage</li>
          <li>Microservices requiring lightweight databases</li>
          <li>Web applications with browser-based demos</li>
          <li>IoT devices and edge computing</li>
          <li>Testing and development environments</li>
        </ul>
      </>
    ),
  },

  'installation': {
    title: 'Installation',
    description: 'Get CobaltDB running in your Go project in minutes.',
    headings: [
      { id: 'requirements', title: 'Requirements' },
      { id: 'go-get', title: 'Install with Go' },
      { id: 'verify', title: 'Verify Installation' },
    ],
    content: (
      <>
        <h2 id="requirements">Requirements</h2>
        <ul>
          <li>Go 1.21 or later</li>
          <li>Compatible with Linux, macOS, and Windows</li>
        </ul>

        <h2 id="go-get">Install with Go</h2>
        <pre>go get github.com/cobaltdb/cobaltdb/pkg/engine</pre>

        <DocsTip title="Module Management">
          Make sure you have Go modules enabled in your project. Run `go mod init` if you haven't already.
        </DocsTip>

        <h2 id="verify">Verify Installation</h2>
        <pre>{`package main

import (
    "context"
    "fmt"
    "github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
    db, err := engine.Open(":memory:", nil)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    fmt.Println("CobaltDB is working!")
}`}</pre>
      </>
    ),
  },

  'quick-start': {
    title: 'Quick Start',
    description: 'Build your first application with CobaltDB.',
    headings: [
      { id: 'create-db', title: 'Create a Database' },
      { id: 'create-tables', title: 'Create Tables' },
      { id: 'insert-data', title: 'Insert Data' },
      { id: 'query-data', title: 'Query Data' },
    ],
    content: (
      <>
        <h2 id="create-db">Create a Database</h2>
        <pre>{`db, err := engine.Open("myapp.db", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()`}</pre>

        <h2 id="create-tables">Create Tables</h2>
        <pre>{`ctx := context.Background()

_, err = db.Exec(ctx, \`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
\`)`}</pre>

        <h2 id="insert-data">Insert Data</h2>
        <pre>{`// Single insert
_, err = db.Exec(ctx,
    "INSERT INTO users (name, email) VALUES (?, ?)",
    "John Doe", "john@example.com")

// Or use named parameters
_, err = db.Exec(ctx,
    "INSERT INTO users (name, email) VALUES ($name, $email)",
    map[string]any{
        "name": "Jane Doe",
        "email": "jane@example.com",
    })`}</pre>

        <h2 id="query-data">Query Data</h2>
        <pre>{`rows, err := db.Query(ctx, "SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}

for rows.Next() {
    var id int
    var name, email string
    var createdAt time.Time
    err = rows.Scan(&id, &name, &email, &createdAt)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("User: %d, %s, %s\\n", id, name, email)
}`}</pre>
      </>
    ),
  },

  'sql': {
    title: 'SQL Reference',
    description: 'Complete SQL syntax supported by CobaltDB.',
    headings: [
      { id: 'ddl', title: 'Data Definition' },
      { id: 'dml', title: 'Data Manipulation' },
      { id: 'queries', title: 'Querying Data' },
      { id: 'functions', title: 'Functions' },
    ],
    content: (
      <>
        <h2 id="ddl">Data Definition Language (DDL)</h2>
        <h3>CREATE TABLE</h3>
        <pre>{`CREATE TABLE table_name (
    column_name data_type [constraints],
    ...
    [table_constraints]
);`}</pre>

        <p>Supported data types:</p>
        <ul>
          <li><code>INTEGER</code> - 64-bit signed integer</li>
          <li><code>REAL</code> / <code>FLOAT</code> / <code>DOUBLE</code> - 64-bit floating point</li>
          <li><code>TEXT</code> / <code>VARCHAR</code> / <code>STRING</code> - Variable-length string</li>
          <li><code>BLOB</code> / <code>BYTES</code> - Binary data</li>
          <li><code>BOOLEAN</code> - True/False values</li>
          <li><code>TIMESTAMP</code> / <code>DATETIME</code> - Date and time</li>
          <li><code>DECIMAL(precision, scale)</code> - Exact decimal numbers</li>
          <li><code>JSON</code> - JSON data with query support</li>
        </ul>

        <h2 id="dml">Data Manipulation Language (DML)</h2>
        <h3>INSERT</h3>
        <pre>{`INSERT INTO table_name (column1, column2) VALUES (value1, value2);
INSERT INTO table_name VALUES (value1, value2, value3);
INSERT INTO table_name SELECT ...;`}</pre>

        <h3>UPDATE</h3>
        <pre>{`UPDATE table_name SET column1 = value1 WHERE condition;
UPDATE table_name SET column1 = value1 FROM other_table WHERE ...;`}</pre>

        <h3>DELETE</h3>
        <pre>{`DELETE FROM table_name WHERE condition;
DELETE FROM table_name USING other_table WHERE ...;`}</pre>

        <h2 id="queries">Querying Data</h2>
        <h3>SELECT</h3>
        <pre>{`SELECT [DISTINCT] select_list
FROM table_reference
[WHERE condition]
[GROUP BY grouping_expression]
[HAVING condition]
[ORDER BY expression [ASC|DESC]]
[LIMIT count [OFFSET offset]];`}</pre>

        <h3>JOINs</h3>
        <pre>{`SELECT * FROM a INNER JOIN b ON a.id = b.a_id;
SELECT * FROM a LEFT JOIN b ON a.id = b.a_id;
SELECT * FROM a CROSS JOIN b;`}</pre>

        <DocsInfo>
          NATURAL JOIN is not currently supported. Use explicit JOIN conditions instead.
        </DocsInfo>

        <h2 id="functions">Functions</h2>
        <h3>Aggregate Functions</h3>
        <ul>
          <li><code>COUNT(*)</code>, <code>COUNT(expression)</code></li>
          <li><code>SUM(expression)</code></li>
          <li><code>AVG(expression)</code></li>
          <li><code>MIN(expression)</code>, <code>MAX(expression)</code></li>
          <li><code>GROUP_CONCAT(expression [, separator])</code></li>
        </ul>

        <h3>String Functions</h3>
        <ul>
          <li><code>UPPER(string)</code>, <code>LOWER(string)</code></li>
          <li><code>LENGTH(string)</code></li>
          <li><code>SUBSTRING(string, start [, length])</code></li>
          <li><code>REPLACE(string, search, replace)</code></li>
          <li><code>TRIM(string)</code>, <code>LTRIM(string)</code>, <code>RTRIM(string)</code></li>
          <li><code>CONCAT(string1, string2, ...)</code>, <code>CONCAT_WS(separator, ...)</code></li>
        </ul>

        <h3>Date/Time Functions</h3>
        <ul>
          <li><code>NOW()</code>, <code>CURRENT_TIMESTAMP</code></li>
          <li><code>CURRENT_DATE</code>, <code>CURRENT_TIME</code></li>
          <li><code>DATE(expression)</code>, <code>TIME(expression)</code></li>
          <li><code>EXTRACT(part FROM expression)</code></li>
          <li><code>STRFTIME(format, timestamp)</code></li>
        </ul>
      </>
    ),
  },

  'transactions': {
    title: 'Transactions',
    description: 'ACID transactions and concurrency control.',
    headings: [
      { id: 'basic', title: 'Basic Transactions' },
      { id: 'savepoints', title: 'Savepoints' },
      { id: 'isolation', title: 'Isolation Levels' },
    ],
    content: (
      <>
        <h2 id="basic">Basic Transactions</h2>
        <pre>{`_, err := db.Exec(ctx, "BEGIN")
if err != nil {
    return err
}

// ... perform operations ...

_, err = db.Exec(ctx, "COMMIT")
// Or: _, err = db.Exec(ctx, "ROLLBACK")`}</pre>

        <DocsTip>
          Use <code>BEGIN IMMEDIATE</code> to acquire the write lock immediately,
          preventing other transactions from interfering.
        </DocsTip>

        <h2 id="savepoints">Savepoints</h2>
        <p>Savepoints allow partial rollbacks within a transaction:</p>
        <pre>{`BEGIN;
  INSERT INTO accounts VALUES (1, 100);
  SAVEPOINT before_transfer;
    UPDATE accounts SET balance = balance - 50 WHERE id = 1;
    -- Oops, something went wrong!
    ROLLBACK TO before_transfer;
  -- Account still has 100
COMMIT;`}</pre>

        <h2 id="isolation">Isolation Levels</h2>
        <p>CobaltDB uses <strong>Serializable</strong> isolation by default, the strictest level.</p>
        <ul>
          <li>Dirty reads are prevented</li>
          <li>Non-repeatable reads are prevented</li>
          <li>Phantom reads are prevented</li>
        </ul>
      </>
    ),
  },

  'indexes': {
    title: 'Indexes',
    description: 'Optimize query performance with indexes.',
    headings: [
      { id: 'creating', title: 'Creating Indexes' },
      { id: 'types', title: 'Index Types' },
      { id: 'usage', title: 'When to Use Indexes' },
    ],
    content: (
      <>
        <h2 id="creating">Creating Indexes</h2>
        <pre>{`CREATE INDEX idx_users_email ON users(email);
CREATE UNIQUE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_name ON users(last_name, first_name);`}</pre>

        <h2 id="types">Index Types</h2>
        <ul>
          <li><strong>B-Tree Indexes:</strong> Default index type, excellent for equality and range queries</li>
          <li><strong>Unique Indexes:</strong> Enforce uniqueness constraints</li>
          <li><strong>Composite Indexes:</strong> Index on multiple columns</li>
        </ul>

        <h2 id="usage">When to Use Indexes</h2>
        <p>Create indexes on columns that are:</p>
        <ul>
          <li>Frequently used in WHERE clauses</li>
          <li>Used in JOIN conditions</li>
          <li>Used in ORDER BY clauses</li>
        </ul>

        <DocsWarning>
          Indexes slow down INSERT, UPDATE, and DELETE operations. Don't over-index!
        </DocsWarning>
      </>
    ),
  },

  'window-functions': {
    title: 'Window Functions',
    description: 'Advanced analytics with window functions.',
    headings: [
      { id: 'syntax', title: 'Syntax' },
      { id: 'functions', title: 'Supported Functions' },
      { id: 'examples', title: 'Examples' },
    ],
    content: (
      <>
        <h2 id="syntax">Syntax</h2>
        <pre>{`function_name(expression) OVER (
    [PARTITION BY partition_expression]
    [ORDER BY sort_expression]
    [frame_specification]
) AS alias`}</pre>

        <h2 id="functions">Supported Functions</h2>
        <ul>
          <li><code>ROW_NUMBER()</code> - Unique sequential row number</li>
          <li><code>RANK()</code> - Rank with gaps for ties</li>
          <li><code>DENSE_RANK()</code> - Rank without gaps</li>
          <li><code>NTILE(n)</code> - Divide rows into n buckets</li>
          <li><code>LAG(expr, offset)</code> - Access previous row</li>
          <li><code>LEAD(expr, offset)</code> - Access next row</li>
          <li><code>FIRST_VALUE(expr)</code> - First value in window</li>
          <li><code>LAST_VALUE(expr)</code> - Last value in window</li>
        </ul>

        <h2 id="examples">Examples</h2>
        <pre>{`-- Running total
SELECT
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total
FROM sales;

-- Rank employees by salary per department
SELECT
    name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank
FROM employees;

-- Moving average
SELECT
    date,
    value,
    AVG(value) OVER (ORDER BY date ROWS 2 PRECEDING) as moving_avg
FROM metrics;`}</pre>
      </>
    ),
  },

  'ctes': {
    title: 'CTEs & Subqueries',
    description: 'Common Table Expressions and subqueries.',
    headings: [
      { id: 'cte', title: 'Common Table Expressions' },
      { id: 'recursive', title: 'Recursive CTEs' },
      { id: 'subqueries', title: 'Subqueries' },
    ],
    content: (
      <>
        <h2 id="cte">Common Table Expressions</h2>
        <pre>{`WITH top_sales AS (
    SELECT salesperson_id, SUM(amount) as total
    FROM sales
    GROUP BY salesperson_id
    ORDER BY total DESC
    LIMIT 10
)
SELECT s.name, ts.total
FROM salespeople s
JOIN top_sales ts ON s.id = ts.salesperson_id;`}</pre>

        <h2 id="recursive">Recursive CTEs</h2>
        <pre>{`WITH RECURSIVE subordinates AS (
    -- Base case: direct reports
    SELECT id, name, manager_id, 0 as level
    FROM employees
    WHERE manager_id = 1

    UNION ALL

    -- Recursive case
    SELECT e.id, e.name, e.manager_id, s.level + 1
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.id
)
SELECT * FROM subordinates;`}</pre>

        <h2 id="subqueries">Subqueries</h2>
        <pre>{`-- Correlated subquery
SELECT name
FROM employees e
WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept = e.dept);

-- IN subquery
SELECT * FROM products
WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics');

-- EXISTS subquery
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);`}</pre>
      </>
    ),
  },

  'fts': {
    title: 'Full-Text Search',
    description: 'Search text content efficiently.',
    headings: [
      { id: 'setup', title: 'Creating FTS Tables' },
      { id: 'querying', title: 'Querying' },
      { id: 'tokenizers', title: 'Tokenizers' },
    ],
    content: (
      <>
        <h2 id="setup">Creating FTS Tables</h2>
        <pre>{`CREATE VIRTUAL TABLE docs USING fts4(title, content);

-- Insert documents
INSERT INTO docs VALUES ('Introduction', 'CobaltDB is a fast database');
INSERT INTO docs VALUES ('Features', 'Full-text search is supported');`}</pre>

        <h2 id="querying">Querying</h2>
        <pre>{`-- Basic match
SELECT * FROM docs WHERE docs MATCH 'database';

-- Multiple terms (AND)
SELECT * FROM docs WHERE docs MATCH 'fast database';

-- OR operator
SELECT * FROM docs WHERE docs MATCH 'fast OR features';

-- Phrase search
SELECT * FROM docs WHERE docs MATCH '"full text"';

-- Prefix search
SELECT * FROM docs WHERE docs MATCH 'data*';

-- With snippet/highlight
SELECT title, snippet(docs) FROM docs WHERE docs MATCH 'fast';`}</pre>

        <h2 id="tokenizers">Tokenizers</h2>
        <p>Choose how text is broken into tokens:</p>
        <ul>
          <li><code>simple</code> - Default, case-insensitive ASCII</li>
          <li><code>porter</code> - English stemming</li>
          <li><code>unicode61</code> - Unicode-aware tokenization</li>
        </ul>
        <pre>{`CREATE VIRTUAL TABLE docs USING fts4(content, tokenize=porter);`}</pre>
      </>
    ),
  },

  'json': {
    title: 'JSON Support',
    description: 'Store and query JSON data.',
    headings: [
      { id: 'storage', title: 'JSON Storage' },
      { id: 'functions', title: 'JSON Functions' },
      { id: 'indexing', title: 'Indexing JSON' },
    ],
    content: (
      <>
        <h2 id="storage">JSON Storage</h2>
        <pre>{`CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO events (data) VALUES ('{"user": "john", "action": "login"}');`}</pre>

        <h2 id="functions">JSON Functions</h2>
        <pre>{`-- Extract value
SELECT JSON_EXTRACT(data, '$.user') FROM events;

-- Set value
UPDATE events SET data = JSON_SET(data, '$.ip', '192.168.1.1');

-- Remove key
UPDATE events SET data = JSON_REMOVE(data, '$.temp');

-- Check if valid JSON
SELECT * FROM events WHERE JSON_VALID(data);

-- Array functions
SELECT JSON_ARRAY_LENGTH('[1, 2, 3]');  -- Returns 3
SELECT JSON_ARRAY(1, 2, 'three');       -- Returns [1,2,"three"]

-- Object functions
SELECT JSON_OBJECT('a', 1, 'b', 2);     -- Returns {"a":1,"b":2}`}</pre>

        <DocsInfo>
          JSON paths use dot notation: <code>$.key</code> for objects, <code>$[0]</code> for arrays.
        </DocsInfo>

        <h2 id="indexing">Indexing JSON</h2>
        <pre>{`-- Create index on extracted value
CREATE INDEX idx_events_user ON events(JSON_EXTRACT(data, '$.user'));

-- Query using index
SELECT * FROM events WHERE JSON_EXTRACT(data, '$.user') = 'john';`}</pre>
      </>
    ),
  },

  'triggers': {
    title: 'Triggers',
    description: 'Automate actions with database triggers.',
    headings: [
      { id: 'syntax', title: 'Trigger Syntax' },
      { id: 'examples', title: 'Examples' },
      { id: 'limitations', title: 'Limitations' },
    ],
    content: (
      <>
        <h2 id="syntax">Trigger Syntax</h2>
        <pre>{`CREATE TRIGGER trigger_name
[BEFORE | AFTER] [INSERT | UPDATE | DELETE]
ON table_name
[FOR EACH ROW]
BEGIN
    -- Trigger body
    INSERT INTO audit_log VALUES (OLD.id, datetime('now'));
END;`}</pre>

        <h2 id="examples">Examples</h2>
        <pre>{`-- Audit logging
CREATE TRIGGER log_user_changes
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, row_id, action, changed_at)
    VALUES ('users', OLD.id, 'UPDATE', datetime('now'));
END;

-- Auto-update timestamp
CREATE TRIGGER update_modified
AFTER UPDATE ON articles
FOR EACH ROW
BEGIN
    UPDATE articles SET modified_at = datetime('now')
    WHERE id = OLD.id;
END;

-- Enforce business rules
CREATE TRIGGER check_balance
BEFORE INSERT ON transactions
FOR EACH ROW
WHEN NEW.amount > (
    SELECT balance FROM accounts WHERE id = NEW.account_id
)
BEGIN
    SELECT RAISE(ABORT, 'Insufficient funds');
END;`}</pre>

        <h2 id="limitations">Limitations</h2>
        <DocsWarning>
          INSTEAD OF triggers are not supported. Use BEFORE/AFTER triggers instead.
        </DocsWarning>
      </>
    ),
  },

  'views': {
    title: 'Views',
    description: 'Virtual tables based on queries.',
    headings: [
      { id: 'simple', title: 'Simple Views' },
      { id: 'materialized', title: 'Materialized Views' },
      { id: 'updatable', title: 'Updatable Views' },
    ],
    content: (
      <>
        <h2 id="simple">Simple Views</h2>
        <pre>{`CREATE VIEW active_users AS
SELECT id, name, email
FROM users
WHERE status = 'active';

-- Use the view
SELECT * FROM active_users WHERE email LIKE '%@company.com';`}</pre>

        <h2 id="materialized">Materialized Views</h2>
        <pre>{`CREATE MATERIALIZED VIEW daily_sales AS
SELECT
    date(created_at) as sale_date,
    COUNT(*) as order_count,
    SUM(total) as revenue
FROM orders
GROUP BY date(created_at);

-- Refresh the materialized view
REFRESH MATERIALIZED VIEW daily_sales;`}</pre>

        <DocsInfo>
          Materialized views store query results for faster access but need to be refreshed manually.
        </DocsInfo>

        <h2 id="updatable">Updatable Views</h2>
        <p>Simple views can be updated directly:</p>
        <pre>{`CREATE VIEW user_emails AS
SELECT id, email FROM users;

-- This works for simple views
UPDATE user_emails SET email = 'new@example.com' WHERE id = 1;`}</pre>
      </>
    ),
  },

  'wasm': {
    title: 'WASM Guide',
    description: 'Run CobaltDB in the browser with WebAssembly.',
    headings: [
      { id: 'setup', title: 'Setup' },
      { id: 'usage', title: 'Basic Usage' },
      { id: 'playground', title: 'Playground' },
    ],
    content: (
      <>
        <h2 id="setup">Setup</h2>
        <p>Install the WASM package:</p>
        <pre>npm install @cobaltdb/wasm</pre>

        <h2 id="usage">Basic Usage</h2>
        <pre>{`import { CobaltDB } from '@cobaltdb/wasm';

async function main() {
    // Initialize the database
    const db = await CobaltDB.init();

    // Execute SQL
    db.exec(\`
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    \`);

    // Insert data
    db.exec(
        "INSERT INTO messages (content) VALUES (?)",
        ["Hello from WASM!"]
    );

    // Query results
    const results = db.query("SELECT * FROM messages");
    console.log(results);
}

main();`}</pre>

        <h2 id="playground">Playground</h2>
        <p>
          Try CobaltDB directly in your browser using our{' '}
          <a href="/playground">interactive playground</a>.
          No installation required!
        </p>
      </>
    ),
  },

  'api': {
    title: 'Go API Reference',
    description: 'Complete Go API documentation.',
    headings: [
      { id: 'database', title: 'Database' },
      { id: 'query', title: 'Querying' },
      { id: 'transaction', title: 'Transactions' },
    ],
    content: (
      <>
        <h2 id="database">Database</h2>
        <h3>Open</h3>
        <pre>{`func Open(name string, options *Options) (*Database, error)

// Example
db, err := engine.Open("mydb.db", &engine.Options{
    CacheSize: 1024 * 1024 * 100, // 100MB cache
})`}</pre>

        <h3>Close</h3>
        <pre>{`func (db *Database) Close() error`}</pre>

        <h2 id="query">Querying</h2>
        <h3>Exec</h3>
        <pre>{`func (db *Database) Exec(ctx context.Context, sql string, args ...any) (Result, error)

// Example
result, err := db.Exec(ctx,
    "INSERT INTO users (name) VALUES (?)",
    "John")`}</pre>

        <h3>Query</h3>
        <pre>{`func (db *Database) Query(ctx context.Context, sql string, args ...any) (*Rows, error)

// Example
rows, err := db.Query(ctx, "SELECT * FROM users WHERE active = ?", true)
for rows.Next() {
    var u User
    rows.Scan(&u.ID, &u.Name, &u.Active)
}`}</pre>

        <h2 id="transaction">Transactions</h2>
        <pre>{`func (db *Database) Begin() (*Tx, error)
func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error

// Example
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback()

_, err = tx.Exec(ctx, "INSERT INTO ...")
if err != nil {
    return err
}

return tx.Commit()`}</pre>
      </>
    ),
  },

  'security': {
    title: 'Security',
    description: 'Encryption, authentication, and access control.',
    headings: [
      { id: 'encryption', title: 'Encryption at Rest' },
      { id: 'tls', title: 'TLS' },
      { id: 'rls', title: 'Row-Level Security' },
      { id: 'audit', title: 'Audit Logging' },
    ],
    content: (
      <>
        <h2 id="encryption">Encryption at Rest</h2>
        <pre>{`key := []byte("your-32-byte-key-here!!!!!!!")

db, err := engine.Open("encrypted.db", &engine.Options{
    EncryptionKey: key,
})`}</pre>

        <DocsWarning title="Key Management">
          Never hardcode encryption keys in your source code. Use environment variables
          or a proper key management service.
        </DocsWarning>

        <h2 id="tls">TLS</h2>
        <pre>{`server, err := server.New(server.Options{
    TLSCert: "server.crt",
    TLSKey:  "server.key",
})`}</pre>

        <h2 id="rls">Row-Level Security</h2>
        <pre>{`-- Enable RLS on a table
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

-- Create a policy
CREATE POLICY user_documents ON documents
    FOR ALL
    TO application_user
    USING (owner_id = current_user_id());`}</pre>

        <DocsInfo>
          RLS policies automatically filter rows based on the current user context.
        </DocsInfo>

        <h2 id="audit">Audit Logging</h2>
        <pre>{`db, err := engine.Open("mydb.db", &engine.Options{
    AuditLogger: &audit.FileLogger{
        Path: "/var/log/cobaltdb/audit.log",
    },
})`}</pre>
      </>
    ),
  },

  'engine': {
    title: 'Database Engine',
    description: 'Architecture and internals.',
    headings: [
      { id: 'architecture', title: 'Architecture' },
      { id: 'storage', title: 'Storage Layer' },
      { id: 'btree', title: 'B-Tree Engine' },
    ],
    content: (
      <>
        <h2 id="architecture">Architecture</h2>
        <p>CobaltDB follows a layered architecture:</p>
        <ol>
          <li><strong>SQL Layer:</strong> Parser, analyzer, query planner</li>
          <li><strong>Execution Layer:</strong> Query execution, transactions</li>
          <li><strong>Storage Layer:</strong> B-tree indexes, heap storage</li>
          <li><strong>Page Layer:</strong> Buffer pool, disk I/O</li>
        </ol>

        <h2 id="storage">Storage Layer</h2>
        <p>Data is stored in pages (default 4KB):</p>
        <ul>
          <li>Table data stored as JSON-encoded rows</li>
          <li>B-tree indexes for fast lookups</li>
          <li>Write-ahead logging (WAL) for durability</li>
        </ul>

        <h2 id="btree">B-Tree Engine</h2>
        <p>The B-tree implementation features:</p>
        <ul>
          <li>Self-balancing tree structure</li>
          <li>Efficient range scans</li>
          <li>Node splitting and merging</li>
          <li>Cursor-based iteration</li>
        </ul>
      </>
    ),
  },
}
