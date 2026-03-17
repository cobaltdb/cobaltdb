import { useState, useRef, useEffect, useCallback } from 'react'
import { usePageTitle } from '@hooks/usePageTitle'
import { Button } from '@components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@components/ui/tabs'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@components/ui/select'
import { Play, RotateCcw, Copy, Check, Database, Table2, FileJson, Loader2, AlertCircle, Trash2, History, Clock } from 'lucide-react'
import { Alert, AlertDescription } from '@components/ui/alert'
import { SqlEditor } from '@components/playground/SqlEditor'
import initSqlJs, { type Database as SqlJsDatabase } from 'sql.js'

interface QueryResult {
  columns: string[]
  rows: any[][]
  executionTime: number
  rowCount: number
  statement: string
}

const sampleDatabases = {
  empty: {
    name: 'Empty Database',
    setup: '',
  },
  employees: {
    name: 'HR Database',
    setup: `CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    department_id INTEGER,
    salary INTEGER,
    hire_date TEXT,
    manager_id INTEGER,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (manager_id) REFERENCES employees(id)
);

INSERT INTO departments (name, location) VALUES
    ('Engineering', 'Building A'),
    ('Sales', 'Building B'),
    ('Marketing', 'Building C');

INSERT INTO employees (name, email, department_id, salary, hire_date) VALUES
    ('Alice Johnson', 'alice@company.com', 1, 120000, '2020-01-15'),
    ('Bob Smith', 'bob@company.com', 1, 95000, '2021-03-22'),
    ('Carol White', 'carol@company.com', 2, 85000, '2020-06-10'),
    ('David Brown', 'david@company.com', 2, 75000, '2022-01-05'),
    ('Eve Davis', 'eve@company.com', 3, 70000, '2021-09-14');

UPDATE employees SET manager_id = 1 WHERE id IN (2, 3, 4, 5);`,
  },
  ecommerce: {
    name: 'E-Commerce',
    setup: `CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    price REAL,
    stock INTEGER
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    city TEXT
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date TEXT,
    total REAL,
    status TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price REAL,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

INSERT INTO products (name, category, price, stock) VALUES
    ('Laptop Pro', 'Electronics', 1299.99, 50),
    ('Wireless Mouse', 'Electronics', 29.99, 200),
    ('USB-C Hub', 'Electronics', 79.99, 100),
    ('Coffee Maker', 'Appliances', 89.99, 30),
    ('Desk Lamp', 'Office', 45.99, 75);

INSERT INTO customers (name, email, city) VALUES
    ('John Doe', 'john@email.com', 'New York'),
    ('Jane Smith', 'jane@email.com', 'Los Angeles'),
    ('Bob Wilson', 'bob@email.com', 'Chicago');

INSERT INTO orders (customer_id, order_date, total, status) VALUES
    (1, '2024-01-15', 1329.98, 'completed'),
    (1, '2024-02-03', 45.99, 'completed'),
    (2, '2024-01-28', 89.99, 'completed'),
    (3, '2024-02-10', 79.99, 'pending');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1299.99),
    (1, 2, 1, 29.99),
    (2, 5, 1, 45.99),
    (3, 4, 1, 89.99),
    (4, 3, 1, 79.99);`,
  },
}

const sampleQueries: Record<string, { name: string; sql: string }[]> = {
  employees: [
    { name: 'All Employees', sql: 'SELECT * FROM employees;' },
    { name: 'JOIN Query', sql: 'SELECT e.name, d.name as department, e.salary\nFROM employees e\nJOIN departments d ON e.department_id = d.id\nORDER BY e.salary DESC;' },
    { name: 'Aggregation', sql: 'SELECT\n    d.name as department,\n    COUNT(*) as emp_count,\n    AVG(e.salary) as avg_salary,\n    MAX(e.salary) as max_salary\nFROM employees e\nJOIN departments d ON e.department_id = d.id\nGROUP BY d.name;' },
    { name: 'Window Function', sql: 'SELECT\n    name,\n    salary,\n    department_id,\n    RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as dept_rank,\n    RANK() OVER (ORDER BY salary DESC) as overall_rank\nFROM employees;' },
    { name: 'Subquery', sql: 'SELECT name, salary\nFROM employees\nWHERE salary > (\n    SELECT AVG(salary) FROM employees\n)\nORDER BY salary DESC;' },
    { name: 'CTE', sql: 'WITH dept_stats AS (\n    SELECT\n        department_id,\n        AVG(salary) as avg_sal,\n        COUNT(*) as cnt\n    FROM employees\n    GROUP BY department_id\n)\nSELECT\n    d.name,\n    ds.avg_sal,\n    ds.cnt\nFROM dept_stats ds\nJOIN departments d ON d.id = ds.department_id;' },
  ],
  ecommerce: [
    { name: 'All Products', sql: 'SELECT * FROM products;' },
    { name: 'Revenue by Customer', sql: 'SELECT\n    c.name,\n    c.city,\n    COUNT(o.id) as order_count,\n    SUM(o.total) as total_spent\nFROM customers c\nJOIN orders o ON c.id = o.customer_id\nGROUP BY c.id\nORDER BY total_spent DESC;' },
    { name: 'Order Details', sql: 'SELECT\n    o.id as order_id,\n    c.name as customer,\n    p.name as product,\n    oi.quantity,\n    oi.unit_price,\n    o.status\nFROM orders o\nJOIN customers c ON o.customer_id = c.id\nJOIN order_items oi ON o.id = oi.order_id\nJOIN products p ON oi.product_id = p.id;' },
    { name: 'Category Stats', sql: 'SELECT\n    category,\n    COUNT(*) as product_count,\n    ROUND(AVG(price), 2) as avg_price,\n    SUM(stock) as total_stock\nFROM products\nGROUP BY category\nORDER BY avg_price DESC;' },
  ],
  empty: [
    { name: 'Create Table', sql: 'CREATE TABLE notes (\n    id INTEGER PRIMARY KEY,\n    title TEXT NOT NULL,\n    content TEXT,\n    created_at TEXT DEFAULT (datetime(\'now\'))\n);' },
    { name: 'Insert Data', sql: "INSERT INTO notes (title, content) VALUES\n    ('Hello', 'My first note'),\n    ('Todo', 'Learn SQL with CobaltDB');" },
    { name: 'Query Data', sql: 'SELECT * FROM notes;' },
  ],
}

export function PlaygroundPage() {
  usePageTitle('SQL Playground')
  const [sql, setSql] = useState('SELECT * FROM employees;')
  const [results, setResults] = useState<QueryResult[]>([])
  const [error, setError] = useState<string | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [selectedDb, setSelectedDb] = useState('employees')
  const [copied, setCopied] = useState(false)
  const [activeTab, setActiveTab] = useState('results')
  const [wasmLoaded, setWasmLoaded] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [history, setHistory] = useState<{ sql: string; time: string; ok: boolean }[]>([])
  const [schemaKey, setSchemaKey] = useState(0) // bump to re-render schema
  const dbRef = useRef<SqlJsDatabase | null>(null)
  const sqlRef = useRef<typeof initSqlJs | null>(null)

  // Load sql.js WASM
  useEffect(() => {
    const load = async () => {
      try {
        const SQL = await initSqlJs({
          locateFile: () => '/sql-wasm.wasm'
        })
        sqlRef.current = SQL as any
        // Create initial database
        const db = new SQL.Database()
        dbRef.current = db
        // Run setup SQL for default database
        const setup = sampleDatabases.employees.setup
        if (setup) {
          db.run(setup)
        }
        setWasmLoaded(true)
      } catch (err: any) {
        setLoadError(err.message || 'Failed to load SQL engine')
      }
    }
    load()

    return () => {
      if (dbRef.current) {
        dbRef.current.close()
      }
    }
  }, [])

  const initializeDatabase = useCallback((dbKey: string) => {
    if (!sqlRef.current) return
    // Close old db
    if (dbRef.current) {
      dbRef.current.close()
    }
    // Create fresh database
    const SQL = sqlRef.current as any
    const db = new SQL.Database()
    dbRef.current = db
    const setup = sampleDatabases[dbKey as keyof typeof sampleDatabases]?.setup
    if (setup) {
      try {
        db.run(setup)
      } catch (err: any) {
        setError(`Setup error: ${err.message}`)
      }
    }
    setResults([])
    setError(null)
  }, [])

  const handleDbChange = (value: string | null) => {
    if (value) {
      setSelectedDb(value)
      initializeDatabase(value)
      // Set appropriate default query
      const queries = sampleQueries[value]
      if (queries && queries.length > 0) {
        setSql(queries[0].sql)
      }
    }
  }

  const executeQuery = useCallback(() => {
    if (!dbRef.current) return

    setIsExecuting(true)
    setError(null)
    setActiveTab('results')

    // Use setTimeout to allow UI to update before potentially heavy query
    setTimeout(() => {
      try {
        const db = dbRef.current!
        const start = performance.now()

        // Split by semicolons for multi-statement support
        const statements = sql.split(';').map(s => s.trim()).filter(Boolean)
        const newResults: QueryResult[] = []

        for (const stmt of statements) {
          try {
            const stmtResults = db.exec(stmt)
            const elapsed = performance.now() - start

            if (stmtResults.length > 0) {
              for (const res of stmtResults) {
                newResults.push({
                  columns: res.columns,
                  rows: res.values,
                  executionTime: Math.round(elapsed * 100) / 100,
                  rowCount: res.values.length,
                  statement: stmt,
                })
              }
            } else {
              // DDL/DML statement with no result set
              const changes = db.getRowsModified()
              newResults.push({
                columns: ['Result'],
                rows: [[`Query OK. ${changes} row(s) affected.`]],
                executionTime: Math.round(elapsed * 100) / 100,
                rowCount: 0,
                statement: stmt,
              })
            }
          } catch (err: any) {
            setError(`${err.message}\n\nStatement: ${stmt}`)
            setIsExecuting(false)
            return
          }
        }

        setResults(newResults)
        setHistory(prev => [{ sql: sql.trim(), time: new Date().toLocaleTimeString(), ok: true }, ...prev].slice(0, 50))
        // Bump schema if DDL detected
        if (/\b(CREATE|DROP|ALTER)\b/i.test(sql)) {
          setSchemaKey(k => k + 1)
        }
      } catch (err: any) {
        setError(err.message || 'An error occurred while executing the query')
        setHistory(prev => [{ sql: sql.trim(), time: new Date().toLocaleTimeString(), ok: false }, ...prev].slice(0, 50))
      } finally {
        setIsExecuting(false)
      }
    }, 10)
  }, [sql])

  const copyToClipboard = async () => {
    await navigator.clipboard.writeText(sql)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const resetDatabase = () => {
    initializeDatabase(selectedDb)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault()
      executeQuery()
    }
  }

  const currentQueries = sampleQueries[selectedDb] || sampleQueries.empty

  // Loading state
  if (!wasmLoaded) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            {loadError ? (
              <>
                <AlertCircle className="h-12 w-12 mx-auto mb-4 text-red-500" />
                <p className="text-red-500 font-medium mb-2">Failed to load SQL engine</p>
                <p className="text-sm text-muted-foreground max-w-sm">{loadError}</p>
                <Button
                  variant="outline"
                  size="sm"
                  className="mt-4"
                  onClick={() => window.location.reload()}
                >
                  Retry
                </Button>
              </>
            ) : (
              <>
                <Loader2 className="h-12 w-12 mx-auto mb-4 text-cobalt-600 animate-spin" />
                <p className="text-muted-foreground font-medium">Loading SQL Engine (WASM)...</p>
                <p className="text-xs text-muted-foreground mt-1">Downloading SQLite compiled to WebAssembly</p>
              </>
            )}
          </div>
        </div>
      </div>
    )
  }

  // Latest result for display
  const latestResult = results.length > 0 ? results[results.length - 1] : null

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <h1 className="text-3xl font-bold tracking-tight">
              SQL <span className="text-gradient">Playground</span>
            </h1>
            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400 text-[10px] font-semibold">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
              WASM Live
            </span>
          </div>
          <p className="text-muted-foreground">
            Real SQL execution in your browser via WebAssembly. Press <kbd className="px-1.5 py-0.5 text-xs bg-muted rounded border font-mono">Ctrl+Enter</kbd> to run.
          </p>
        </div>

        <div className="grid lg:grid-cols-3 gap-6">
          {/* Left Panel - Editor */}
          <div className="lg:col-span-2 space-y-4">
            <Card className="overflow-hidden">
              <CardHeader className="pb-3 border-b bg-muted/30">
                <div className="flex items-center justify-between flex-wrap gap-2">
                  <CardTitle className="text-base flex items-center gap-2">
                    <Database className="h-4 w-4 text-cobalt-600" />
                    Query Editor
                  </CardTitle>
                  <div className="flex items-center gap-2">
                    <Select value={selectedDb} onValueChange={handleDbChange}>
                      <SelectTrigger className="w-40 h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {Object.entries(sampleDatabases).map(([key, db]) => (
                          <SelectItem key={key} value={key}>
                            {db.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Button variant="outline" size="icon" onClick={resetDatabase} title="Reset database" className="h-8 w-8">
                      <RotateCcw className="h-3.5 w-3.5" />
                    </Button>
                    <Button variant="outline" size="icon" onClick={copyToClipboard} title="Copy SQL" className="h-8 w-8">
                      {copied ? <Check className="h-3.5 w-3.5 text-green-500" /> : <Copy className="h-3.5 w-3.5" />}
                    </Button>
                    <Button
                      onClick={executeQuery}
                      disabled={isExecuting || !sql.trim()}
                      size="sm"
                      className="bg-cobalt-600 hover:bg-cobalt-700 text-white h-8"
                    >
                      {isExecuting ? (
                        <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                      ) : (
                        <Play className="h-3.5 w-3.5 mr-1.5" />
                      )}
                      Run
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <SqlEditor
                  value={sql}
                  onChange={setSql}
                  onKeyDown={handleKeyDown}
                />
              </CardContent>
            </Card>

            {error && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription className="font-mono text-xs whitespace-pre-wrap">{error}</AlertDescription>
              </Alert>
            )}

            {/* Results */}
            <div className="flex items-center justify-between">
              <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1">
                <div className="flex items-center justify-between mb-2">
                  <TabsList>
                    <TabsTrigger value="results">
                      <Table2 className="h-3.5 w-3.5 mr-1.5" />
                      Results
                      {latestResult && latestResult.rowCount > 0 && (
                        <span className="ml-1.5 text-[10px] px-1.5 py-0.5 rounded-full bg-cobalt-100 dark:bg-cobalt-900/40 text-cobalt-700 dark:text-cobalt-300">
                          {latestResult.rowCount}
                        </span>
                      )}
                    </TabsTrigger>
                    <TabsTrigger value="json">
                      <FileJson className="h-3.5 w-3.5 mr-1.5" />
                      JSON
                    </TabsTrigger>
                  </TabsList>
                  {results.length > 0 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs text-muted-foreground"
                      onClick={() => setResults([])}
                    >
                      <Trash2 className="h-3 w-3 mr-1" />
                      Clear
                    </Button>
                  )}
                </div>

                <TabsContent value="results">
                  {results.length > 0 ? (
                    <div className="space-y-3">
                      {results.map((result, idx) => (
                        <Card key={idx} className="overflow-hidden">
                          <CardContent className="p-0">
                            <div className="overflow-x-auto">
                              <table className="w-full text-sm">
                                <thead>
                                  <tr className="bg-muted/50 border-b">
                                    {result.columns.map((col, colIdx) => (
                                      <th key={colIdx} className="px-4 py-2.5 text-left text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                                        {col}
                                      </th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {result.rows.map((row, rowIdx) => (
                                    <tr key={rowIdx} className="border-b last:border-0 hover:bg-muted/30 transition-colors">
                                      {row.map((cell, cellIdx) => (
                                        <td key={cellIdx} className="px-4 py-2.5 font-mono text-xs">
                                          {cell === null ? (
                                            <span className="text-muted-foreground/50 italic">NULL</span>
                                          ) : (
                                            String(cell)
                                          )}
                                        </td>
                                      ))}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                            <div className="px-4 py-2 bg-muted/30 border-t text-xs text-muted-foreground flex items-center gap-3">
                              <span>{result.rowCount} row{result.rowCount !== 1 ? 's' : ''}</span>
                              <span className="text-border">|</span>
                              <span>{result.executionTime}ms</span>
                              {results.length > 1 && (
                                <>
                                  <span className="text-border">|</span>
                                  <span className="font-mono text-[10px] truncate max-w-[200px]">{result.statement}</span>
                                </>
                              )}
                            </div>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  ) : (
                    <Card className="h-40 flex items-center justify-center">
                      <p className="text-sm text-muted-foreground">Run a query to see results</p>
                    </Card>
                  )}
                </TabsContent>

                <TabsContent value="json">
                  {results.length > 0 ? (
                    <Card>
                      <CardContent className="p-4">
                        <pre className="text-xs overflow-x-auto font-mono">
                          {JSON.stringify(
                            results.map((r) =>
                              r.rows.map((row) =>
                                Object.fromEntries(
                                  r.columns.map((col, idx) => [col, row[idx]])
                                )
                              )
                            ).flat(),
                            null,
                            2
                          )}
                        </pre>
                      </CardContent>
                    </Card>
                  ) : (
                    <Card className="h-40 flex items-center justify-center">
                      <p className="text-sm text-muted-foreground">Run a query to see JSON output</p>
                    </Card>
                  )}
                </TabsContent>
              </Tabs>
            </div>
          </div>

          {/* Right Panel */}
          <div className="space-y-4">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Sample Queries</CardTitle>
              </CardHeader>
              <CardContent className="space-y-1">
                {currentQueries.map((query) => (
                  <button
                    key={query.name}
                    className="w-full text-left px-3 py-2.5 rounded-lg hover:bg-muted transition-colors group"
                    onClick={() => setSql(query.sql)}
                  >
                    <div className="text-sm font-medium group-hover:text-cobalt-600 dark:group-hover:text-cobalt-400 transition-colors">{query.name}</div>
                    <div className="text-[11px] text-muted-foreground truncate font-mono mt-0.5">
                      {query.sql.split('\n')[0].substring(0, 50)}...
                    </div>
                  </button>
                ))}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Schema</CardTitle>
              </CardHeader>
              <CardContent>
                <SchemaExplorer db={dbRef.current} key={schemaKey} />
              </CardContent>
            </Card>

            {history.length > 0 && (
              <Card>
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base flex items-center gap-1.5">
                      <History className="h-3.5 w-3.5" />
                      History
                    </CardTitle>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 text-[10px] text-muted-foreground px-1.5"
                      onClick={() => setHistory([])}
                    >
                      Clear
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className="space-y-1 max-h-48 overflow-y-auto">
                  {history.map((entry, i) => (
                    <button
                      key={i}
                      className="w-full text-left px-2 py-1.5 rounded hover:bg-muted transition-colors group"
                      onClick={() => setSql(entry.sql)}
                    >
                      <div className="flex items-center gap-1.5">
                        <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${entry.ok ? 'bg-emerald-500' : 'bg-red-400'}`} />
                        <span className="text-[11px] font-mono text-muted-foreground truncate group-hover:text-foreground transition-colors">
                          {entry.sql.split('\n')[0].substring(0, 40)}
                        </span>
                      </div>
                      <div className="flex items-center gap-1 ml-3 mt-0.5">
                        <Clock className="h-2.5 w-2.5 text-muted-foreground/50" />
                        <span className="text-[9px] text-muted-foreground/50">{entry.time}</span>
                      </div>
                    </button>
                  ))}
                </CardContent>
              </Card>
            )}

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Shortcuts</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2 text-xs text-muted-foreground">
                  <div className="flex justify-between">
                    <span>Run query</span>
                    <kbd className="px-1.5 py-0.5 bg-muted rounded border font-mono text-[10px]">Ctrl+Enter</kbd>
                  </div>
                  <div className="flex justify-between">
                    <span>Multiple statements</span>
                    <kbd className="px-1.5 py-0.5 bg-muted rounded border font-mono text-[10px]">;</kbd>
                  </div>
                </div>
                <div className="mt-4 pt-3 border-t">
                  <p className="text-[10px] text-muted-foreground">
                    Powered by SQLite via WebAssembly. Full SQL support including JOINs, CTEs, window functions, subqueries, and aggregates.
                  </p>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  )
}

/** Shows tables and their columns from the live database */
function SchemaExplorer({ db }: { db: SqlJsDatabase | null }) {
  if (!db) return <p className="text-xs text-muted-foreground">No database loaded</p>

  let tables: { name: string; columns: string[] }[] = []
  try {
    const result = db.exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    if (result.length > 0) {
      tables = result[0].values.map(([name]: any[]) => {
        const cols = db.exec(`PRAGMA table_info(${name})`)
        const columns = cols.length > 0
          ? cols[0].values.map((row: any[]) => `${row[1]} ${row[2]}`)
          : []
        return { name: String(name), columns: columns.map(String) }
      })
    }
  } catch {
    return <p className="text-xs text-muted-foreground">Unable to read schema</p>
  }

  if (tables.length === 0) {
    return <p className="text-xs text-muted-foreground">No tables yet. Create one!</p>
  }

  return (
    <div className="space-y-3">
      {tables.map((table) => (
        <div key={table.name}>
          <div className="text-xs font-semibold text-cobalt-600 dark:text-cobalt-400 flex items-center gap-1.5 mb-1">
            <Table2 className="h-3 w-3" />
            {table.name}
          </div>
          <div className="pl-4 space-y-0.5">
            {table.columns.map((col, i) => (
              <div key={i} className="text-[11px] font-mono text-muted-foreground">{col}</div>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
