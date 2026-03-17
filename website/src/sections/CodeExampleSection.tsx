import { Tabs, TabsContent, TabsList, TabsTrigger } from '@components/ui/tabs'
import { Check, Copy } from 'lucide-react'
import { useState } from 'react'
import { Button } from '@components/ui/button'
import { highlightCode } from '@lib/syntax'

const codeExamples = {
  go: {
    lang: 'go',
    label: 'Go',
    code: `package main

import (
    "context"
    "log"
    "github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
    // Open database
    db, err := engine.Open("mydb", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Create table
    _, err = db.Exec(ctx, \`
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    \`)

    // Insert data
    _, err = db.Exec(ctx,
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "John Doe", "john@example.com")

    // Query data
    rows, err := db.Query(ctx, "SELECT * FROM users")
    for rows.Next() {
        var id int
        var name, email string
        rows.Scan(&id, &name, &email)
        log.Printf("User: %d, %s, %s", id, name, email)
    }
}`,
  },
  sql: {
    lang: 'sql',
    label: 'SQL',
    code: `-- Create a table with constraints
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10, 2),
    category_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- Insert sample data
INSERT INTO products (name, price, category_id) VALUES
    ('Laptop', 999.99, 1),
    ('Mouse', 29.99, 2),
    ('Keyboard', 79.99, 2);

-- Query with JOIN and aggregation
SELECT
    c.name AS category,
    COUNT(*) AS product_count,
    AVG(p.price) AS avg_price
FROM products p
JOIN categories c ON p.category_id = c.id
GROUP BY c.name
HAVING COUNT(*) > 1;

-- Full-text search
CREATE VIRTUAL TABLE docs USING fts4(title, content);
INSERT INTO docs VALUES ('Getting Started', 'CobaltDB is fast...');
SELECT * FROM docs WHERE docs MATCH 'fast';`,
  },
  wasm: {
    lang: 'js',
    label: 'WASM',
    code: `// In your web application
import { CobaltDB } from '@cobaltdb/wasm';

async function initDatabase() {
    // Load the WASM module
    const db = await CobaltDB.init();

    // Create tables
    db.exec(\`
        CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            title TEXT,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    \`);

    // Insert data
    db.exec(
        "INSERT INTO notes (title, content) VALUES (?, ?)",
        ["Hello", "Running SQL in the browser!"]
    );

    // Query results
    const results = db.query("SELECT * FROM notes");
    console.log(results);

    return results;
}

// Run it
initDatabase().then(data => {
    document.getElementById('output').innerHTML =
        JSON.stringify(data, null, 2);
});`,
  },
}

export function CodeExampleSection() {
  const [copied, setCopied] = useState<string | null>(null)

  const copyToClipboard = async (code: string, lang: string) => {
    await navigator.clipboard.writeText(code)
    setCopied(lang)
    setTimeout(() => setCopied(null), 2000)
  }

  return (
    <section className="py-24 relative">
      <div className="absolute inset-0 bg-gradient-to-b from-muted/30 via-muted/50 to-muted/30" />
      <div className="container mx-auto px-4 relative">
        <div className="text-center max-w-2xl mx-auto mb-12">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">Developer Experience</span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Simple, <span className="text-gradient">Powerful</span> API
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            Use CobaltDB from Go, SQL, or WebAssembly in the browser.
          </p>
        </div>

        <div className="max-w-4xl mx-auto">
          <Tabs defaultValue="go" className="w-full">
            <TabsList className="grid w-full max-w-sm mx-auto grid-cols-3 mb-6">
              {Object.entries(codeExamples).map(([key, example]) => (
                <TabsTrigger key={key} value={key}>{example.label}</TabsTrigger>
              ))}
            </TabsList>

            {Object.entries(codeExamples).map(([key, example]) => (
              <TabsContent key={key} value={key}>
                <div className="relative group">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="absolute top-3 right-3 z-10 opacity-0 group-hover:opacity-100 transition-opacity bg-gray-800/50 hover:bg-gray-800/80 text-gray-300"
                    onClick={() => copyToClipboard(example.code, key)}
                  >
                    {copied === key ? (
                      <><Check className="h-3.5 w-3.5 text-green-400 mr-1" /> Copied</>
                    ) : (
                      <><Copy className="h-3.5 w-3.5 mr-1" /> Copy</>
                    )}
                  </Button>
                  <div className="rounded-xl overflow-hidden border border-gray-200 dark:border-white/10 bg-[#0c0e14] shadow-lg dark:shadow-2xl">
                    {/* File tab header */}
                    <div className="flex items-center px-4 py-2 bg-[#12141c] border-b border-white/5">
                      <div className="flex gap-1.5 mr-4">
                        <div className="w-2.5 h-2.5 rounded-full bg-red-500/60" />
                        <div className="w-2.5 h-2.5 rounded-full bg-yellow-500/60" />
                        <div className="w-2.5 h-2.5 rounded-full bg-green-500/60" />
                      </div>
                      <span className="text-[11px] text-gray-500 font-mono">
                        {key === 'go' ? 'main.go' : key === 'sql' ? 'queries.sql' : 'app.js'}
                      </span>
                    </div>
                    {/* Code with line numbers */}
                    <div className="p-4 overflow-x-auto font-mono text-[13px] leading-6">
                      <div className="table w-full">
                        {highlightCode(example.code, example.lang)}
                      </div>
                    </div>
                  </div>
                </div>
              </TabsContent>
            ))}
          </Tabs>
        </div>
      </div>
    </section>
  )
}
