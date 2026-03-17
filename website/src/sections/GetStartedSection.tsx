import { useState } from 'react'
import { Check, Copy, Terminal, ArrowRight } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Button } from '@components/ui/button'
import { highlightCode } from '@lib/syntax'

const tabs = [
  {
    id: 'quickstart',
    label: 'Quick Start',
    icon: Terminal,
    steps: [
      {
        title: 'Install CobaltDB',
        code: 'go get github.com/cobaltdb/cobaltdb',
        lang: 'bash',
      },
      {
        title: 'Open a database',
        code: `db, err := engine.Open("myapp.db", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()`,
        lang: 'go',
      },
      {
        title: 'Run queries',
        code: `ctx := context.Background()

// Create table
db.Exec(ctx, \`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
)\`)

// Insert data
db.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)",
    "Alice", "alice@example.com")

// Query
rows, _ := db.Query(ctx, "SELECT * FROM users")
for rows.Next() {
    var id int
    var name, email string
    rows.Scan(&id, &name, &email)
    fmt.Printf("%d: %s (%s)\\n", id, name, email)
}`,
        lang: 'go',
      },
    ],
  },
  {
    id: 'advanced',
    label: 'Advanced SQL',
    icon: Terminal,
    steps: [
      {
        title: 'Transactions with Savepoints',
        code: `BEGIN;

INSERT INTO orders (customer_id, total) VALUES (1, 99.99);
SAVEPOINT before_items;

INSERT INTO order_items (order_id, product, qty)
VALUES (1, 'Widget', 3), (1, 'Gadget', 1);

-- Oops, rollback items only
ROLLBACK TO before_items;

INSERT INTO order_items (order_id, product, qty)
VALUES (1, 'Widget', 2);

COMMIT;`,
        lang: 'sql',
      },
      {
        title: 'Window Functions & CTEs',
        code: `WITH monthly_sales AS (
  SELECT
    DATE(created_at, 'start of month') as month,
    SUM(amount) as total
  FROM sales
  GROUP BY 1
)
SELECT
  month,
  total,
  SUM(total) OVER (
    ORDER BY month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as running_total,
  RANK() OVER (ORDER BY total DESC) as rank
FROM monthly_sales;`,
        lang: 'sql',
      },
      {
        title: 'Full-Text Search',
        code: `-- Create FTS table
CREATE VIRTUAL TABLE articles
USING fts4(title, body, tokenize=porter);

-- Index content
INSERT INTO articles VALUES
  ('Getting Started', 'CobaltDB is an embeddable SQL database...'),
  ('Performance', 'Optimized B-tree storage with buffer pool...');

-- Search with snippets
SELECT title, snippet(articles, '<b>', '</b>', '...', 1, 30)
FROM articles
WHERE articles MATCH 'database OR storage';`,
        lang: 'sql',
      },
    ],
  },
]

function CodeBlock({ code, lang }: { code: string; lang: string }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="relative group rounded-lg overflow-hidden border border-gray-200 dark:border-white/10 bg-[#0c0e14]">
      <button
        onClick={handleCopy}
        className="absolute top-2 right-2 z-10 p-1.5 rounded-md bg-white/5 hover:bg-white/10 opacity-0 group-hover:opacity-100 transition-opacity"
      >
        {copied ? (
          <Check className="h-3.5 w-3.5 text-emerald-400" />
        ) : (
          <Copy className="h-3.5 w-3.5 text-gray-400" />
        )}
      </button>
      <div className="p-4 overflow-x-auto font-mono text-[13px] leading-6">
        <div className="table w-full">
          {lang === 'bash' ? (
            <div className="table-row">
              <span className="table-cell text-right pr-4 select-none text-gray-600 text-xs w-8">$</span>
              <span className="table-cell text-emerald-300">{code}</span>
            </div>
          ) : (
            highlightCode(code, lang)
          )}
        </div>
      </div>
    </div>
  )
}

export function GetStartedSection() {
  const [activeTab, setActiveTab] = useState('quickstart')
  const activeContent = tabs.find((t) => t.id === activeTab)!

  return (
    <section className="py-24">
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-12">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">
            Get Started
          </span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Up and Running in <span className="text-gradient">Minutes</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            From installation to your first query in three simple steps.
          </p>
        </div>

        <div className="max-w-4xl mx-auto">
          {/* Tab switcher */}
          <div className="flex justify-center mb-10">
            <div className="inline-flex p-1 bg-muted rounded-lg">
              {tabs.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`px-5 py-2 rounded-md text-sm font-medium transition-all flex items-center gap-2 ${
                    activeTab === tab.id
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  <tab.icon className="h-3.5 w-3.5" />
                  {tab.label}
                </button>
              ))}
            </div>
          </div>

          {/* Steps */}
          <div className="space-y-6">
            {activeContent.steps.map((step, i) => (
              <div key={`${activeTab}-${i}`} className="relative">
                {/* Step number */}
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-7 h-7 rounded-full bg-cobalt-600 text-white text-xs font-bold flex items-center justify-center shrink-0">
                    {i + 1}
                  </div>
                  <h3 className="text-sm font-semibold">{step.title}</h3>
                </div>
                <div className="ml-10">
                  <CodeBlock code={step.code} lang={step.lang} />
                </div>

                {/* Connector line */}
                {i < activeContent.steps.length - 1 && (
                  <div className="absolute left-[13px] top-10 bottom-0 w-px bg-border" />
                )}
              </div>
            ))}
          </div>

          {/* CTA */}
          <div className="text-center mt-12">
            <Link to="/playground">
              <Button size="lg" className="bg-cobalt-600 hover:bg-cobalt-700 text-white shadow-lg shadow-cobalt-600/25 group">
                Try It Live in Browser
                <ArrowRight className="h-4 w-4 ml-2 transition-transform group-hover:translate-x-1" />
              </Button>
            </Link>
          </div>
        </div>
      </div>
    </section>
  )
}
