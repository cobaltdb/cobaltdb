import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { highlightCode } from '@lib/syntax'
import { ArrowRight } from 'lucide-react'

interface ShowcaseItem {
  label: string
  title: string
  description: string
  code: string
  lang: string
  output: {
    columns: string[]
    rows: string[][]
    time: string
  }
  learnMoreLink: string
}

const showcases: ShowcaseItem[] = [
  {
    label: 'Transactions',
    title: 'ACID Transactions with Savepoints',
    description: 'Full serializable isolation with BEGIN, COMMIT, ROLLBACK, and SAVEPOINT support. Data stays consistent under concurrent access — no half-written states.',
    code: `BEGIN;

INSERT INTO accounts (name, balance)
VALUES ('Alice', 1000), ('Bob', 500);

SAVEPOINT before_transfer;

UPDATE accounts SET balance = balance - 200
WHERE name = 'Alice';
UPDATE accounts SET balance = balance + 200
WHERE name = 'Bob';

-- Verify the transfer
SELECT name, balance FROM accounts;`,
    lang: 'sql',
    output: {
      columns: ['name', 'balance'],
      rows: [['Alice', '800'], ['Bob', '700']],
      time: '0.12ms',
    },
    learnMoreLink: '/docs/transactions',
  },
  {
    label: 'Analytics',
    title: 'Window Functions & CTEs',
    description: 'Advanced analytics without leaving SQL. Running totals, rankings, moving averages — computed at the database level for maximum performance.',
    code: `WITH dept_stats AS (
  SELECT department,
    AVG(salary) as avg_sal,
    COUNT(*) as cnt
  FROM employees
  GROUP BY department
)
SELECT
  e.name,
  e.salary,
  ds.avg_sal as dept_avg,
  RANK() OVER (
    PARTITION BY e.department
    ORDER BY e.salary DESC
  ) as dept_rank
FROM employees e
JOIN dept_stats ds
  ON e.department = ds.department;`,
    lang: 'sql',
    output: {
      columns: ['name', 'salary', 'dept_avg', 'dept_rank'],
      rows: [
        ['Alice', '120000', '107500', '1'],
        ['Bob', '95000', '107500', '2'],
        ['Carol', '85000', '80000', '1'],
        ['David', '75000', '80000', '2'],
      ],
      time: '0.31ms',
    },
    learnMoreLink: '/docs/window-functions',
  },
  {
    label: 'Search',
    title: 'Built-in Full-Text Search',
    description: 'FTS4-compatible full-text search with porter stemming, prefix queries, phrase matching, and snippet highlighting. No external search engine needed.',
    code: `-- Create FTS table
CREATE VIRTUAL TABLE docs
USING fts4(title, content, tokenize=porter);

-- Index documents
INSERT INTO docs VALUES
  ('CobaltDB Guide', 'Fast embeddable SQL database'),
  ('Query Tutorial', 'Learn SQL with window functions'),
  ('Go Integration', 'Embed CobaltDB in your Go app');

-- Search with ranking
SELECT title, snippet(docs) as match
FROM docs
WHERE docs MATCH 'SQL database';`,
    lang: 'sql',
    output: {
      columns: ['title', 'match'],
      rows: [
        ['CobaltDB Guide', 'Fast embeddable <b>SQL</b> <b>database</b>'],
        ['Query Tutorial', 'Learn <b>SQL</b> with window functions'],
      ],
      time: '0.09ms',
    },
    learnMoreLink: '/docs/fts',
  },
  {
    label: 'JSON',
    title: 'Native JSON Operations',
    description: 'Store, query, and modify JSON documents directly in SQL. Extract nested values, update fields, and combine relational + document patterns in a single query.',
    code: `CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  type TEXT,
  data JSON
);

INSERT INTO events (type, data) VALUES
  ('signup', '{"user":"alice","plan":"pro"}'),
  ('purchase', '{"user":"bob","amount":99.99}');

SELECT
  type,
  JSON_EXTRACT(data, '$.user') as user,
  JSON_EXTRACT(data, '$.plan') as plan,
  JSON_EXTRACT(data, '$.amount') as amount
FROM events;`,
    lang: 'sql',
    output: {
      columns: ['type', 'user', 'plan', 'amount'],
      rows: [
        ['signup', 'alice', 'pro', 'NULL'],
        ['purchase', 'bob', 'NULL', '99.99'],
      ],
      time: '0.06ms',
    },
    learnMoreLink: '/docs/json',
  },
]

function OutputTable({ output }: { output: ShowcaseItem['output'] }) {
  return (
    <div className="rounded-lg border border-gray-200 dark:border-white/10 bg-[#0a0c12] overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-[13px] font-mono">
          <thead>
            <tr className="border-b border-white/5 bg-white/[0.02]">
              {output.columns.map((col, i) => (
                <th key={i} className="px-4 py-2 text-left text-cobalt-300 font-semibold text-xs">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {output.rows.map((row, ri) => (
              <tr key={ri} className="border-b border-white/[0.03] last:border-0">
                {row.map((cell, ci) => (
                  <td key={ci} className={`px-4 py-1.5 ${cell === 'NULL' ? 'text-gray-600 italic' : 'text-emerald-300'}`}>
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="px-4 py-1.5 border-t border-white/5 text-[11px] text-gray-500 flex items-center gap-2">
        <span>{output.rows.length} row{output.rows.length !== 1 ? 's' : ''}</span>
        <span className="text-gray-700">·</span>
        <span>{output.time}</span>
      </div>
    </div>
  )
}

function ShowcaseCard({ item, index }: { item: ShowcaseItem; index: number }) {
  const ref = useRef<HTMLDivElement>(null)
  const [isVisible, setIsVisible] = useState(false)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true)
          observer.disconnect()
        }
      },
      { threshold: 0.15 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [])

  const isReversed = index % 2 === 1

  return (
    <div
      ref={ref}
      className={`grid lg:grid-cols-2 gap-8 lg:gap-12 items-start transition-all duration-700 ${
        isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'
      }`}
    >
      {/* Text side */}
      <div className={`${isReversed ? 'lg:order-2' : ''} pt-4`}>
        <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">
          {item.label}
        </span>
        <h3 className="text-2xl md:text-3xl font-bold mb-4 tracking-tight">
          {item.title}
        </h3>
        <p className="text-muted-foreground leading-relaxed mb-6">
          {item.description}
        </p>
        <Link
          to={item.learnMoreLink}
          className="inline-flex items-center gap-1.5 text-sm font-medium text-cobalt-600 dark:text-cobalt-400 hover:underline underline-offset-4 group"
        >
          Learn more
          <ArrowRight className="h-3.5 w-3.5 transition-transform group-hover:translate-x-1" />
        </Link>
      </div>

      {/* Code + output side */}
      <div className={`${isReversed ? 'lg:order-1' : ''} space-y-3`}>
        {/* Code block */}
        <div className="rounded-xl overflow-hidden border border-gray-200 dark:border-white/10 bg-[#0c0e14] shadow-lg dark:shadow-xl">
          <div className="flex items-center px-4 py-2 bg-[#11131a] border-b border-white/5">
            <div className="flex gap-1.5 mr-3">
              <div className="w-2 h-2 rounded-full bg-red-500/50" />
              <div className="w-2 h-2 rounded-full bg-yellow-500/50" />
              <div className="w-2 h-2 rounded-full bg-green-500/50" />
            </div>
            <span className="text-[11px] text-gray-500 font-mono">query.sql</span>
          </div>
          <div className="p-4 overflow-x-auto font-mono text-[13px] leading-6">
            <div className="table w-full">
              {highlightCode(item.code, item.lang)}
            </div>
          </div>
        </div>

        {/* Output table */}
        <OutputTable output={item.output} />
      </div>
    </div>
  )
}

export function ShowcaseSection() {
  return (
    <section className="py-24">
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-20">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">See It In Action</span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Real SQL, <span className="text-gradient">Real Results</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            Every feature works end-to-end. Here's what CobaltDB can do.
          </p>
        </div>

        <div className="max-w-6xl mx-auto space-y-24">
          {showcases.map((item, i) => (
            <ShowcaseCard key={item.label} item={item} index={i} />
          ))}
        </div>
      </div>
    </section>
  )
}
