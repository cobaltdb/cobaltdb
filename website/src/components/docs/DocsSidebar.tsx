import { Link } from 'react-router-dom'
import { cn } from '@lib/utils'

const sidebarNav = [
  {
    title: 'Getting Started',
    items: [
      { title: 'Introduction', href: '/docs/getting-started' },
      { title: 'Installation', href: '/docs/installation' },
      { title: 'Quick Start', href: '/docs/quick-start' },
    ],
  },
  {
    title: 'Core Concepts',
    items: [
      { title: 'Database Engine', href: '/docs/engine' },
      { title: 'SQL Reference', href: '/docs/sql' },
      { title: 'Transactions', href: '/docs/transactions' },
      { title: 'Indexes', href: '/docs/indexes' },
    ],
  },
  {
    title: 'Advanced',
    items: [
      { title: 'Window Functions', href: '/docs/window-functions' },
      { title: 'CTEs & Subqueries', href: '/docs/ctes' },
      { title: 'Full-Text Search', href: '/docs/fts' },
      { title: 'JSON Support', href: '/docs/json' },
      { title: 'Triggers', href: '/docs/triggers' },
      { title: 'Views', href: '/docs/views' },
    ],
  },
  {
    title: 'Integration',
    items: [
      { title: 'WASM Guide', href: '/docs/wasm' },
      { title: 'Go API', href: '/docs/api' },
      { title: 'Security', href: '/docs/security' },
    ],
  },
]

interface DocsSidebarProps {
  currentSection: string
}

export function DocsSidebar({ currentSection }: DocsSidebarProps) {
  return (
    <nav className="sticky top-24 space-y-6">
      {sidebarNav.map((group) => (
        <div key={group.title}>
          <h4 className="text-[11px] font-semibold mb-2 text-muted-foreground uppercase tracking-wider">
            {group.title}
          </h4>
          <ul className="space-y-0.5">
            {group.items.map((item) => {
              const isActive = item.href === `/docs/${currentSection}`
              return (
                <li key={item.href}>
                  <Link
                    to={item.href}
                    className={cn(
                      'block text-sm py-1.5 px-3 rounded-lg transition-all duration-200',
                      isActive
                        ? 'bg-cobalt-100 dark:bg-cobalt-900/40 text-cobalt-700 dark:text-cobalt-300 font-medium border border-cobalt-200/50 dark:border-cobalt-800/50'
                        : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                    )}
                  >
                    {item.title}
                  </Link>
                </li>
              )
            })}
          </ul>
        </div>
      ))}
    </nav>
  )
}
