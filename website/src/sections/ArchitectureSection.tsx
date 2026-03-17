import { useEffect, useRef, useState } from 'react'

const layers = [
  {
    name: 'SQL Interface',
    description: 'Parser, Lexer, Cost-Based Optimizer',
    items: ['SQL Parser', 'Query Planner', 'Cost Optimizer'],
  },
  {
    name: 'Execution Engine',
    description: 'Transactions, Catalog, Expression Evaluator',
    items: ['ACID Transactions', 'Catalog Manager', 'Expression Engine'],
  },
  {
    name: 'Access Methods',
    description: 'Indexes, Full-Text Search, JSON Engine',
    items: ['B-Tree Indexes', 'FTS4 Engine', 'JSON Functions'],
  },
  {
    name: 'Storage Layer',
    description: 'Buffer Pool, WAL, Page Management',
    items: ['Buffer Pool', 'WAL Logger', 'Page Manager'],
  },
  {
    name: 'Security & Monitoring',
    description: 'Encryption, RLS, Audit Logging, Metrics',
    items: ['AES Encryption', 'Row-Level Security', 'Audit Log'],
  },
]

const stats = [
  { label: 'Packages', value: '22' },
  { label: 'Test Suites', value: '107' },
  { label: 'Integration Tests', value: '5,000+' },
  { label: 'Dependencies', value: '0' },
]

export function ArchitectureSection() {
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
      { threshold: 0.1 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [])

  return (
    <section className="py-24 relative" ref={ref}>
      <div className="absolute inset-0 bg-gradient-to-b from-muted/30 via-muted/50 to-muted/30" />

      <div className="container mx-auto px-4 relative">
        <div className="text-center max-w-2xl mx-auto mb-16">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">
            Architecture
          </span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Built From <span className="text-gradient">The Ground Up</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            Every layer is written in pure Go with zero external dependencies.
            A clean, modular architecture designed for reliability.
          </p>
        </div>

        <div className="max-w-4xl mx-auto">
          {/* Architecture stack */}
          <div className="space-y-3">
            {layers.map((layer, i) => (
              <div
                key={layer.name}
                className={`rounded-xl border border-cobalt-500/20 dark:border-cobalt-500/30 bg-cobalt-50/50 dark:bg-cobalt-500/5 p-5 transition-all duration-700 ${
                  isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-8'
                }`}
                style={{ transitionDelay: `${i * 120}ms` }}
              >
                <div className="flex flex-col md:flex-row md:items-center gap-3 md:gap-6">
                  <div className="md:w-48 shrink-0">
                    <h3 className="text-sm font-bold text-cobalt-600 dark:text-cobalt-400">
                      {layer.name}
                    </h3>
                    <p className="text-[11px] text-muted-foreground mt-0.5">
                      {layer.description}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {layer.items.map((item) => (
                      <span
                        key={item}
                        className="inline-flex items-center px-3 py-1 rounded-md bg-gray-100 dark:bg-white/5 border border-gray-200 dark:border-white/10 text-xs font-mono text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-white/10 transition-colors"
                      >
                        {item}
                      </span>
                    ))}
                  </div>
                </div>

                {/* Arrow connector */}
                {i < layers.length - 1 && (
                  <div className="flex justify-center mt-3 -mb-5">
                    <div className="w-px h-3 bg-gray-300 dark:bg-white/20" />
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Stats bar */}
          <div
            className={`grid grid-cols-2 md:grid-cols-4 gap-4 mt-12 transition-all duration-700 ${
              isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
            }`}
            style={{ transitionDelay: '800ms' }}
          >
            {stats.map((stat) => (
              <div key={stat.label} className="text-center p-4 rounded-lg bg-card border border-border/60">
                <div className="text-2xl font-black text-cobalt-600 dark:text-cobalt-400 tabular-nums">
                  {stat.value}
                </div>
                <div className="text-xs text-muted-foreground mt-1 font-medium">
                  {stat.label}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
