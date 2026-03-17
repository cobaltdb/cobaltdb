import { useEffect, useRef, useState } from 'react'
import {
  Zap,
  Shield,
  Database,
  Search,
  Code2,
  Lock,
  Layers,
  Workflow,
  Globe
} from 'lucide-react'

const features = [
  {
    icon: Zap,
    title: 'High Performance',
    description: 'B-tree storage with buffer pool caching delivers 100K+ QPS. Optimized for read-heavy workloads.',
  },
  {
    icon: Shield,
    title: 'ACID Compliant',
    description: 'Full transaction support with serializable isolation. Data stays consistent under concurrent access.',
  },
  {
    icon: Database,
    title: 'Embeddable',
    description: 'Zero external dependencies. Import the Go package and run a full SQL database inside your app.',
  },
  {
    icon: Search,
    title: 'Full-Text Search',
    description: 'Built-in FTS4/FTS5 compatible search with tokenizers, rankings, and snippet highlighting.',
  },
  {
    icon: Code2,
    title: 'Modern SQL',
    description: 'CTEs, window functions, subqueries, JSON ops, triggers, and views. PostgreSQL-compatible syntax.',
  },
  {
    icon: Lock,
    title: 'Security First',
    description: 'Encryption at rest, TLS, audit logging, and row-level security for multi-tenant apps.',
  },
  {
    icon: Layers,
    title: 'WASM Support',
    description: 'Compile to WebAssembly and run in the browser. Perfect for demos, testing, and offline-first.',
  },
  {
    icon: Workflow,
    title: 'Advanced Features',
    description: 'Materialized views, recursive CTEs, set operations (UNION/INTERSECT/EXCEPT), and savepoints.',
  },
  {
    icon: Globe,
    title: 'Production Ready',
    description: '16 packages at 90%+ coverage. Battle-tested with 5,000+ integration tests across 107 suites.',
  },
]

function FeatureCard({ feature, index }: { feature: typeof features[0]; index: number }) {
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
      { threshold: 0.2 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [])

  return (
    <div
      ref={ref}
      className={`group relative p-6 rounded-xl bg-card border border-border/60 hover:border-cobalt-300/60 dark:hover:border-cobalt-700/60 transition-all duration-500 hover:shadow-lg hover:shadow-cobalt-500/5 hover:-translate-y-1 ${
        isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-6'
      }`}
      style={{ transitionDelay: `${index * 80}ms` }}
    >
      <div className="w-10 h-10 rounded-lg bg-cobalt-50 dark:bg-cobalt-500/10 flex items-center justify-center mb-4 group-hover:scale-110 transition-transform duration-300">
        <feature.icon className="h-5 w-5 text-cobalt-600 dark:text-cobalt-400" />
      </div>
      <h3 className="text-base font-semibold mb-2 group-hover:text-cobalt-600 dark:group-hover:text-cobalt-400 transition-colors">
        {feature.title}
      </h3>
      <p className="text-muted-foreground text-sm leading-relaxed">
        {feature.description}
      </p>
    </div>
  )
}

export function FeaturesSection() {
  return (
    <section id="features" className="py-24 relative">
      <div className="absolute inset-0 bg-gradient-to-b from-muted/30 via-muted/50 to-muted/30" />

      <div className="container mx-auto px-4 relative">
        <div className="text-center max-w-2xl mx-auto mb-16">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">Features</span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Everything You Need in a{' '}
            <span className="text-gradient">Database</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            The simplicity of SQLite with the power of PostgreSQL,
            written from the ground up in Go.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-5 max-w-6xl mx-auto">
          {features.map((feature, i) => (
            <FeatureCard key={feature.title} feature={feature} index={i} />
          ))}
        </div>
      </div>
    </section>
  )
}
