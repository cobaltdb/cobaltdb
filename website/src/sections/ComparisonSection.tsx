import { Check, X, Minus } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

type Support = 'yes' | 'no' | 'partial'

interface FeatureRow {
  feature: string
  cobaltdb: Support
  sqlite: Support
  postgres: Support
}

const features: FeatureRow[] = [
  { feature: 'Embeddable (in-process)', cobaltdb: 'yes', sqlite: 'yes', postgres: 'no' },
  { feature: 'Zero Dependencies', cobaltdb: 'yes', sqlite: 'yes', postgres: 'no' },
  { feature: 'ACID Transactions', cobaltdb: 'yes', sqlite: 'yes', postgres: 'yes' },
  { feature: 'Window Functions', cobaltdb: 'yes', sqlite: 'yes', postgres: 'yes' },
  { feature: 'Full-Text Search', cobaltdb: 'yes', sqlite: 'partial', postgres: 'yes' },
  { feature: 'JSON Operations', cobaltdb: 'yes', sqlite: 'partial', postgres: 'yes' },
  { feature: 'WASM Support', cobaltdb: 'yes', sqlite: 'partial', postgres: 'no' },
  { feature: 'Row-Level Security', cobaltdb: 'yes', sqlite: 'no', postgres: 'yes' },
  { feature: 'Encryption at Rest', cobaltdb: 'yes', sqlite: 'no', postgres: 'partial' },
  { feature: 'Vector Search (HNSW)', cobaltdb: 'yes', sqlite: 'no', postgres: 'partial' },
  { feature: 'Query Plan Cache', cobaltdb: 'yes', sqlite: 'no', postgres: 'yes' },
  { feature: 'Temporal Queries', cobaltdb: 'yes', sqlite: 'no', postgres: 'partial' },
  { feature: 'Master-Slave Replication', cobaltdb: 'yes', sqlite: 'no', postgres: 'yes' },
  { feature: 'Hot Backup', cobaltdb: 'yes', sqlite: 'partial', postgres: 'yes' },
  { feature: 'Written in Go', cobaltdb: 'yes', sqlite: 'no', postgres: 'no' },
]

function SupportIcon({ support }: { support: Support }) {
  if (support === 'yes') return (
    <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-emerald-100 dark:bg-emerald-500/10">
      <Check className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />
    </span>
  )
  if (support === 'no') return (
    <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-red-100/50 dark:bg-red-500/5">
      <X className="h-3.5 w-3.5 text-red-400/60" />
    </span>
  )
  return (
    <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-amber-100/50 dark:bg-amber-500/10">
      <Minus className="h-3.5 w-3.5 text-amber-500" />
    </span>
  )
}

export function ComparisonSection() {
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
    <section className="py-24" ref={ref}>
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-12">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">Comparison</span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            How CobaltDB <span className="text-gradient">Compares</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            The best of both worlds: embeddable like SQLite, feature-rich like PostgreSQL.
          </p>
        </div>

        <div className="max-w-3xl mx-auto">
          <div
            className={`rounded-xl border bg-card overflow-hidden shadow-sm transition-all duration-700 ${
              isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-6'
            }`}
          >
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-muted/50 border-b">
                    <th className="px-6 py-3.5 text-left text-xs font-semibold text-muted-foreground uppercase tracking-wider">Feature</th>
                    <th className="px-4 py-3.5 text-center">
                      <div className="text-xs font-bold text-cobalt-600 dark:text-cobalt-400 uppercase tracking-wider">CobaltDB</div>
                    </th>
                    <th className="px-4 py-3.5 text-center text-xs font-semibold text-muted-foreground uppercase tracking-wider">SQLite</th>
                    <th className="px-4 py-3.5 text-center text-xs font-semibold text-muted-foreground uppercase tracking-wider">PostgreSQL</th>
                  </tr>
                </thead>
                <tbody>
                  {features.map((row, idx) => (
                    <tr
                      key={row.feature}
                      className={`border-b last:border-0 comparison-row transition-all duration-500 ${
                        isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-4'
                      }`}
                      style={{ transitionDelay: `${200 + idx * 60}ms` }}
                    >
                      <td className="px-6 py-3.5 text-sm font-medium">{row.feature}</td>
                      <td className="px-4 py-3.5 text-center">
                        <span className="inline-flex justify-center">
                          <SupportIcon support={row.cobaltdb} />
                        </span>
                      </td>
                      <td className="px-4 py-3.5 text-center">
                        <span className="inline-flex justify-center">
                          <SupportIcon support={row.sqlite} />
                        </span>
                      </td>
                      <td className="px-4 py-3.5 text-center">
                        <span className="inline-flex justify-center">
                          <SupportIcon support={row.postgres} />
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="flex items-center justify-center gap-6 mt-6 text-xs text-muted-foreground">
            <div className="flex items-center gap-1.5">
              <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-emerald-100 dark:bg-emerald-500/10">
                <Check className="h-3 w-3 text-emerald-600 dark:text-emerald-400" />
              </span>
              Full Support
            </div>
            <div className="flex items-center gap-1.5">
              <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-amber-100/50 dark:bg-amber-500/10">
                <Minus className="h-3 w-3 text-amber-500" />
              </span>
              Partial
            </div>
            <div className="flex items-center gap-1.5">
              <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-red-100/50 dark:bg-red-500/5">
                <X className="h-3 w-3 text-red-400/60" />
              </span>
              Not Available
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
