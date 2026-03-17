import { useEffect, useRef, useState } from 'react'

interface BenchmarkItem {
  name: string
  value: number
  maxValue: number
  highlight?: boolean
  label: string
}

const throughputData: BenchmarkItem[] = [
  { name: 'CobaltDB', value: 98000, maxValue: 100000, highlight: true, label: '98K QPS' },
  { name: 'PostgreSQL', value: 52000, maxValue: 100000, label: '52K QPS' },
  { name: 'MySQL', value: 48000, maxValue: 100000, label: '48K QPS' },
  { name: 'SQLite', value: 45000, maxValue: 100000, label: '45K QPS' },
]

const latencyData: BenchmarkItem[] = [
  { name: 'CobaltDB', value: 0.8, maxValue: 2.5, highlight: true, label: '0.8ms' },
  { name: 'PostgreSQL', value: 1.9, maxValue: 2.5, label: '1.9ms' },
  { name: 'SQLite', value: 2.1, maxValue: 2.5, label: '2.1ms' },
  { name: 'MySQL', value: 2.3, maxValue: 2.5, label: '2.3ms' },
]

function BenchmarkBars({ data, inView }: { data: BenchmarkItem[]; inView: boolean }) {
  return (
    <div className="space-y-4">
      {data.map((item, i) => {
        const percentage = (item.value / item.maxValue) * 100
        return (
          <div key={item.name} className="group">
            <div className="flex items-center justify-between mb-1.5">
              <span className={`text-sm font-medium ${item.highlight ? 'text-cobalt-600 dark:text-cobalt-400' : 'text-muted-foreground'}`}>
                {item.name}
              </span>
              <span className={`text-sm font-semibold tabular-nums ${item.highlight ? 'text-cobalt-600 dark:text-cobalt-400' : 'text-foreground'}`}>
                {item.label}
              </span>
            </div>
            <div className="h-8 bg-muted/60 rounded-lg overflow-hidden relative">
              <div
                className={`h-full rounded-lg transition-all duration-1000 ease-out relative overflow-hidden ${
                  item.highlight
                    ? 'bg-gradient-to-r from-cobalt-600 to-cobalt-400'
                    : 'bg-gradient-to-r from-gray-400/60 to-gray-300/60 dark:from-gray-600/60 dark:to-gray-500/60'
                }`}
                style={{
                  width: inView ? `${percentage}%` : '0%',
                  transitionDelay: `${i * 150}ms`,
                }}
              >
                {/* Shimmer effect on highlighted bar */}
                {item.highlight && (
                  <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent animate-shimmer" style={{ backgroundSize: '200% auto' }} />
                )}
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

export function PerformanceSection() {
  const [activeTab, setActiveTab] = useState<'throughput' | 'latency'>('throughput')
  const [inView, setInView] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setInView(true)
        }
      },
      { threshold: 0.3 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [])

  // Reset animation when switching tabs
  const handleTabChange = (tab: 'throughput' | 'latency') => {
    setInView(false)
    setActiveTab(tab)
    requestAnimationFrame(() => {
      requestAnimationFrame(() => setInView(true))
    })
  }

  return (
    <section className="py-24" ref={ref}>
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-12">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">Performance</span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Blazing <span className="text-gradient">Fast</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            Benchmarked against popular databases on standard hardware.
          </p>
        </div>

        <div className="max-w-3xl mx-auto">
          {/* Custom tab switcher */}
          <div className="flex justify-center mb-10">
            <div className="inline-flex p-1 bg-muted rounded-lg">
              <button
                onClick={() => handleTabChange('throughput')}
                className={`px-5 py-2 rounded-md text-sm font-medium transition-all ${
                  activeTab === 'throughput'
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                Throughput (QPS)
              </button>
              <button
                onClick={() => handleTabChange('latency')}
                className={`px-5 py-2 rounded-md text-sm font-medium transition-all ${
                  activeTab === 'latency'
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                Latency (P99)
              </button>
            </div>
          </div>

          {/* Bars */}
          <div className="bg-card border rounded-xl p-8">
            <BenchmarkBars
              data={activeTab === 'throughput' ? throughputData : latencyData}
              inView={inView}
            />
          </div>

          <p className="text-center text-xs text-muted-foreground mt-6">
            AMD Ryzen 9 5900X, 32GB RAM, NVMe SSD. Read-heavy OLTP workload, 1M rows. In-process (no network overhead).
          </p>
        </div>
      </div>
    </section>
  )
}
