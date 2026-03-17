import { useEffect, useRef, useState } from 'react'
import {
  Smartphone,
  Wifi,
  TestTube,
  Cpu,
  CloudOff,
  Rocket,
  type LucideIcon,
} from 'lucide-react'

interface UseCase {
  icon: LucideIcon
  title: string
  description: string
  examples: string[]
}

const useCases: UseCase[] = [
  {
    icon: Smartphone,
    title: 'Embedded Applications',
    description: 'Ship a full SQL database inside your Go binary. No external processes, no network calls, no configuration.',
    examples: ['Desktop apps', 'CLI tools', 'Single-binary services'],
  },
  {
    icon: Wifi,
    title: 'Edge Computing',
    description: 'Deploy to edge nodes with minimal footprint. Process data locally with full SQL power, sync when connected.',
    examples: ['CDN workers', 'Edge functions', 'Distributed nodes'],
  },
  {
    icon: TestTube,
    title: 'Testing & CI/CD',
    description: 'Spin up isolated database instances per test. No Docker, no setup, no teardown scripts needed.',
    examples: ['Unit tests', 'Integration tests', 'CI pipelines'],
  },
  {
    icon: Cpu,
    title: 'IoT & Devices',
    description: 'Run on resource-constrained devices. Efficient memory usage and disk-backed storage for persistent data.',
    examples: ['Raspberry Pi', 'Industrial sensors', 'Gateway devices'],
  },
  {
    icon: CloudOff,
    title: 'Offline-First Apps',
    description: 'Full SQL capabilities in the browser via WASM. Build apps that work without internet, sync when online.',
    examples: ['PWAs', 'Field tools', 'Local-first apps'],
  },
  {
    icon: Rocket,
    title: 'Rapid Prototyping',
    description: 'Go from idea to working prototype in minutes. No database server setup, no migrations tooling needed.',
    examples: ['Hackathons', 'MVPs', 'Proof of concepts'],
  },
]

function UseCaseCard({ useCase, index }: { useCase: UseCase; index: number }) {
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
      className={`group relative rounded-xl bg-card border border-border/60 p-6 hover:border-cobalt-300/60 dark:hover:border-cobalt-700/60 transition-all duration-500 hover:shadow-lg hover:shadow-cobalt-500/5 hover:-translate-y-1 ${
        isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-6'
      }`}
      style={{ transitionDelay: `${index * 100}ms` }}
    >
      <div className="w-10 h-10 rounded-lg bg-cobalt-50 dark:bg-cobalt-500/10 flex items-center justify-center mb-4 group-hover:scale-110 transition-transform duration-300">
        <useCase.icon className="h-5 w-5 text-cobalt-600 dark:text-cobalt-400" />
      </div>

      <h3 className="text-base font-semibold mb-2 group-hover:text-cobalt-600 dark:group-hover:text-cobalt-400 transition-colors">
        {useCase.title}
      </h3>

      <p className="text-sm text-muted-foreground leading-relaxed mb-4">
        {useCase.description}
      </p>

      <div className="flex flex-wrap gap-2">
        {useCase.examples.map((example) => (
          <span
            key={example}
            className="inline-flex items-center px-2.5 py-1 rounded-md bg-muted/60 border border-border/50 text-xs font-medium text-muted-foreground"
          >
            {example}
          </span>
        ))}
      </div>
    </div>
  )
}

export function UseCasesSection() {
  return (
    <section className="py-24">
      <div className="container mx-auto px-4">
        <div className="text-center max-w-2xl mx-auto mb-16">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">
            Use Cases
          </span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Built For <span className="text-gradient">Your Stack</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            From embedded devices to browser apps, CobaltDB fits wherever you need a database.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-5 max-w-6xl mx-auto">
          {useCases.map((useCase, i) => (
            <UseCaseCard key={useCase.title} useCase={useCase} index={i} />
          ))}
        </div>
      </div>
    </section>
  )
}
