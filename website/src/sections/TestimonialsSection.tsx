import { useEffect, useRef, useState } from 'react'
import { Star, GitFork, Eye, Code2, Package, TestTube, Shield } from 'lucide-react'

const projectStats = [
  { icon: Code2, value: '35,000+', label: 'Lines of Go' },
  { icon: TestTube, value: '5,000+', label: 'Integration Tests' },
  { icon: Package, value: '22', label: 'Packages' },
  { icon: Shield, value: '90%+', label: 'Test Coverage' },
]

const highlights = [
  {
    title: 'Zero Dependencies',
    description: 'Pure Go standard library only. No CGo, no external C libraries, no surprises in your dependency tree.',
  },
  {
    title: 'Battle-Tested',
    description: '107 test suites covering every SQL feature. From edge cases to concurrent access patterns.',
  },
  {
    title: 'Single Import',
    description: 'One go get command. One import. Your app now has a full SQL database with ACID transactions.',
  },
]

export function TestimonialsSection() {
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
            Why CobaltDB
          </span>
          <h2 className="text-3xl md:text-4xl font-bold mb-4 tracking-tight">
            Engineering <span className="text-gradient">Excellence</span>
          </h2>
          <p className="text-lg text-muted-foreground leading-relaxed">
            Every line of code is written with care. No shortcuts, no hacks, no compromises.
          </p>
        </div>

        {/* Stats grid */}
        <div
          className={`grid grid-cols-2 md:grid-cols-4 gap-4 max-w-4xl mx-auto mb-16 transition-all duration-700 ${
            isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
          }`}
        >
          {projectStats.map((stat, i) => (
            <div
              key={stat.label}
              className="text-center p-6 rounded-xl bg-card border border-border/60 hover:border-cobalt-300/60 dark:hover:border-cobalt-700/60 transition-all duration-300 hover:shadow-lg hover:-translate-y-0.5"
              style={{ transitionDelay: `${i * 100}ms` }}
            >
              <stat.icon className="h-5 w-5 mx-auto mb-3 text-cobalt-600 dark:text-cobalt-400" />
              <div className="text-2xl md:text-3xl font-black tabular-nums text-cobalt-600 dark:text-cobalt-400">
                {stat.value}
              </div>
              <div className="text-xs text-muted-foreground mt-1 font-medium">
                {stat.label}
              </div>
            </div>
          ))}
        </div>

        {/* Highlights */}
        <div className="grid md:grid-cols-3 gap-6 max-w-5xl mx-auto">
          {highlights.map((item, i) => (
            <div
              key={item.title}
              className={`relative p-6 rounded-xl bg-card border border-border/60 transition-all duration-700 ${
                isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-6'
              }`}
              style={{ transitionDelay: `${400 + i * 150}ms` }}
            >
              <h3 className="text-base font-semibold mb-2">{item.title}</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                {item.description}
              </p>
            </div>
          ))}
        </div>

        {/* GitHub CTA */}
        <div
          className={`text-center mt-12 transition-all duration-700 ${
            isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'
          }`}
          style={{ transitionDelay: '800ms' }}
        >
          <a
            href="https://github.com/cobaltdb/cobaltdb"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-3 px-6 py-3 rounded-xl bg-card border border-border/60 hover:border-cobalt-300/60 dark:hover:border-cobalt-700/60 transition-all duration-300 hover:shadow-lg group"
          >
            <div className="flex items-center gap-2 text-sm font-medium">
              <Star className="h-4 w-4 text-cobalt-500 group-hover:scale-110 transition-transform" />
              Star on GitHub
            </div>
            <span className="text-muted-foreground text-xs">·</span>
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <GitFork className="h-3 w-3" /> Fork
              </span>
              <span className="flex items-center gap-1">
                <Eye className="h-3 w-3" /> Watch
              </span>
            </div>
          </a>
        </div>
      </div>
    </section>
  )
}
