import { Link } from 'react-router-dom'
import { Button } from '@components/ui/button'
import { ArrowRight, Play, Github, ChevronRight, Terminal } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

function AnimatedCounter({ target, suffix = '' }: { target: number; suffix?: string }) {
  const [count, setCount] = useState(0)
  const ref = useRef<HTMLDivElement>(null)
  const hasAnimated = useRef(false)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !hasAnimated.current) {
          hasAnimated.current = true
          const duration = 2000
          const steps = 60
          const increment = target / steps
          let current = 0
          const timer = setInterval(() => {
            current += increment
            if (current >= target) {
              setCount(target)
              clearInterval(timer)
            } else {
              setCount(Math.floor(current))
            }
          }, duration / steps)
        }
      },
      { threshold: 0.5 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [target])

  return (
    <div ref={ref} className="text-3xl md:text-4xl font-black tracking-tight text-cobalt-600 dark:text-cobalt-400">
      {count.toLocaleString()}{suffix}
    </div>
  )
}

interface DemoLine {
  type: 'prompt' | 'output' | 'table' | 'info'
  text?: string
  columns?: string[]
  rows?: string[][]
  delay: number
}

const demoScript: DemoLine[] = [
  { type: 'prompt', text: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT);", delay: 0 },
  { type: 'info', text: 'Query OK. 0 row(s) affected. (0.02ms)', delay: 600 },
  { type: 'prompt', text: "INSERT INTO users VALUES (1,'Alice','admin'), (2,'Bob','user'), (3,'Carol','admin');", delay: 1200 },
  { type: 'info', text: 'Query OK. 3 row(s) affected. (0.05ms)', delay: 1800 },
  { type: 'prompt', text: "SELECT name, role, RANK() OVER (ORDER BY name) as rnk FROM users WHERE role = 'admin';", delay: 2600 },
  {
    type: 'table',
    columns: ['name', 'role', 'rnk'],
    rows: [['Alice', 'admin', '1'], ['Carol', 'admin', '2']],
    delay: 3400,
  },
  { type: 'info', text: '2 rows returned (0.08ms)', delay: 3400 },
]

function TypingText({ text, startDelay }: { text: string; startDelay: number }) {
  const [displayed, setDisplayed] = useState('')
  const [done, setDone] = useState(false)

  useEffect(() => {
    let idx = 0
    let timer: ReturnType<typeof setTimeout>

    const startTimer = setTimeout(() => {
      const interval = setInterval(() => {
        idx++
        if (idx >= text.length) {
          setDisplayed(text)
          setDone(true)
          clearInterval(interval)
        } else {
          setDisplayed(text.slice(0, idx))
        }
      }, 12)
      timer = interval as any
    }, startDelay)

    return () => {
      clearTimeout(startTimer)
      if (timer) clearInterval(timer)
    }
  }, [text, startDelay])

  return (
    <>
      <span className="text-gray-200">{displayed}</span>
      {!done && <span className="inline-block w-1.5 h-4 bg-cobalt-400 ml-0.5 animate-pulse align-middle" />}
    </>
  )
}

function InteractiveTerminal() {
  const [visibleCount, setVisibleCount] = useState(0)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const timers: ReturnType<typeof setTimeout>[] = []
    demoScript.forEach((_, i) => {
      timers.push(setTimeout(() => {
        setVisibleCount(i + 1)
        if (containerRef.current) {
          containerRef.current.scrollTop = containerRef.current.scrollHeight
        }
      }, demoScript[i].delay))
    })
    return () => timers.forEach(clearTimeout)
  }, [])

  const visibleLines = demoScript.slice(0, visibleCount)

  return (
    <div className="rounded-xl overflow-hidden border border-gray-200 dark:border-white/10 bg-[#0a0c12] shadow-2xl shadow-cobalt-950/10 dark:shadow-cobalt-950/30">
      {/* Terminal header */}
      <div className="flex items-center gap-2 px-4 py-2.5 bg-[#11131a] border-b border-white/5">
        <div className="flex gap-1.5">
          <div className="w-2.5 h-2.5 rounded-full bg-red-500/70 hover:bg-red-500 transition-colors" />
          <div className="w-2.5 h-2.5 rounded-full bg-yellow-500/70 hover:bg-yellow-500 transition-colors" />
          <div className="w-2.5 h-2.5 rounded-full bg-green-500/70 hover:bg-green-500 transition-colors" />
        </div>
        <div className="flex-1 text-center">
          <span className="text-[11px] text-gray-500 font-mono">cobaltdb — in-memory</span>
        </div>
        <Terminal className="h-3 w-3 text-gray-600" />
      </div>

      {/* Terminal body */}
      <div ref={containerRef} className="p-4 font-mono text-[13px] leading-6 min-h-[280px] max-h-[340px] overflow-y-auto">
        {visibleLines.map((line, i) => {
          if (line.type === 'prompt') {
            return (
              <div key={i} className="flex items-start gap-2 mb-1">
                <span className="text-cobalt-400 select-none shrink-0">cobaltdb&gt;</span>
                <TypingText text={line.text!} startDelay={0} />
              </div>
            )
          }
          if (line.type === 'info') {
            return (
              <div key={i} className="text-gray-500 text-xs mb-2 pl-1 animate-fade-in">{line.text}</div>
            )
          }
          if (line.type === 'table' && line.columns && line.rows) {
            const widths = line.columns.map((col, ci) =>
              Math.max(col.length, ...line.rows!.map(r => r[ci]?.length || 0))
            )
            const pad = (s: string, w: number) => s + ' '.repeat(Math.max(0, w - s.length))
            const separator = '+-' + widths.map(w => '-'.repeat(w)).join('-+-') + '-+'

            return (
              <div key={i} className="text-xs mb-1 text-gray-300 animate-fade-in">
                <div className="text-gray-600">{separator}</div>
                <div>
                  {'| '}
                  {line.columns.map((col, ci) => (
                    <span key={ci}>
                      <span className="text-cobalt-300 font-semibold">{pad(col, widths[ci])}</span>
                      {ci < line.columns!.length - 1 ? ' | ' : ''}
                    </span>
                  ))}
                  {' |'}
                </div>
                <div className="text-gray-600">{separator}</div>
                {line.rows.map((row, ri) => (
                  <div key={ri}>
                    {'| '}
                    {row.map((cell, ci) => (
                      <span key={ci}>
                        <span className="text-emerald-300">{pad(cell, widths[ci])}</span>
                        {ci < row.length - 1 ? ' | ' : ''}
                      </span>
                    ))}
                    {' |'}
                  </div>
                ))}
                <div className="text-gray-600">{separator}</div>
              </div>
            )
          }
          return null
        })}

        {/* Blinking cursor */}
        {visibleCount >= demoScript.length && (
          <div className="flex items-center gap-2 mt-1">
            <span className="text-cobalt-400 select-none">cobaltdb&gt;</span>
            <span className="w-2 h-4 bg-cobalt-400/70 animate-pulse" />
          </div>
        )}
        {visibleCount > 0 && visibleCount < demoScript.length && (
          <div className="flex items-center gap-2">
            <span className="text-cobalt-400 select-none">cobaltdb&gt;</span>
            <span className="w-2 h-4 bg-cobalt-400 animate-pulse" />
          </div>
        )}
      </div>
    </div>
  )
}

export function HeroSection() {
  return (
    <section className="relative overflow-hidden hero-radial">
      {/* Dot grid background */}
      <div className="absolute inset-0 dot-grid opacity-40 dark:opacity-60" />

      {/* Gradient orbs */}
      <div className="absolute top-0 right-1/4 w-[500px] h-[500px] bg-cobalt-400/5 dark:bg-cobalt-400/8 rounded-full blur-[100px] animate-glow-pulse" />
      <div className="absolute bottom-0 left-1/4 w-[400px] h-[400px] bg-cobalt-600/5 dark:bg-cobalt-600/8 rounded-full blur-[100px] animate-glow-pulse" style={{ animationDelay: '1s' }} />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-cobalt-500/3 rounded-full blur-[120px] animate-glow-pulse" style={{ animationDelay: '2s' }} />

      <div className="container mx-auto px-4 relative pt-16 pb-20 lg:pt-28 lg:pb-28">
        {/* Centered heading */}
        <div className="text-center max-w-4xl mx-auto mb-12">
          {/* Version badge */}
          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-cobalt-100/80 dark:bg-cobalt-900/40 text-cobalt-700 dark:text-cobalt-300 text-xs font-semibold mb-6 border border-cobalt-200/50 dark:border-cobalt-800/50 animate-fade-in-down backdrop-blur-sm">
            <span className="flex h-2 w-2 rounded-full bg-cobalt-500 animate-pulse" />
            v0.2.22 — WASM, Vector Search & Temporal Queries
            <ChevronRight className="h-3 w-3" />
          </div>

          <h1 className="text-4xl md:text-5xl lg:text-[3.5rem] font-black tracking-tight mb-6 animate-fade-in-up leading-[1.1]">
            The SQL Database That{' '}
            <span className="text-gradient-animated">Ships With Your App</span>
          </h1>

          <p className="text-lg md:text-xl text-muted-foreground mb-8 max-w-2xl mx-auto leading-relaxed animate-fade-in-up stagger-2">
            Pure Go. Zero dependencies. ACID transactions, cost-based optimizer,
            window functions, JSON support, full-text search, and encryption at rest.
          </p>

          {/* CTA Buttons */}
          <div className="flex flex-col sm:flex-row items-center justify-center gap-3 animate-fade-in-up stagger-3">
            <Link to="/playground">
              <Button size="lg" className="bg-cobalt-600 hover:bg-cobalt-700 text-white shadow-lg shadow-cobalt-600/25 hover:shadow-cobalt-600/40 transition-all text-base px-7 group">
                <Play className="h-4 w-4 mr-2 transition-transform group-hover:scale-110" />
                Try in Browser
              </Button>
            </Link>
            <Link to="/docs">
              <Button size="lg" variant="outline" className="text-base px-7 group">
                Get Started
                <ArrowRight className="h-4 w-4 ml-2 transition-transform group-hover:translate-x-1" />
              </Button>
            </Link>
            <a href="https://github.com/cobaltdb/cobaltdb" target="_blank" rel="noopener noreferrer">
              <Button size="lg" variant="ghost" className="text-base px-5 group">
                <Github className="h-4 w-4 mr-2 transition-transform group-hover:scale-110" />
                GitHub
              </Button>
            </a>
          </div>
        </div>

        {/* Interactive terminal demo */}
        <div className="max-w-3xl mx-auto animate-fade-in-up stagger-4">
          <InteractiveTerminal />
        </div>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-6 max-w-2xl mx-auto mt-16 animate-fade-in-up stagger-5">
          <div className="text-center p-4 rounded-xl hover:bg-card/50 transition-colors">
            <AnimatedCounter target={100000} suffix="+" />
            <div className="text-sm text-muted-foreground mt-1 font-medium">Queries/sec</div>
          </div>
          <div className="text-center p-4 border-x border-border rounded-xl">
            <AnimatedCounter target={5000} suffix="+" />
            <div className="text-sm text-muted-foreground mt-1 font-medium">Tests</div>
          </div>
          <div className="text-center p-4 rounded-xl hover:bg-card/50 transition-colors">
            <AnimatedCounter target={90} suffix="%+" />
            <div className="text-sm text-muted-foreground mt-1 font-medium">Coverage</div>
          </div>
        </div>
      </div>
    </section>
  )
}
