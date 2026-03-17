import { Link } from 'react-router-dom'
import { Button } from '@components/ui/button'
import { ArrowRight, Github, Sparkles, Copy, Check } from 'lucide-react'
import { useState } from 'react'

export function CTASection() {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText('go get github.com/cobaltdb/cobaltdb')
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <section className="py-24">
      <div className="container mx-auto px-4">
        <div className="max-w-4xl mx-auto relative">
          {/* Glow behind card */}
          <div className="absolute inset-0 bg-cobalt-500/20 blur-3xl rounded-3xl" />

          <div className="relative animated-gradient rounded-2xl p-12 md:p-16 text-white overflow-hidden">
            {/* Decorative elements */}
            <div className="absolute top-0 right-0 w-64 h-64 bg-white/5 rounded-full -translate-y-1/2 translate-x-1/3" />
            <div className="absolute bottom-0 left-0 w-48 h-48 bg-white/5 rounded-full translate-y-1/2 -translate-x-1/3" />
            <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-96 h-96 bg-white/3 rounded-full blur-3xl" />

            <div className="relative text-center">
              <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-white/10 text-white/90 text-xs font-medium mb-6 border border-white/10">
                <Sparkles className="h-3 w-3" />
                Open Source & Free Forever
              </div>

              <h2 className="text-3xl md:text-5xl font-bold mb-4 tracking-tight">
                Ready to Get Started?
              </h2>
              <p className="text-lg text-white/80 mb-10 max-w-2xl mx-auto leading-relaxed">
                Join developers building fast, reliable applications with CobaltDB.
                Get up and running in minutes.
              </p>

              <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
                <Link to="/docs/getting-started">
                  <Button size="lg" className="bg-white text-cobalt-700 hover:bg-white/90 shadow-lg text-base px-8 font-semibold group">
                    Get Started
                    <ArrowRight className="h-4 w-4 ml-2 transition-transform group-hover:translate-x-1" />
                  </Button>
                </Link>
                <a
                  href="https://github.com/cobaltdb/cobaltdb"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Button size="lg" variant="outline" className="text-base px-8 border-white/30 text-white hover:bg-white/10 hover:border-white/50 transition-all">
                    <Github className="h-4 w-4 mr-2" />
                    Star on GitHub
                  </Button>
                </a>
              </div>

              {/* Install command - now clickable to copy */}
              <button
                onClick={handleCopy}
                className="mt-10 inline-flex items-center gap-3 px-5 py-3 rounded-lg bg-black/20 border border-white/10 font-mono text-sm text-white/80 hover:bg-black/30 hover:border-white/20 transition-all group cursor-pointer"
              >
                <span className="text-cobalt-300">$</span>
                go get github.com/cobaltdb/cobaltdb
                {copied ? (
                  <Check className="h-3.5 w-3.5 text-emerald-400 ml-2" />
                ) : (
                  <Copy className="h-3.5 w-3.5 text-white/40 group-hover:text-white/70 transition-colors ml-2" />
                )}
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
