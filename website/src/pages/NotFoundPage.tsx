import { Link } from 'react-router-dom'
import { Button } from '@components/ui/button'
import { Database, ArrowLeft, Home } from 'lucide-react'
import { usePageTitle } from '@hooks/usePageTitle'

export function NotFoundPage() {
  usePageTitle('Page Not Found')
  return (
    <div className="container mx-auto px-4 py-24">
      <div className="max-w-lg mx-auto text-center">
        {/* Animated icon */}
        <div className="relative inline-block mb-8">
          <Database className="h-20 w-20 text-cobalt-600/20 animate-float" />
          <div className="absolute inset-0 bg-cobalt-400/10 blur-3xl rounded-full" />
        </div>

        {/* Error code */}
        <h1 className="text-7xl md:text-8xl font-black tracking-tighter text-gradient mb-4">
          404
        </h1>

        <h2 className="text-xl md:text-2xl font-bold mb-3 tracking-tight">
          Page Not Found
        </h2>

        <p className="text-muted-foreground mb-10 max-w-sm mx-auto leading-relaxed">
          The page you're looking for doesn't exist or has been moved.
          Let's get you back on track.
        </p>

        <div className="flex flex-col sm:flex-row items-center justify-center gap-3">
          <Link to="/">
            <Button size="lg" className="bg-cobalt-600 hover:bg-cobalt-700 text-white shadow-lg shadow-cobalt-600/25">
              <Home className="h-4 w-4 mr-2" />
              Back to Home
            </Button>
          </Link>
          <Link to="/docs">
            <Button size="lg" variant="outline" className="group">
              <ArrowLeft className="h-4 w-4 mr-2 transition-transform group-hover:-translate-x-1" />
              Documentation
            </Button>
          </Link>
        </div>
      </div>
    </div>
  )
}
