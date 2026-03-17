import { Link, useLocation } from 'react-router-dom'
import { Database, Menu, Github, BookOpen, Play, Code2 } from 'lucide-react'
import { useState, useEffect } from 'react'
import { Button } from '@components/ui/button'
import { Sheet, SheetContent, SheetTrigger } from '@components/ui/sheet'
import { ThemeToggle } from '@components/theme-toggle'

const navItems = [
  { path: '/', label: 'Home', icon: null },
  { path: '/docs', label: 'Docs', icon: BookOpen },
  { path: '/examples', label: 'Examples', icon: Code2 },
  { path: '/playground', label: 'Playground', icon: Play },
]

export function Header() {
  const location = useLocation()
  const [isOpen, setIsOpen] = useState(false)
  const [scrolled, setScrolled] = useState(false)

  useEffect(() => {
    const handleScroll = () => setScrolled(window.scrollY > 10)
    window.addEventListener('scroll', handleScroll, { passive: true })
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  return (
    <header className={`sticky top-0 z-50 w-full transition-all duration-300 ${
      scrolled
        ? 'bg-background/80 backdrop-blur-xl border-b border-border shadow-sm'
        : 'bg-transparent backdrop-blur-sm border-b border-transparent'
    }`}>
      <div className="container mx-auto px-4 h-16 flex items-center justify-between">
        {/* Logo */}
        <Link to="/" className="flex items-center gap-2.5 group">
          <div className="relative">
            <Database className="h-7 w-7 text-cobalt-600 transition-transform duration-300 group-hover:scale-110" />
            <div className="absolute inset-0 bg-cobalt-400/20 blur-xl rounded-full opacity-0 group-hover:opacity-100 transition-opacity" />
          </div>
          <span className="font-bold text-xl tracking-tight">
            <span className="text-cobalt-600">Cobalt</span>
            <span className="text-foreground">DB</span>
          </span>
        </Link>

        {/* Desktop Navigation */}
        <nav className="hidden md:flex items-center gap-1">
          {navItems.map((item) => {
            const isActive = item.path === '/'
              ? location.pathname === '/'
              : location.pathname.startsWith(item.path)
            return (
              <Link key={item.path} to={item.path}>
                <Button
                  variant={isActive ? 'default' : 'ghost'}
                  size="sm"
                  className={`gap-1.5 ${isActive ? 'bg-cobalt-600 text-white hover:bg-cobalt-700' : ''}`}
                >
                  {item.icon && <item.icon className="h-3.5 w-3.5" />}
                  {item.label}
                </Button>
              </Link>
            )
          })}
        </nav>

        {/* Right Side Actions */}
        <div className="flex items-center gap-1.5">
          <a
            href="https://github.com/cobaltdb/cobaltdb"
            target="_blank"
            rel="noopener noreferrer"
            className="hidden sm:flex"
          >
            <Button variant="ghost" size="icon" className="hover:text-foreground">
              <Github className="h-[18px] w-[18px]" />
            </Button>
          </a>

          <ThemeToggle />

          <a
            href="https://github.com/cobaltdb/cobaltdb/releases"
            target="_blank"
            rel="noopener noreferrer"
            className="hidden sm:block ml-1"
          >
            <Button size="sm" className="bg-cobalt-600 hover:bg-cobalt-700 text-white shadow-sm">
              Download
            </Button>
          </a>

          {/* Mobile Menu */}
          <div className="md:hidden">
            <Sheet open={isOpen} onOpenChange={setIsOpen}>
              <SheetTrigger render={<Button variant="ghost" size="icon">
                <Menu className="h-5 w-5" />
              </Button>} />
              <SheetContent side="right" className="w-72">
                <div className="flex flex-col gap-2 mt-8">
                  <div className="flex items-center gap-2 mb-4 px-2">
                    <Database className="h-6 w-6 text-cobalt-600" />
                    <span className="font-bold text-lg">
                      <span className="text-cobalt-600">Cobalt</span>
                      <span className="text-foreground">DB</span>
                    </span>
                  </div>
                  {navItems.map((item) => {
                    const isActive = item.path === '/'
                      ? location.pathname === '/'
                      : location.pathname.startsWith(item.path)
                    return (
                      <Link
                        key={item.path}
                        to={item.path}
                        onClick={() => setIsOpen(false)}
                      >
                        <Button
                          variant={isActive ? 'default' : 'ghost'}
                          className={`w-full justify-start gap-2 ${isActive ? 'bg-cobalt-600 text-white' : ''}`}
                        >
                          {item.icon && <item.icon className="h-4 w-4" />}
                          {item.label}
                        </Button>
                      </Link>
                    )
                  })}
                  <hr className="my-2" />
                  <a
                    href="https://github.com/cobaltdb/cobaltdb"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <Button variant="ghost" className="w-full justify-start gap-2">
                      <Github className="h-4 w-4" />
                      GitHub
                    </Button>
                  </a>
                  <a
                    href="https://github.com/cobaltdb/cobaltdb/releases"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <Button className="w-full bg-cobalt-600 hover:bg-cobalt-700 text-white mt-2">
                      Download
                    </Button>
                  </a>
                </div>
              </SheetContent>
            </Sheet>
          </div>
        </div>
      </div>
    </header>
  )
}
