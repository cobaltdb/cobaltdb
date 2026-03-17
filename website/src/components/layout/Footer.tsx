import { Link } from 'react-router-dom'
import { Database, Github, Heart } from 'lucide-react'

const footerLinks = {
  product: [
    { label: 'Features', href: '/#features' },
    { label: 'Documentation', href: '/docs' },
    { label: 'Examples', href: '/examples' },
    { label: 'Playground', href: '/playground' },
  ],
  resources: [
    { label: 'Getting Started', href: '/docs/getting-started' },
    { label: 'API Reference', href: '/docs/api' },
    { label: 'SQL Guide', href: '/docs/sql' },
    { label: 'WASM Integration', href: '/docs/wasm' },
  ],
  community: [
    { label: 'GitHub', href: 'https://github.com/cobaltdb/cobaltdb', external: true },
    { label: 'Discussions', href: 'https://github.com/cobaltdb/cobaltdb/discussions', external: true },
    { label: 'Issues', href: 'https://github.com/cobaltdb/cobaltdb/issues', external: true },
    { label: 'Releases', href: 'https://github.com/cobaltdb/cobaltdb/releases', external: true },
  ],
}

export function Footer() {
  return (
    <footer className="border-t bg-muted/20 relative">
      {/* Top gradient line */}
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-cobalt-500/20 to-transparent" />

      <div className="container mx-auto px-4 py-14">
        <div className="grid grid-cols-2 md:grid-cols-5 gap-10">
          {/* Logo & Description */}
          <div className="col-span-2">
            <Link to="/" className="flex items-center gap-2 mb-4 group">
              <div className="relative">
                <Database className="h-6 w-6 text-cobalt-600 transition-transform group-hover:scale-110" />
                <div className="absolute inset-0 bg-cobalt-400/20 blur-lg rounded-full opacity-0 group-hover:opacity-100 transition-opacity" />
              </div>
              <span className="font-bold text-lg">
                <span className="text-cobalt-600">Cobalt</span>
                <span className="text-foreground">DB</span>
              </span>
            </Link>
            <p className="text-sm text-muted-foreground mb-5 max-w-xs leading-relaxed">
              A high-performance, embeddable SQL database written in pure Go.
              Zero dependencies, maximum reliability.
            </p>
            <div className="flex items-center gap-2">
              <a
                href="https://github.com/cobaltdb/cobaltdb"
                target="_blank"
                rel="noopener noreferrer"
                className="p-2 rounded-lg bg-background border hover:border-cobalt-300 dark:hover:border-cobalt-700 transition-all hover:shadow-sm group"
              >
                <Github className="h-4 w-4 group-hover:text-cobalt-600 dark:group-hover:text-cobalt-400 transition-colors" />
              </a>
            </div>
          </div>

          {/* Product Links */}
          <div>
            <h4 className="text-sm font-semibold mb-4">Product</h4>
            <ul className="space-y-2.5">
              {footerLinks.product.map((link) => (
                <li key={link.label}>
                  <Link
                    to={link.href}
                    className="text-sm text-muted-foreground hover:text-cobalt-600 dark:hover:text-cobalt-400 transition-colors"
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Resources Links */}
          <div>
            <h4 className="text-sm font-semibold mb-4">Resources</h4>
            <ul className="space-y-2.5">
              {footerLinks.resources.map((link) => (
                <li key={link.label}>
                  <Link
                    to={link.href}
                    className="text-sm text-muted-foreground hover:text-cobalt-600 dark:hover:text-cobalt-400 transition-colors"
                  >
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Community Links */}
          <div>
            <h4 className="text-sm font-semibold mb-4">Community</h4>
            <ul className="space-y-2.5">
              {footerLinks.community.map((link) => (
                <li key={link.label}>
                  <a
                    href={link.href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm text-muted-foreground hover:text-cobalt-600 dark:hover:text-cobalt-400 transition-colors"
                  >
                    {link.label}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        </div>

        {/* Bottom Bar */}
        <div className="border-t mt-12 pt-6 flex flex-col sm:flex-row items-center justify-between gap-4">
          <p className="text-xs text-muted-foreground">
            &copy; {new Date().getFullYear()} CobaltDB. Open source under MIT License.
          </p>
          <p className="text-xs text-muted-foreground flex items-center gap-1">
            Built with <Heart className="h-3 w-3 text-red-400 inline" /> using Go, React & TypeScript
          </p>
        </div>
      </div>
    </footer>
  )
}
