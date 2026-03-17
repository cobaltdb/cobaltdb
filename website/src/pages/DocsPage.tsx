import { usePageTitle } from '@hooks/usePageTitle'
import { useParams } from 'react-router-dom'
import { DocsSidebar } from '@components/docs/DocsSidebar'
import { DocsContent } from '@components/docs/DocsContent'
import { docsData } from '@data/docs'
import { useState, useEffect } from 'react'
import { Button } from '@components/ui/button'
import { Menu, X } from 'lucide-react'

export function DocsPage() {
  const { section } = useParams()
  const currentSection = section || 'getting-started'
  usePageTitle(`Docs - ${currentSection.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`)
  const [sidebarOpen, setSidebarOpen] = useState(false)

  // Close mobile sidebar on section change
  useEffect(() => {
    setSidebarOpen(false)
  }, [currentSection])

  return (
    <div className="container mx-auto px-4 py-8">
      {/* Mobile sidebar toggle */}
      <div className="lg:hidden mb-4">
        <Button
          variant="outline"
          size="sm"
          onClick={() => setSidebarOpen(!sidebarOpen)}
          className="gap-2"
        >
          {sidebarOpen ? <X className="h-4 w-4" /> : <Menu className="h-4 w-4" />}
          {sidebarOpen ? 'Close Menu' : 'Navigation'}
        </Button>
      </div>

      <div className="flex gap-8">
        {/* Sidebar - mobile overlay */}
        {sidebarOpen && (
          <div
            className="fixed inset-0 bg-background/80 backdrop-blur-sm z-40 lg:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        {/* Sidebar */}
        <aside className={`
          ${sidebarOpen
            ? 'fixed left-0 top-16 bottom-0 z-50 w-72 bg-background border-r p-6 overflow-y-auto shadow-xl animate-slide-in-left'
            : 'hidden'
          } lg:block lg:static lg:w-64 lg:shrink-0 lg:p-0 lg:border-0 lg:shadow-none lg:bg-transparent lg:animate-none
        `}>
          <DocsSidebar currentSection={currentSection} />
        </aside>

        {/* Main Content */}
        <main className="flex-1 min-w-0">
          <DocsContent section={currentSection} />
        </main>

        {/* On this page - Table of contents */}
        <aside className="hidden xl:block w-48 shrink-0">
          <div className="sticky top-24">
            <h4 className="text-xs font-semibold mb-3 text-muted-foreground uppercase tracking-wider">
              On this page
            </h4>
            <nav className="space-y-1 border-l border-border pl-3">
              {docsData[currentSection]?.headings?.map((heading) => (
                <a
                  key={heading.id}
                  href={`#${heading.id}`}
                  className="block text-xs text-muted-foreground hover:text-cobalt-600 dark:hover:text-cobalt-400 py-1 transition-colors"
                >
                  {heading.title}
                </a>
              ))}
            </nav>
          </div>
        </aside>
      </div>
    </div>
  )
}
