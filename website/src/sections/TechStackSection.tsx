import {
  Binary,
  HardDrive,
  FileText,
  Layers,
  BarChart3,
  Search,
  type LucideIcon,
} from 'lucide-react'

interface TechItem {
  icon: LucideIcon
  name: string
  description: string
}

const techItems: TechItem[] = [
  {
    icon: Binary,
    name: 'B-Tree Engine',
    description: 'Disk-backed B+ tree storage with efficient range scans',
  },
  {
    icon: HardDrive,
    name: 'Buffer Pool',
    description: 'LRU page cache with pin counting and dirty tracking',
  },
  {
    icon: FileText,
    name: 'WAL',
    description: 'Write-ahead logging for crash recovery and durability',
  },
  {
    icon: Layers,
    name: 'MVCC',
    description: 'Multi-version concurrency with serializable isolation',
  },
  {
    icon: BarChart3,
    name: 'Query Optimizer',
    description: 'Cost-based planning with index selection and join ordering',
  },
  {
    icon: Search,
    name: 'FTS4 Engine',
    description: 'Full-text search with porter stemming and ranking',
  },
]

export function TechStackSection() {
  return (
    <section className="py-20 border-t border-b border-border/40 relative overflow-hidden">
      <div className="container mx-auto px-4 relative">
        <p className="text-center text-xs font-semibold tracking-widest uppercase text-muted-foreground mb-12">
          Built on Proven Database Internals
        </p>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 max-w-5xl mx-auto">
          {techItems.map((item) => (
            <div
              key={item.name}
              className="text-center group p-4 rounded-xl hover:bg-muted/50 transition-all duration-300 cursor-default"
            >
              <div className="mx-auto w-10 h-10 rounded-lg bg-cobalt-50 dark:bg-cobalt-500/10 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform duration-300">
                <item.icon className="h-5 w-5 text-cobalt-600 dark:text-cobalt-400" />
              </div>
              <div className="text-sm font-semibold text-foreground group-hover:text-cobalt-600 dark:group-hover:text-cobalt-400 transition-colors mb-1">
                {item.name}
              </div>
              <div className="text-[11px] text-muted-foreground leading-snug">
                {item.description}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
