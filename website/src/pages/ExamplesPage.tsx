import { useState } from 'react'
import { usePageTitle } from '@hooks/usePageTitle'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@components/ui/tabs'
import { Badge } from '@components/ui/badge'
import { Check, Copy } from 'lucide-react'
import { Button } from '@components/ui/button'
import { examplesData } from '@data/examples'
import { highlightCode, detectLang } from '@lib/syntax'

export function ExamplesPage() {
  usePageTitle('Examples')
  const [copiedId, setCopiedId] = useState<string | null>(null)

  const copyToClipboard = async (code: string, id: string) => {
    await navigator.clipboard.writeText(code)
    setCopiedId(id)
    setTimeout(() => setCopiedId(null), 2000)
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <span className="inline-block text-xs font-semibold tracking-widest uppercase text-cobalt-600 dark:text-cobalt-400 mb-3">Learn by Example</span>
          <h1 className="text-4xl font-bold mb-4 tracking-tight">
            Code <span className="text-gradient">Examples</span>
          </h1>
          <p className="text-lg text-muted-foreground">
            Practical examples and patterns for building with CobaltDB.
          </p>
        </div>

        <Tabs defaultValue="basic" className="w-full">
          <TabsList className="grid w-full grid-cols-4 mb-8">
            <TabsTrigger value="basic">Basic</TabsTrigger>
            <TabsTrigger value="advanced">Advanced</TabsTrigger>
            <TabsTrigger value="realworld">Real World</TabsTrigger>
            <TabsTrigger value="patterns">Patterns</TabsTrigger>
          </TabsList>

          {Object.entries(examplesData).map(([category, examples]) => (
            <TabsContent key={category} value={category} className="space-y-6">
              {examples.map((example) => (
                <div key={example.id} className="rounded-xl border bg-card overflow-hidden hover:border-cobalt-300/50 dark:hover:border-cobalt-700/50 transition-colors">
                  {/* Header */}
                  <div className="px-6 py-4 border-b bg-muted/20">
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <h3 className="text-lg font-semibold mb-1">{example.title}</h3>
                        <p className="text-sm text-muted-foreground">{example.description}</p>
                      </div>
                      <div className="flex gap-1.5 shrink-0">
                        {example.tags.map((tag) => (
                          <Badge key={tag} variant="secondary" className="text-[10px] px-2 py-0.5">
                            {tag}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  </div>
                  {/* Code with syntax highlighting */}
                  <div className="relative group">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="absolute top-3 right-3 z-10 opacity-0 group-hover:opacity-100 transition-opacity bg-gray-800/50 hover:bg-gray-800/80 text-gray-300"
                      onClick={() => copyToClipboard(example.code, example.id)}
                    >
                      {copiedId === example.id ? (
                        <><Check className="h-3.5 w-3.5 text-green-400 mr-1" /> Copied</>
                      ) : (
                        <><Copy className="h-3.5 w-3.5 mr-1" /> Copy</>
                      )}
                    </Button>
                    <div className="bg-[#0c0e14] p-4 overflow-x-auto font-mono text-[13px] leading-6">
                      <div className="table w-full">
                        {highlightCode(example.code, detectLang(example.code))}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </TabsContent>
          ))}
        </Tabs>
      </div>
    </div>
  )
}
