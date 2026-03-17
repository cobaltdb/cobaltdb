import { docsData } from '@data/docs'
import { Alert, AlertDescription, AlertTitle } from '@components/ui/alert'
import { Info, AlertTriangle, Lightbulb } from 'lucide-react'

interface DocsContentProps {
  section: string
}

export function DocsContent({ section }: DocsContentProps) {
  const doc = docsData[section]

  if (!doc) {
    return (
      <div className="prose dark:prose-invert max-w-none">
        <h1>Documentation Not Found</h1>
        <p>The requested documentation page could not be found.</p>
      </div>
    )
  }

  return (
    <article className="prose dark:prose-invert max-w-none">
      <h1>{doc.title}</h1>
      {doc.description && (
        <p className="text-lg text-muted-foreground lead">{doc.description}</p>
      )}

      {doc.content}
    </article>
  )
}

// Alert components for use in docs
export function DocsInfo({ title, children }: { title?: string; children: React.ReactNode }) {
  return (
    <Alert className="my-6">
      <Info className="h-4 w-4" />
      {title && <AlertTitle>{title}</AlertTitle>}
      <AlertDescription>{children}</AlertDescription>
    </Alert>
  )
}

export function DocsWarning({ title, children }: { title?: string; children: React.ReactNode }) {
  return (
    <Alert variant="destructive" className="my-6">
      <AlertTriangle className="h-4 w-4" />
      {title && <AlertTitle>{title}</AlertTitle>}
      <AlertDescription>{children}</AlertDescription>
    </Alert>
  )
}

export function DocsTip({ title, children }: { title?: string; children: React.ReactNode }) {
  return (
    <Alert className="my-6 border-cobalt-200 bg-cobalt-50 dark:bg-cobalt-900/20">
      <Lightbulb className="h-4 w-4 text-cobalt-600" />
      {title && <AlertTitle className="text-cobalt-800 dark:text-cobalt-200">{title}</AlertTitle>}
      <AlertDescription className="text-cobalt-700 dark:text-cobalt-300">{children}</AlertDescription>
    </Alert>
  )
}
