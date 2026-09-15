import { cleanup, render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { DocsPage } from './DocsPage'
import { ExamplesPage } from './ExamplesPage'
import { HomePage } from './HomePage'
import { NotFoundPage } from './NotFoundPage'
import { PlaygroundPage } from './PlaygroundPage'

vi.mock('@components/playground/SqlEditor', () => ({
  SqlEditor: () => <div data-testid="sql-editor" />,
}))
vi.mock('sql.js', () => ({ default: vi.fn(() => new Promise(() => undefined)) }))

afterEach(cleanup)

function renderAt(path: string, element: React.ReactNode) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="*" element={element} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('website pages', () => {
  it('renders the home page', () => {
    renderAt('/', <HomePage />)
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
  })

  it('renders documentation', () => {
    renderAt('/docs/getting-started', <DocsPage />)
    expect(screen.getByRole('main')).toBeInTheDocument()
  })

  it('renders examples', () => {
    renderAt('/examples', <ExamplesPage />)
    expect(screen.getByRole('heading', { name: /code examples/i })).toBeInTheDocument()
  })

  it('renders the playground shell', () => {
    renderAt('/playground', <PlaygroundPage />)
    expect(document.body.textContent?.length).toBeGreaterThan(0)
  })

  it('renders the not-found page', () => {
    renderAt('/missing', <NotFoundPage />)
    expect(screen.getByRole('heading', { name: /page not found/i })).toBeInTheDocument()
  })
})
