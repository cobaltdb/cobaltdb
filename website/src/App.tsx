import { Routes, Route } from 'react-router-dom'
import { Layout } from '@components/layout/Layout'
import { ScrollToTop } from '@components/ScrollToTop'
import { ScrollToTopButton } from '@components/ScrollToTopButton'
import { HomePage } from '@pages/HomePage'
import { DocsPage } from '@pages/DocsPage'
import { ExamplesPage } from '@pages/ExamplesPage'
import { PlaygroundPage } from '@pages/PlaygroundPage'
import { NotFoundPage } from '@pages/NotFoundPage'

function App() {
  return (
    <>
      <ScrollToTop />
      <Layout>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/docs" element={<DocsPage />} />
          <Route path="/docs/:section" element={<DocsPage />} />
          <Route path="/examples" element={<ExamplesPage />} />
          <Route path="/playground" element={<PlaygroundPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </Layout>
      <ScrollToTopButton />
    </>
  )
}

export default App
