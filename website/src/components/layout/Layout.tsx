import { Outlet } from 'react-router-dom'
import { Header } from './Header'
import { Footer } from './Footer'
import { PageTransition } from '@components/PageTransition'

export function Layout({ children }: { children?: React.ReactNode }) {
  return (
    <div className="min-h-screen flex flex-col bg-background">
      <Header />
      <main className="flex-1">
        <PageTransition>
          {children || <Outlet />}
        </PageTransition>
      </main>
      <Footer />
    </div>
  )
}
