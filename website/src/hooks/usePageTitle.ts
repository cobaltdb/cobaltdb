import { useEffect } from 'react'

const BASE_TITLE = 'CobaltDB'

export function usePageTitle(title?: string) {
  useEffect(() => {
    document.title = title ? `${title} - ${BASE_TITLE}` : `${BASE_TITLE} - Modern Embeddable SQL Database`
    return () => {
      document.title = `${BASE_TITLE} - Modern Embeddable SQL Database`
    }
  }, [title])
}
