import { useRef, useEffect } from 'react'

interface SqlEditorProps {
  value: string
  onChange: (value: string) => void
  onKeyDown: (e: React.KeyboardEvent) => void
}

export function SqlEditor({ value, onChange, onKeyDown }: SqlEditorProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const lineNumbersRef = useRef<HTMLDivElement>(null)

  const lines = value.split('\n')
  const lineCount = lines.length

  const syncScroll = () => {
    if (textareaRef.current && lineNumbersRef.current) {
      lineNumbersRef.current.scrollTop = textareaRef.current.scrollTop
    }
  }

  useEffect(() => {
    const textarea = textareaRef.current
    if (textarea) {
      textarea.addEventListener('scroll', syncScroll)
      return () => textarea.removeEventListener('scroll', syncScroll)
    }
  }, [])

  return (
    <div className="relative flex bg-[#0c0e14]">
      {/* Line numbers */}
      <div
        ref={lineNumbersRef}
        className="py-4 pl-4 pr-2 text-right select-none overflow-hidden shrink-0 font-mono text-xs leading-[1.625rem] text-gray-600"
        aria-hidden="true"
      >
        {Array.from({ length: lineCount }, (_, i) => (
          <div key={i + 1}>{i + 1}</div>
        ))}
      </div>

      {/* Editor */}
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={onKeyDown}
        className="flex-1 h-56 py-4 pr-4 pl-2 font-mono text-sm leading-[1.625rem] bg-transparent text-gray-200 resize-none focus:outline-none border-none"
        placeholder="Enter your SQL query here..."
        spellCheck={false}
      />
    </div>
  )
}
