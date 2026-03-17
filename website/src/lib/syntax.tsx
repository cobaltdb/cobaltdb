import React from 'react'

interface Token { text: string; className: string }

export function tokenizeLine(line: string, lang: string): Token[] {
  const tokens: Token[] = []
  let remaining = line

  while (remaining.length > 0) {
    let matched = false

    // Comments
    if (remaining.startsWith('//') || remaining.startsWith('--')) {
      tokens.push({ text: remaining, className: 'token-comment' })
      return tokens
    }

    // Strings (double or single or backtick)
    const strMatch = remaining.match(/^("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|`(?:[^`\\]|\\.)*`)/)
    if (strMatch) {
      tokens.push({ text: strMatch[0], className: 'token-string' })
      remaining = remaining.slice(strMatch[0].length)
      matched = true
      continue
    }

    // Numbers
    const numMatch = remaining.match(/^\b(\d+\.?\d*)\b/)
    if (numMatch) {
      tokens.push({ text: numMatch[0], className: 'token-number' })
      remaining = remaining.slice(numMatch[0].length)
      matched = true
      continue
    }

    // Keywords by language
    const keywords = lang === 'sql'
      ? /^(SELECT|FROM|WHERE|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|INDEX|VIEW|TRIGGER|DROP|ALTER|JOIN|INNER|LEFT|RIGHT|CROSS|ON|AND|OR|NOT|NULL|AS|ORDER|BY|GROUP|HAVING|LIMIT|OFFSET|DISTINCT|UNION|ALL|EXISTS|IN|BETWEEN|LIKE|IS|CASE|WHEN|THEN|ELSE|END|BEGIN|COMMIT|ROLLBACK|FOREIGN|KEY|REFERENCES|PRIMARY|UNIQUE|CHECK|DEFAULT|CONSTRAINT|WITH|RECURSIVE|VIRTUAL|USING|MATCH|OVER|PARTITION|ROWS|PRECEDING|CURRENT|ROW|FOLLOWING|INTEGER|TEXT|REAL|BOOLEAN|TIMESTAMP|DECIMAL|JSON|BLOB|VARCHAR|FLOAT|DOUBLE|AUTOINCREMENT|NOT|NULL|TRUE|FALSE|DESC|ASC|IF|FUNCTION|RETURN|FOR|EACH|AFTER|BEFORE|NEW|OLD|COALESCE|COUNT|SUM|AVG|MIN|MAX|RANK|DATE|CAST|ROUND)\b/i
      : lang === 'go'
      ? /^(package|import|func|var|const|type|struct|interface|map|chan|go|defer|return|if|else|for|range|switch|case|default|break|continue|nil|true|false|err|string|int|int64|float64|bool|byte|error|any|context)\b/
      : /^(import|from|export|default|const|let|var|function|async|await|return|if|else|for|while|class|new|try|catch|throw|typeof|instanceof|true|false|null|undefined|this|yield|of|in)\b/

    const kwMatch = remaining.match(keywords)
    if (kwMatch) {
      tokens.push({ text: kwMatch[0], className: 'token-keyword' })
      remaining = remaining.slice(kwMatch[0].length)
      matched = true
      continue
    }

    // Function calls
    const funcMatch = remaining.match(/^([a-zA-Z_]\w*)\s*(?=\()/)
    if (funcMatch) {
      tokens.push({ text: funcMatch[1], className: 'token-function' })
      remaining = remaining.slice(funcMatch[1].length)
      matched = true
      continue
    }

    // Types (Go)
    if (lang === 'go') {
      const typeMatch = remaining.match(/^(Database|Rows|Result|Tx|Options|Context)\b/)
      if (typeMatch) {
        tokens.push({ text: typeMatch[0], className: 'token-type' })
        remaining = remaining.slice(typeMatch[0].length)
        matched = true
        continue
      }
    }

    // Operators and punctuation
    const opMatch = remaining.match(/^([{}()[\];:.,=<>!+\-*/%&|^~?@#]+)/)
    if (opMatch) {
      tokens.push({ text: opMatch[0], className: 'token-operator' })
      remaining = remaining.slice(opMatch[0].length)
      matched = true
      continue
    }

    if (!matched) {
      const nextSpecial = remaining.slice(1).search(/[/"'`\d{}()[\];:.,=<>!+\-*/%&|^~?@#\s]/)
      const end = nextSpecial === -1 ? remaining.length : nextSpecial + 1
      tokens.push({ text: remaining.slice(0, end), className: 'token-default' })
      remaining = remaining.slice(end)
    }
  }

  return tokens
}

export function highlightCode(code: string, lang: string): React.ReactNode[] {
  const lines = code.split('\n')
  return lines.map((line, lineIdx) => {
    const tokens = tokenizeLine(line, lang)
    return (
      <div key={lineIdx} className="table-row">
        <span className="table-cell text-right pr-4 select-none text-gray-600 text-xs w-8">{lineIdx + 1}</span>
        <span className="table-cell">
          {tokens.map((token, i) => (
            <span key={i} className={token.className}>{token.text}</span>
          ))}
        </span>
      </div>
    )
  })
}

export function detectLang(code: string): string {
  if (code.includes('package ') || code.includes('func ') || code.includes(':= ')) return 'go'
  if (/\b(SELECT|INSERT|CREATE TABLE|ALTER|DROP)\b/i.test(code)) return 'sql'
  return 'js'
}
