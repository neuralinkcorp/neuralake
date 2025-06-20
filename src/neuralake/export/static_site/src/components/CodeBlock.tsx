import { Button, Card } from '@radix-ui/themes'
import { type ReactNode } from 'react'
import { BundledLanguage } from 'shiki'
import SyntaxHighlightedCode from './SyntaxHighlightedCode'
import { CopyIcon } from '@radix-ui/react-icons'

export interface CodeBlockProps {
  code: string
  lang: BundledLanguage
}

export default function CodeBlock ({ code, lang }: CodeBlockProps): ReactNode {
  return (
    <Card style={{ position: 'relative' }}>
      <SyntaxHighlightedCode code={code} lang={lang} />

      <Button
        variant='soft'
        style={{ position: 'absolute', top: 'var(--space-2)', right: 'var(--space-2)' }}
        onClick={() => navigator.clipboard.writeText(code)}
      >
        <CopyIcon /> Copy
      </Button>
    </Card>
  )
}
