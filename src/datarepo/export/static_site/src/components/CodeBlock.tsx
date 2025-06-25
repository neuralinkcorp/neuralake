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
    <Card 
      style={{ 
        position: 'relative',
        overflow: 'auto'
      }}
    >
      <SyntaxHighlightedCode code={code} lang={lang} />

      <Button
        variant='soft'
        size={{ initial: '1', sm: '2' }}
        style={{ 
          position: 'absolute', 
          top: 'var(--space-2)', 
          right: 'var(--space-2)',
          minWidth: '44px',
          minHeight: '44px'
        }}
        onClick={() => navigator.clipboard.writeText(code)}
      >
        <CopyIcon />
        <span className='desktop-only' style={{ marginLeft: 'var(--space-1)' }}>
          Copy
        </span>
      </Button>
    </Card>
  )
}
