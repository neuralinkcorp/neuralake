import { useThemeContext } from '@radix-ui/themes'
import { useEffect, type ReactNode, useState } from 'react'
import { BundledLanguage, codeToHtml } from 'shiki'

export interface SyntaxHighlightedCodeProps {
  code: string
  lang: BundledLanguage
}

export default function SyntaxHighlightedCode ({ code, lang }: SyntaxHighlightedCodeProps): ReactNode {
  const [ html, setHtml ] = useState('')

  const theme = useThemeContext()
  const isDark = theme.appearance === 'dark'
  useEffect(() => {
    codeToHtml(code, {
      lang,
      theme: isDark ? 'github-dark' : 'github-light',
      transformers: [
        {
          pre (node) {
            this.addClassToHast(node, 'rt-Code rt-reset rt-reset rt-Code rt-variant-ghost rt-r-size-2')
            node.properties.style = String(node.properties.style ?? '') + '; background-color: transparent;'
          },
        },
      ],
    }).then((html) => {
      setHtml(html)
    }).catch((error) => {
      console.error('Syntax highlighting error:', error)
    })
  }, [ code, lang, isDark ])

  return <div dangerouslySetInnerHTML={{ __html: html }} />
}
