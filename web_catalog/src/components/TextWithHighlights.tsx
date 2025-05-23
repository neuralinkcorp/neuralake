import { RangeTuple } from 'fuse.js'
import { ReactNode } from 'react'

export interface TextWithHighlightsProps {
  text: string
  ranges: readonly RangeTuple[]
  notHighlightedClassName?: string
  highlightedClassName?: string
}

export default function TextWithHighlights ({ text, ranges, notHighlightedClassName, highlightedClassName }: TextWithHighlightsProps): ReactNode {
  const nodes: ReactNode[] = []

  let currentTextIndex = 0
  for (const [ start, end ] of ranges) {
    if (start > currentTextIndex) {
      nodes.push(
        <span key={`${currentTextIndex}-${start}-${text}`} className={notHighlightedClassName}>
          {text.slice(currentTextIndex, start)}
        </span>
      )
    }

    nodes.push(
      <span key={`${start}-${end}-${text}`} className={highlightedClassName}>
        {text.slice(start, end + 1)}
      </span>
    )

    currentTextIndex = end + 1
  }

  if (currentTextIndex < text.length) {
    nodes.push(
      <span key={`final-${currentTextIndex}-${text}`} className={notHighlightedClassName}>
        {text.slice(currentTextIndex)}
      </span>
    )
  }

  return (
    <span>{nodes}</span>
  )
}
