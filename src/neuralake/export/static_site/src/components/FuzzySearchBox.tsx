import classes from './FuzzySearchBox.module.css'

import { ChevronRightIcon, ColumnSpacingIcon, CubeIcon, MagnifyingGlassIcon, StackIcon, TableIcon } from '@radix-ui/react-icons'
import { Popover, Box, TextField, Flex, Text } from '@radix-ui/themes'
import { flushSync } from 'react-dom'
import classNames from 'classnames'
import { Fragment, ReactNode, useEffect, useRef, useState } from 'react'
import { FuseResult } from 'fuse.js'
import { useNavigate } from 'react-router-dom'
import { useHotkeys } from 'react-hotkeys-hook'

import { IndexItem, indexByCatalog, searchSegmentedIndex } from '../lib/fuzzy-search'
import TextWithHighlights from './TextWithHighlights'
import { useThrottle } from '../lib/useThrottle'

const WIDTH = '600px'

interface ResultProps {
  result: FuseResult<IndexItem>
}

function InnerResult ({ result }: ResultProps): ReactNode {
  const { item, matches } = result

  const Icon = {
    database: CubeIcon,
    table: TableIcon,
    column: StackIcon,
    partition: ColumnSpacingIcon,
  }[item.kind]

  const breadcrumbs: ReactNode[] = [
    <TextWithHighlights
      key='database'
      text={item.databaseName}
      ranges={matches?.find((match) => match.key === 'databaseName')?.indices ?? []}
      highlightedClassName={classes.highlightedRange}
    />
  ]
  if (item.kind === 'table' || item.kind === 'partition' || item.kind === 'column') {
    breadcrumbs.push(
      <TextWithHighlights
        key='table'
        text={item.tableName}
        ranges={matches?.find((match) => match.key === 'tableName')?.indices ?? []}
        highlightedClassName={classes.highlightedRange}
      />
    )
  }
  if (item.kind === 'column' || item.kind === 'partition') {
    breadcrumbs.push(
      <TextWithHighlights
        key='columnOrPartition'
        text={item.columnOrPartitionName}
        ranges={matches?.find((match) => match.key === 'columnOrPartitionName')?.indices ?? []}
        highlightedClassName={classes.highlightedRange}
      />
    )
  }

  return (
    <Flex gap='2' width='100%' align='center'>
      <Flex flexGrow='1' gap='4' align='center'>
        <Icon width='16' height='16' />

        <Flex gap='1' align='center'>
          {breadcrumbs.map((breadcrumb, i) => (
            <Fragment key={`${i}-${breadcrumb}`}>
              {breadcrumb}

              {i < breadcrumbs.length - 1 && (
                <ChevronRightIcon className='breadcrumb-chevron' />
              )}
            </Fragment>
          ))}
        </Flex>
      </Flex>

      {'typeInfo' in item && (
        <Text size='2' color='gray' align='right'>
          {item.typeInfo?.split('(')[0]}
        </Text>
      )}
    </Flex>
  )
}

export interface FuzzySearchBoxProps {
  catalogKey: string
  enableKeyboardShortcuts?: boolean
}

export function FuzzySearchBox ({ catalogKey, enableKeyboardShortcuts }: FuzzySearchBoxProps): ReactNode {
  const [ query, setQuery ] = useState('')

  const index = indexByCatalog.get(catalogKey)
  const [ results, setResults ] = useState<FuseResult<IndexItem>[]>([])
  const [ activeIndex, setActiveIndex ] = useState<number>(0)

  const textFieldRef = useRef<HTMLInputElement>(null)
  const popupRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const listener = (event: PointerEvent) => {
      const isInTextField = textFieldRef.current?.contains(event.target as Node)
      const isInPopup = popupRef.current?.contains(event.target as Node)
      if (!isInTextField && !isInPopup) {
        setQuery('')
        setResults([])
      }
    }

    window.addEventListener('pointerdown', listener)
    return () => window.removeEventListener('pointerdown', listener)
  })

  const throttledQuery = useThrottle(query, 100)
  useEffect(() => {
    const results = index ? searchSegmentedIndex(index, throttledQuery, 10) : []

    queueMicrotask(() => {
      flushSync(() => {
        setResults(results)
        setActiveIndex(0)
      })

      // Opening the popover blurs the input, so we have to refocus.
      if (results.length > 0) textFieldRef.current?.focus()
    })
  }, [ throttledQuery, catalogKey ])

  useHotkeys(
    [ '/', 'mod+k' ],
    () => { textFieldRef.current?.focus() },
    { enabled: enableKeyboardShortcuts ?? false, preventDefault: true }
  )

  const navigate = useNavigate()

  function onActivate (item: IndexItem) {
    setQuery('')
    setResults([])
    textFieldRef.current?.blur()
    navigate(item.resourceUrl)
  }

  return (
    <Popover.Root open={results.length > 0}>
      <Popover.Trigger>
        <Box width={WIDTH}>
          <TextField.Root
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={`Search ${catalogKey}…`}
            ref={textFieldRef}
            variant='soft'
            color='gray'
            onKeyDown={(event) => {
              if (event.key === 'Escape') {
                event.preventDefault()
                setQuery('')
                setResults([])
              } else if (event.key === 'Enter') {
                event.preventDefault()
                const activeItem = results[activeIndex]
                if (activeItem) onActivate(activeItem.item)
              } else if (event.key === 'ArrowUp') {
                event.preventDefault()

                if (results.length > 0) {
                  if (activeIndex === 0) {
                    setActiveIndex(results.length - 1)
                  } else {
                    setActiveIndex(activeIndex - 1)
                  }
                }
              } else if (event.key === 'ArrowDown') {
                event.preventDefault()
                setActiveIndex(((activeIndex + 1) % results.length) || 0)
              }
            }}
          >
            <TextField.Slot>
              <MagnifyingGlassIcon height='18' width='18' />
            </TextField.Slot>
          </TextField.Root>
        </Box>
      </Popover.Trigger>

      <Box mt='-1' asChild>
        <Popover.Content ref={popupRef} width={WIDTH} size='1'>
          <Flex direction='column' gap='1'>
            {results.map((result, index) => (
              <button
                key={`${result.refIndex}-${result.item.kind}`}
                data-accent-color=''
                className={classNames('rt-reset', classes.item, (index === activeIndex) && classes.active)}
                onPointerDown={(event) => {
                  event.preventDefault()
                  setActiveIndex(index)
                }}
                onClick={() => onActivate(result.item)}
                tabIndex={-1}
              >
                <InnerResult result={result} />
              </button>
            ))}
          </Flex>
        </Popover.Content>
      </Box>
    </Popover.Root>
  )
}
