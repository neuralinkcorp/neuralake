import { Box, Flex, Heading, RadioCards, ScrollArea, Text } from '@radix-ui/themes'
import { ReactNode } from 'react'

export interface SidebarItem {
  label: string
  value: string
}

export interface SidebarProps {
  eyebrow: string
  heading: string
  items: SidebarItem[]
  value?: string
  onValueChange?: (value: string) => void
}

export default function Sidebar ({ eyebrow, heading, items, value, onValueChange }: SidebarProps): ReactNode {
  return (
    <Box width='250px' height='100%'>
      <ScrollArea>
        <Flex gap='3' p='2' pt='3' direction='column'>
          <Flex p='1' direction='column' gap='1'>
            <Text size='1' color='gray' weight='medium'>
              {eyebrow}
            </Text>

            <Heading as='h2' size='4'>
              {heading}
            </Heading>
          </Flex>

          <RadioCards.Root
            gap='1'
            value={value}
            onValueChange={onValueChange}
            // Work around a bug where when the items list changes the selected item wouldn't be reset to null.
            // TODO: Use our own custom navbar items instead.
            key={value === undefined ? 'value-missing' : 'value-present'}
          >
            {items.map((item) => (
              <RadioCards.Item key={item.value} value={item.value}>
                <Box width='100%'>
                  <Text>{item.label}</Text>
                </Box>
              </RadioCards.Item>
            ))}
          </RadioCards.Root>
        </Flex>
      </ScrollArea>
    </Box>
  )
}
