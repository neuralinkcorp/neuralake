import { Box, Button, Flex, Heading, IconButton, RadioCards, ScrollArea, Text } from '@radix-ui/themes'
import { ChevronDownIcon, ChevronRightIcon, Cross2Icon, HamburgerMenuIcon } from '@radix-ui/react-icons'
import { ReactNode, useState, useEffect } from 'react'
import { generatePath, useNavigate, useParams } from 'react-router-dom'
import { useRouteLoaderData } from 'react-router-dom'

import { ExportedCatalog } from '../lib/types'

export interface MobileMenuProps {
  isOpen: boolean
  onClose: () => void
}

export default function MobileMenu({ isOpen, onClose }: MobileMenuProps): ReactNode {
  const { catalogKey, databaseKey, tableKey } = useParams()
  const navigate = useNavigate()
  const catalog = useRouteLoaderData('catalogKey') as ExportedCatalog | null
  
  const [expandedDatabase, setExpandedDatabase] = useState<string | null>(databaseKey || null)

  // Prevent body scroll when menu is open
  useEffect(() => {
    if (isOpen) {
      document.body.classList.add('mobile-menu-open')
    } else {
      document.body.classList.remove('mobile-menu-open')
    }
    
    return () => {
      document.body.classList.remove('mobile-menu-open')
    }
  }, [isOpen])

  if (!isOpen) return null

  const handleDatabaseClick = (selectedDatabaseKey: string) => {
    const isCurrentDatabase = databaseKey === selectedDatabaseKey
    
    if (isCurrentDatabase) {
      // If database is already selected, only toggle expansion
      if (selectedDatabaseKey === expandedDatabase) {
        setExpandedDatabase(null)
      } else {
        setExpandedDatabase(selectedDatabaseKey)
      }
    } else {
      // Navigate to new database and expand it
      navigate(generatePath('/:catalogKey/:databaseKey', {
        catalogKey: catalogKey!,
        databaseKey: selectedDatabaseKey
      }))
      setExpandedDatabase(selectedDatabaseKey)
    }
  }

  const handleTableSelect = (selectedTableKey: string) => {
    navigate(generatePath('/:catalogKey/:databaseKey/:tableKey', {
      catalogKey: catalogKey!,
      databaseKey: databaseKey!,
      tableKey: encodeURIComponent(selectedTableKey)
    }))
    onClose()
  }

  const handleOverlayClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose()
    }
  }

  return (
    <Box
      position="fixed"
      top="0"
      left="0"
      right="0"
      bottom="0"
      className="mobile-menu-container mobile-menu-overlay"
      onClick={handleOverlayClick}
      style={{
        zIndex: 1000,
      }}
    >
      <Box
        position="fixed"
        top="0"
        left="0"
        bottom="0"
        width="280px"
        className="mobile-menu-panel"
        style={{
          backgroundColor: 'var(--color-panel-solid)',
          borderRight: '1px solid var(--gray-a6)',
          transform: isOpen ? 'translateX(0)' : 'translateX(-100%)',
          transition: 'transform 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        }}
      >
        <ScrollArea>
          <Flex direction="column" height="100%">
            {/* Header */}
            <Flex align="center" justify="between" p="3" style={{ borderBottom: '1px solid var(--gray-a6)' }}>
              <Heading size="4">{catalog?.name || 'Navigation'}</Heading>
              <IconButton 
                variant="ghost" 
                onClick={onClose}
                style={{ minWidth: '44px', minHeight: '44px' }}
              >
                <Cross2Icon />
              </IconButton>
            </Flex>

            {/* Navigation Content */}
            <Flex direction="column" flexGrow="1" p="3" gap="3">
              {catalog && (
                <>
                  {/* Database List */}
                  <Flex direction="column" gap="1">
                    {catalog.databases.map((db) => {
                      const isExpanded = expandedDatabase === db.name
                      const isSelected = databaseKey === db.name
                      
                      return (
                        <Box key={db.name}>
                          {/* Database Item - styled as expandable button */}
                          <Button
                            variant={isSelected ? "soft" : "ghost"}
                            onClick={() => handleDatabaseClick(db.name)}
                            style={{
                              width: '100%',
                              justifyContent: 'space-between',
                              minHeight: '44px',
                              padding: 'var(--space-2) var(--space-3)'
                            }}
                          >
                            <Text>{db.name}</Text>
                            {db.tables.length > 0 && (
                              isExpanded ? <ChevronDownIcon /> : <ChevronRightIcon />
                            )}
                          </Button>

                          {/* Tables List (nested) */}
                          {isExpanded && db.tables.length > 0 && (
                            <Box 
                              ml="3" 
                              mt="1" 
                              mb="2"
                              style={{
                                borderLeft: '2px solid var(--gray-a6)',
                                paddingLeft: 'var(--space-2)'
                              }}
                            >
                              <Flex direction="column" gap="1" mt="2">
                                <RadioCards.Root
                                  gap="1"
                                  value={tableKey}
                                  onValueChange={handleTableSelect}
                                >
                                  {db.tables.map((table) => (
                                    <RadioCards.Item key={table.name} value={table.name}>
                                      <Box width="100%">
                                        <Text size="2">{table.name}</Text>
                                      </Box>
                                    </RadioCards.Item>
                                  ))}
                                </RadioCards.Root>
                              </Flex>
                            </Box>
                          )}
                        </Box>
                      )
                    })}
                  </Flex>
                </>
              )}
            </Flex>
          </Flex>
        </ScrollArea>
      </Box>
    </Box>
  )
}

export function MobileMenuButton({ onClick }: { onClick: () => void }): ReactNode {
  return (
    <IconButton
      className="mobile-only"
      variant="soft"
      size="2"
      onClick={onClick}
      style={{ 
        minWidth: '36px', 
        minHeight: '36px',
        padding: 'var(--space-2)'
      }}
    >
      <HamburgerMenuIcon />
    </IconButton>
  )
} 