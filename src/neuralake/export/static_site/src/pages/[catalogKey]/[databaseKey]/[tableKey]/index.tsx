import { useRouteLoaderData } from 'react-router-dom'
import { Badge, Box, Callout, Code, Container, DataList, Flex, Heading, Link, ScrollArea, Tabs, Text, Tooltip } from '@radix-ui/themes'
import sanitizeHtml from 'sanitize-html';

import { ExportedCatalog, ExportedDatabase, ExportedTable } from '../../../../lib/types'
import { genTableCode } from '../../../../lib/codegen'
import CodeBlock from '../../../../components/CodeBlock'

import ArrowTypeView from '../../../../components/ArrowTypeView'
import { InfoCircledIcon } from '@radix-ui/react-icons';

export default function TableKeyPage () {
  const catalog = useRouteLoaderData('catalogKey') as ExportedCatalog
  const database = useRouteLoaderData('databaseKey') as ExportedDatabase
  const table = useRouteLoaderData('tableKey') as ExportedTable | undefined

  if (!table) {
    return (
      <Flex 
        justify='center' 
        flexGrow='1' 
        p='5'
        className='responsive-container'
      >
        <Text color='gray'>No table selected.</Text>
      </Flex>
    )
  }

  // sanitizeHTML uses very sane defaults (essentially all inline tags)
  // replace newlines with linebreaks manually
  const cleanDescriptionHTML = sanitizeHtml(table.description.replace(/\n/g, "<br>"), {
    allowedTags: sanitizeHtml.defaults.allowedTags.concat(['img'])
  });

  const cleanDataInput = table.data_input && sanitizeHtml(table.data_input);

  return (
    <Flex flexGrow='1'>
      <ScrollArea>
        <Container 
          size={{ initial: '1', sm: '2' }}
          className='responsive-container'
        >
          <Flex 
            p={{ initial: '3', sm: '4' }} 
            gap={{ initial: '6', sm: '8' }} 
            direction='column'
          >
            <Flex direction='column' gap='5'>
              <Flex 
                direction={{ initial: 'column', sm: 'row' }} 
                gap='2' 
                align={{ initial: 'start', sm: 'center' }}
              >
                <Heading as='h2' size={{ initial: '6', sm: '7' }}>
                  {table.name}
                </Heading>
                <Badge>
                  {table.table_type === 'DELTA_LAKE'
                    ? 'Delta Lake'
                    : table.table_type === 'PARQUET'
                    ? 'Parquet'
                    : 'Function'}
                </Badge>
              </Flex>

              {cleanDescriptionHTML && <Text
                as='p'
                color='gray'
                align='left'
                size={{ initial: '2', sm: '3' }}
                dangerouslySetInnerHTML={{ __html: cleanDescriptionHTML }} />}

              {
                cleanDataInput && <>
                  <Heading as='h3' size={{ initial: '3', sm: '4' }} color='gray'>
                    Data Source
                  </Heading>
                  <Text 
                    color='gray' 
                    size={{ initial: '2', sm: '3' }}
                    dangerouslySetInnerHTML={{ __html: cleanDataInput }}
                  />
                </>
              }

              {
                table.latency_info && <>
                  <Heading as='h3' size={{ initial: '3', sm: '4' }} color='gray'>
                    Latency
                  </Heading>
                  <Text 
                    color='gray'
                    size={{ initial: '2', sm: '3' }}
                  >
                    {table.latency_info}
                  </Text>
                </>
              }

              {
                table.example_notebook && <Link 
                  href={table.example_notebook} 
                  target='_blank'
                  size={{ initial: '2', sm: '3' }}
                >
                  Example Notebook
                </Link>
              }

            <Tabs.Root defaultValue="filter-list" key={table.name}>
              <Tabs.List>
                <Tabs.Trigger value="filter-list">List Filter</Tabs.Trigger>
                {table.supports_sql_filter && (
                  <Tabs.Trigger value="filter-sql">SQL Filter</Tabs.Trigger>
                )}
              </Tabs.List>

              <Box pt="3">
                <Tabs.Content value="filter-list">
                  <CodeBlock
                    code={genTableCode({
                      catalog,
                      database,
                      table,
                      formatSqlFilter: false,
                    })}
                    lang='python'
                  />
                </Tabs.Content>

                {table.supports_sql_filter && (
                  <Tabs.Content value="filter-sql">
                    <Callout.Root mb="3">
                      <Callout.Icon>
                        <InfoCircledIcon />
                      </Callout.Icon>
                      <Callout.Text size={{ initial: '1', sm: '2' }}>
                        <Link href='https://datafusion.apache.org/user-guide/sql/index.html' target='_blank'>
                          DataFusion SQL Reference
                        </Link>
                        <br />
                        SQL filters are only supported for Delta Lake tables. Look for this "SQL Filter" section in the catalog to see what tables support SQL filtering.
                      </Callout.Text>
                    </Callout.Root>
                    <CodeBlock
                      code={genTableCode({
                        catalog,
                        database,
                        table,
                        formatSqlFilter: true,
                      })}
                      lang='python'
                    />
                  </Tabs.Content>
                )}
              </Box>
            </Tabs.Root>

            </Flex>

            <Flex direction='column' gap='4'>
              <Heading as='h3' size={{ initial: '3', sm: '4' }}>
                Partitions
              </Heading>

              <DataList.Root>
                {table.partitions.length
                  ? table.partitions.map((partition) => (
                    <DataList.Item key={partition.column_name}>
                      <DataList.Label highContrast>
                        <Text size={{ initial: '2', sm: '3' }}>
                          {partition.column_name}
                        </Text>
                      </DataList.Label>

                      <DataList.Value>
                        <Text 
                          color='gray'
                          size={{ initial: '1', sm: '2' }}
                        >
                          {partition.type_annotation ?? '(unknown type)'}
                        </Text>
                      </DataList.Value>
                    </DataList.Item>
                  ))
                  : <Text 
                      color='gray'
                      size={{ initial: '2', sm: '3' }}
                    >
                      This table is not partitioned.
                    </Text>}
              </DataList.Root>
            </Flex>

            <Flex direction='column' gap='4'>
              <Heading as='h3' size={{ initial: '3', sm: '4' }}>
                Schema
              </Heading>

              <DataList.Root>
                {table.columns
                  ? table.columns.map((column) => (
                    <DataList.Item key={column.name}>
                      <DataList.Label highContrast>
                        <Flex 
                          direction={{ initial: 'column', sm: 'row' }}
                          gap='1'
                          align={{ initial: 'start', sm: 'center' }}
                        >
                          <Text size={{ initial: '2', sm: '3' }}>
                            {column.name}
                          </Text>

                          <Flex gap='1' wrap='wrap'>
                            {column.readonly && (
                              <Tooltip content="This column is only computed after the data is loaded. You cannot use this column for filtering.">
                                <Badge ml="1" color="orange" size={{ initial: '1', sm: '2' }}>
                                  Read-only
                                </Badge>
                              </Tooltip>
                            )}

                            {column.filter_only && (
                              <Tooltip content="This column is only available as filters. It will not be in the loaded dataframe.">
                                <Badge ml="1" color="orange" size={{ initial: '1', sm: '2' }}>
                                  Filter-only
                                </Badge>
                              </Tooltip>
                            )}

                            {column.has_stats && (
                              <Tooltip content={
                                <Text>
                                  Adding filters with this column can significantly speed up query time by reducing the amount of data loaded.{" "}
                                  <Link href='https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-file-skipping/' target='_blank'>
                                    Learn more about stats.
                                  </Link>
                                </Text>
                              }>
                                <Badge ml="1" color="blue" size={{ initial: '1', sm: '2' }}>
                                  Stats
                                </Badge>
                              </Tooltip>
                            )}
                          </Flex>
                        </Flex>
                      </DataList.Label>

                      <DataList.Value>
                        <ArrowTypeView type={column.type} name={column.name}></ArrowTypeView>
                      </DataList.Value>
                    </DataList.Item>
                  ))
                  : <Text 
                      color='orange'
                      size={{ initial: '2', sm: '3' }}
                    >
                      Failed to generate schema for this table. Is this table missing a <Code>docs_partition</Code>?
                    </Text>}
              </DataList.Root>
            </Flex>
          </Flex>
        </Container>
      </ScrollArea>
    </Flex>
  )
}
