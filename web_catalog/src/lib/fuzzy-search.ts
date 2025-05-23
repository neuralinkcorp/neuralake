import Fuse, { FuseResult } from 'fuse.js';
import { generatePath } from 'react-router-dom';

import { ExportedCatalog } from './types';
import { neuralake } from './data';

export type IndexItem =
  | {
    kind: 'database'
    resourceUrl: string
    databaseName: string
  }
  | {
    kind: 'table'
    resourceUrl: string
    databaseName: string
    tableName: string
  }
  | {
    kind: 'column'
    resourceUrl: string
    databaseName: string
    tableName: string
    columnOrPartitionName: string
    typeInfo: string | null
  }
  | {
    kind: 'partition'
    resourceUrl: string
    databaseName: string
    tableName: string
    columnOrPartitionName: string
    typeInfo: string | null
  }

export interface SegmentedIndex {
  databasesAndTables: Fuse<IndexItem>
  columnsAndPartitions: Fuse<IndexItem>
}

function buildIndexForCatalog (catalog: ExportedCatalog): SegmentedIndex {
  const databasesAndTablesIndex: IndexItem[] = []
  const columnsAndPartitionsIndex: IndexItem[] = []

  for (const database of catalog.databases) {
    databasesAndTablesIndex.push({
      kind: 'database',
      resourceUrl: generatePath('/:catalogKey/:databaseKey', {
        catalogKey: catalog.name,
        databaseKey: database.name
      }),
      databaseName: database.name
    })

    for (const table of database.tables) {
      const tableUrl = generatePath('/:catalogKey/:databaseKey/:tableKey', {
        catalogKey: catalog.name,
        databaseKey: database.name,
        tableKey: table.name
      })

      databasesAndTablesIndex.push({
        kind: 'table',
        resourceUrl: tableUrl,
        databaseName: database.name,
        tableName: table.name
      })

      for (const column of table.columns ?? []) {
        columnsAndPartitionsIndex.push({
          kind: 'column',
          resourceUrl: tableUrl,
          databaseName: database.name,
          tableName: table.name,
          columnOrPartitionName: column.name,
          typeInfo: column.type
        })
      }

      for (const partition of table.partitions) {
        columnsAndPartitionsIndex.push({
          kind: 'partition',
          resourceUrl: tableUrl,
          databaseName: database.name,
          tableName: table.name,
          columnOrPartitionName: partition.column_name,
          typeInfo: partition.type_annotation
        })
      }
    }
  }

  const databasesAndTables = new Fuse(databasesAndTablesIndex, {
    keys: [ 'databaseName', 'tableName' ],
    includeMatches: true,
    includeScore: true
  })
  const columnsAndPartitions = new Fuse(columnsAndPartitionsIndex, {
    keys: [ 'databaseName', 'tableName', 'columnOrPartitionName' ],
    includeMatches: true,
    includeScore: true
  })
  return { databasesAndTables, columnsAndPartitions }
}

export function searchSegmentedIndex (index: SegmentedIndex, query: string, limit: number = 10): FuseResult<IndexItem>[] {
  const databasesAndTablesResults = index.databasesAndTables.search(query, { limit })
  const columnsAndPartitionsResults = index.columnsAndPartitions.search(query, { limit })

  // Weight databases and tables slightly higher than columns and partitions.
  for (const result of databasesAndTablesResults) {
    result.score = (result.score ?? 0) - 0.05
  }

  const combined = [ ...databasesAndTablesResults, ...columnsAndPartitionsResults]
  combined.sort((a, b) => (a.score ?? 0) - (b.score ?? 0))
  return combined.slice(0, limit)
}

export const indexByCatalog = new Map<string, SegmentedIndex>()

for (const catalog of neuralake.catalogs) {
  indexByCatalog.set(catalog.name, buildIndexForCatalog(catalog))
}
