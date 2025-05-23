import { StrictMode } from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider, createHashRouter, generatePath, json, redirect } from 'react-router-dom'

import { neuralake } from './lib/data'

import RootPage from './pages'
import CatalogKeyPage from './pages/[catalogKey]'
import DatabaseKeyPage from './pages/[catalogKey]/[databaseKey]'
import TableKeyPage from './pages/[catalogKey]/[databaseKey]/[tableKey]'

function notFound () {
  throw new Response(undefined, { status: 404, statusText: 'Not Found' })
}

const router = createHashRouter([
  {
    path: '/',
    element: <RootPage />,
    children: [
      {
        index: true,
        element: null,
        loader: async () => {
          return redirect(generatePath('/:catalogKey', { catalogKey: neuralake.catalogs[0].name }))
        },
      },
      {
        path: '/:catalogKey',
        element: <CatalogKeyPage />,
        id: 'catalogKey',
        loader: async ({ params }) => {
          const catalog = neuralake.catalogs.find((catalog) => catalog.name === params.catalogKey)
          if (!catalog) return notFound()
          return json(catalog)
        },
        children: [
          {
            path: '/:catalogKey/:databaseKey?',
            element: <DatabaseKeyPage />,
            id: 'databaseKey',
            loader: async ({ params }) => {
              const catalog = neuralake.catalogs.find((catalog) => catalog.name === params.catalogKey)
              if (!catalog) return notFound()

              if (!params.databaseKey) return json(null)
              const database = catalog.databases.find((database) => database.name === params.databaseKey)
              if (!database) return notFound()
              return json(database)
            },
            children: [
              {
                path: '/:catalogKey/:databaseKey/:tableKey?',
                element: <TableKeyPage />,
                id: 'tableKey',
                loader: async ({ params }) => {
                  const catalog = neuralake.catalogs.find((catalog) => catalog.name === params.catalogKey)
                  if (!catalog) return notFound()

                  const database = catalog.databases.find((database) => database.name === params.databaseKey)
                  if (!database) return notFound()

                  if (!params.tableKey) return json(null)
                  const table = database.tables.find((table) => table.name === params.tableKey)
                  if (!table) return notFound()
                  return json(table)
                }
              }
            ]
          }
        ]
      }
    ]
  }
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RouterProvider router={router}></RouterProvider>
  </StrictMode>
)
