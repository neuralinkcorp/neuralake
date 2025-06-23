import { Flex, Separator } from '@radix-ui/themes'
import { generatePath, Outlet, useNavigate, useParams, useRouteLoaderData } from 'react-router-dom'

import Sidebar from '../../components/Sidebar'
import { ExportedCatalog } from '../../lib/types'

export default function CatalogKeyPage () {
  const catalog = useRouteLoaderData('catalogKey') as ExportedCatalog | null

  const { databaseKey } = useParams()
  const navigate = useNavigate()

  if (!catalog) return null

  return (
    <Flex 
      flexGrow='1' 
      flexBasis='0%' 
      overflow='hidden'
    >
      {/* Desktop Sidebar - Completely hidden on mobile */}
      <Sidebar
        eyebrow={catalog.name}
        heading='Databases'
        items={catalog.databases.map((database) => ({
          label: database.name,
          value: database.name
        }))}
        value={databaseKey}
        onValueChange={(databaseKey) => {
          navigate(generatePath(
            '/:catalogKey/:databaseKey',
            {
              catalogKey: catalog.name,
              databaseKey
            }
          ))
        }}
      />

      <Separator 
        orientation='vertical'
        size='4' 
        className='desktop-only'
      />

      {/* Content takes full width on mobile */}
      <Flex 
        flexGrow='1' 
        flexBasis='0%' 
        overflow='hidden'
      >
        <Outlet />
      </Flex>
    </Flex>
  )
}
