import '../styles/global.css'

import { ExternalLinkIcon } from '@radix-ui/react-icons'
import { Button, Flex, Heading, Select, Separator, Theme } from '@radix-ui/themes'
import '@radix-ui/themes/styles.css'
import { Outlet, generatePath, useNavigate, useParams } from 'react-router-dom'

import { neuralake } from '../lib/data'
import { FuzzySearchBox } from '../components/FuzzySearchBox'

export default function RootPage () {
  const { catalogKey } = useParams()
  const navigate = useNavigate()

  return (
    <Theme appearance='dark' accentColor='blue'>
      <Flex direction='column' height='100dvh'>
        <Flex px='3' align='center' gap='4'>
          <Flex flexGrow='1' py='2' gap='4' align='center'>
            <Heading as='h1' size='3'>Neuralake</Heading>

            <Select.Root
              value={catalogKey}
              onValueChange={(catalogKey) => {
                navigate(generatePath('/:catalogKey', { catalogKey }))
              }}
            >
              <Select.Trigger variant='surface' />

              <Select.Content>
                {neuralake.catalogs.map((catalog) => (
                  <Select.Item key={catalog.name} value={catalog.name}>
                    {catalog.name}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
          </Flex>

          <Flex flexGrow='1' justify='center'>
            {catalogKey && <FuzzySearchBox enableKeyboardShortcuts catalogKey={catalogKey} />}
          </Flex>

          <Flex flexGrow='1' />
          <Button
            variant='soft'
            style={{ top: 'var(--space-2)', right: 'var(--space-2)' }}
            onClick={() => window.open("")}
          >
            <ExternalLinkIcon /> Run on JupyterHub
          </Button>

        </Flex>

        <Separator size='4' />

        <Outlet />
      </Flex>
    </Theme>
  )
}
