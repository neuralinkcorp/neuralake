export default function dataJsonPlugin(config) {
  const virtualModuleId = 'virtual:data-json'
  const resolvedVirtualModuleId = '\0' + virtualModuleId

  return {
    name: 'data-json-plugin',
    resolveId(id) {
      if (id === virtualModuleId) {
        return resolvedVirtualModuleId
      }
    },
    load(id) {
      if (id === resolvedVirtualModuleId) {
        // Return the site config as a module
        return `export default ${JSON.stringify(config)}`
      }
    },
  }
} 