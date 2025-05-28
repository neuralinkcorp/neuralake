// see https://vitejs.dev/guide/api-plugin.html#virtual-modules
// see web_catalog/src/plugins/data-json-plugin.js

// @ts-ignore
import data from 'virtual:data-json'
import { ExportedNeuralake } from './types'

// We have to as-cast due to https://github.com/microsoft/TypeScript/issues/32063 and the type of invocation_type.
export const neuralake = data as ExportedNeuralake
