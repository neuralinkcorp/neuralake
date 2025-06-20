import { ExportedNeuralake } from './types'

// /data.json is expected to be copied into the
// root directory of the static site. 
const response = await fetch('/data.json')
if (!response.ok) {
  throw new Error(`Failed to load data: ${response.statusText}`)
}

export const neuralake = await response.json() as ExportedNeuralake