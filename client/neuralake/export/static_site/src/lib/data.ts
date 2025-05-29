import { ExportedNeuralake } from './types'

const response = await fetch('/data.json')
if (!response.ok) {
  throw new Error(`Failed to load data: ${response.statusText}`)
}

export const neuralake = await response.json() as ExportedNeuralake