#!/usr/bin/env node

import { program } from 'commander';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Generate a static site from the provided configuration
 * @param {Object} config - The site configuration object
 * @param {string} outputDir - Output directory path
 * @returns {Promise<void>}
 */
export async function generateSite(config, outputDir) {
  const { build } = await import('vite');
  const reactPlugin = (await import('@vitejs/plugin-react')).default();
  const dataJsonPlugin = (await import('./src/plugins/data-json-plugin.js')).default;
  
  // Get the package root directory
  const packageRoot = path.resolve(__dirname);
  
  await build({
    root: path.join(packageRoot, 'src'),
    base: './',
    build: {
      outDir: outputDir,
      emptyOutDir: true,
      sourcemap: true
    },
    plugins: [reactPlugin, dataJsonPlugin(config)],
    define: {
      'process.env.SITE_CONFIG': JSON.stringify(config)
    }
  });
}

program
  .name('neuralake-catalog')
  .description('Generate a static site from a JSON configuration file')
  .requiredOption('-f, --file <path>', 'path to JSON configuration file')
  .option('-o, --output <directory>', 'output directory', './dist')
  .action(async (options) => {
    try {
      const configPath = path.resolve(process.cwd(), options.file);
      const outputPath = path.resolve(process.cwd(), options.output);
      
      if (!fs.existsSync(configPath)) {
        console.error(`Error: Config file not found at ${configPath}`);
        process.exit(1);
      }
      
      const configData = JSON.parse(fs.readFileSync(configPath, 'utf8'));
      
      console.log(`Generating site from ${configPath} to ${outputPath}...`);
      
      await generateSite(configData, outputPath);
      
      console.log('Site generated successfully!');
    } catch (error) {
      console.error('Error generating site:', error);
      process.exit(1);
    }
  });

program.parse();