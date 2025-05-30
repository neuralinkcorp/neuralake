import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import path from 'path';

const packageRoot = path.resolve(__dirname);

// https://vitejs.dev/config/
export default defineConfig({
  root: path.join(packageRoot, 'src'),
  base: './',
  build: {
    outDir: '../precompiled',
    emptyOutDir: true,
    sourcemap: true,
    target: 'ES2022',
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'src/index.html')
      },
    }
  },
  plugins: [react()],
  optimizeDeps: {
  }
})
