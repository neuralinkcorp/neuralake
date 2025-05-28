import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  root: process.env.NODE_ENV === 'production' ? '.' : 'src',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'src/index.html')
      },
      external: ['virtual:data-json']
    }
  },
  plugins: [react()],
  optimizeDeps: {
    exclude: ['virtual:data-json']
  }
})
