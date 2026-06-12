import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'node:path'

// Production build target: src/ha-agent/src/main/resources/static
// Maven jar will pick it up from there and Javalin serves it at "/".
const HA_AGENT_STATIC = path.resolve(
  __dirname,
  '../src/ha-agent/src/main/resources/static'
)

export default defineConfig({
  plugins: [vue()],
  build: {
    outDir: HA_AGENT_STATIC,
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 800
  },
  server: {
    port: 5173,
    proxy: {
      // During `npm run dev`, forward API calls to the local Agent.
      '/api':       { target: 'http://localhost:8080', changeOrigin: false },
      '/health':    { target: 'http://localhost:8080', changeOrigin: false },
      '/metrics':   { target: 'http://localhost:8080', changeOrigin: false }
    }
  }
})
