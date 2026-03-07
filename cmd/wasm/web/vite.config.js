import { defineConfig } from "vite";

export default defineConfig({
  server: {
    port: 18531,
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
  build: {
    modulePreload: { polyfill: false },
  },
  resolve: {
    alias: {
      "node:zlib": new URL("./stubs/zlib.js", import.meta.url).pathname,
    },
  },
});
