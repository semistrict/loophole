// Read testvol created by the daemon (via CLI).
// Usage: node read-testvol.mjs

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createS3Adapter } from "./s3.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// --- Load WASM support ---
await import("./wasm_exec.js");

const wasmPath = path.join(__dirname, "..", "..", "..", "bin", "loophole.wasm");
if (!fs.existsSync(wasmPath)) {
  console.error("loophole.wasm not found. Run: make wasm");
  process.exit(1);
}

// --- Configure S3 ---
const endpoint = process.env.S3_ENDPOINT || "http://localhost:9000";
const bucket = process.env.BUCKET || "testbucket";

console.log(`S3: ${endpoint}/${bucket}`);

const s3 = createS3Adapter({
  endpoint,
  bucket,
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "rustfsadmin",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "rustfsadmin",
  },
});

globalThis.__loophole_s3 = {
  get: (key) => s3.get(key),
  getRange: (key, offset, length) => s3.getRange(key, offset, length),
  put: (key, data) => s3.put(key, data),
  putCAS: (key, data, etag) => s3.putCAS(key, data, etag),
  putIfNotExists: (key, data) => s3.putIfNotExists(key, data),
  del: (key) => s3.del(key),
  list: (pfx) => s3.list(pfx),
};

// --- Start WASM ---
console.log("Loading TinyGo WASM...");
const wasmBytes = fs.readFileSync(wasmPath);
const go = new globalThis.Go();
const { instance } = await WebAssembly.instantiate(wasmBytes, go.importObject);
const runPromise = go.run(instance);

await new Promise((r) => setTimeout(r, 500));

const lh = globalThis.loophole;
if (!lh) {
  console.error("globalThis.loophole not set — WASM init failed");
  process.exit(1);
}

// --- Mount testvol and read hello.txt ---
try {
  console.log("\n=== Mount testvol ===");
  await lh.mount("testvol", "/mnt");
  console.log("OK: mounted testvol at /mnt");

  console.log("\n=== ReadDir / ===");
  const entries = await lh.readDir("/mnt", "/");
  console.log(`  /: ${JSON.stringify(entries)}`);

  console.log("\n=== Read hello.txt ===");
  const data = await lh.readFile("/mnt", "hello.txt");
  const text = new TextDecoder().decode(data);
  console.log(`  hello.txt: ${JSON.stringify(text)}`);

  console.log("\n=== Unmount ===");
  await lh.unmount("/mnt");
  console.log("OK: unmounted");

  console.log("\n=== SUCCESS ===");
  process.exit(0);
} catch (e) {
  console.error("\nFAILED:", e);
  process.exit(1);
}
