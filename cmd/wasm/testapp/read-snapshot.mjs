// Read a snapshot created by the daemon (via CLI in Docker).
// Usage: node read-snapshot.mjs
//
// Expects:
//   - S3_ENDPOINT (default http://localhost:9000)
//   - BUCKET (default testbucket)
//   - loophole.wasm built via `make wasm`

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

// --- Configure S3 (no prefix — daemon writes at bucket root) ---
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

// No prefix — the daemon wrote objects at the bucket root.
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
console.log("Loading Go WASM...");
const wasmBytes = fs.readFileSync(wasmPath);
const go = new globalThis.Go();
const { instance } = await WebAssembly.instantiate(wasmBytes, go.importObject);
const runPromise = go.run(instance);

await new Promise((r) => setTimeout(r, 100));

const lh = globalThis.loophole;
if (!lh) {
  console.error("globalThis.loophole not set — WASM init failed");
  process.exit(1);
}

// --- Mount the snapshot and read files ---
try {
  console.log("\n=== Mount snapshot snap1 ===");
  await lh.mount("snap1", "/snap");
  console.log("OK: mounted snap1 at /snap");

  console.log("\n=== ReadDir / ===");
  const entries = await lh.readDir("/snap", "/");
  console.log(`  /: ${JSON.stringify(entries)}`);

  console.log("\n=== Read greeting.txt ===");
  const greeting = await lh.readFile("/snap", "greeting.txt");
  const greetingStr = new TextDecoder().decode(greeting);
  console.log(`  greeting.txt: ${JSON.stringify(greetingStr)}`);

  console.log("\n=== Read subdir/deep.txt ===");
  const deep = await lh.readFile("/snap", "subdir/deep.txt");
  const deepStr = new TextDecoder().decode(deep);
  console.log(`  subdir/deep.txt: ${JSON.stringify(deepStr)}`);

  console.log("\n=== Unmount ===");
  await lh.unmount("/snap");
  console.log("OK: unmounted");

  console.log("\n=== SUCCESS: Read snapshot from Node/WASM ===");
  process.exit(0);
} catch (e) {
  console.error("\nFAILED:", e);
  process.exit(1);
}
