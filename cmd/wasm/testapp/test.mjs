import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { createS3Adapter } from "./s3.mjs";
import { initSQLite } from "./sqlite-bridge.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// --- Load WASM support ---
const wasmExecPath = path.join(__dirname, "wasm_exec.js");
if (!fs.existsSync(wasmExecPath)) {
  console.error("wasm_exec.js not found.");
  process.exit(1);
}
await import("./wasm_exec.js");

const wasmPath = path.join(__dirname, "..", "..", "..", "bin", "loophole.wasm");
if (!fs.existsSync(wasmPath)) {
  console.error("loophole.wasm not found. Run: make wasm");
  process.exit(1);
}

// --- Configure S3 ---
const endpoint = process.env.S3_ENDPOINT || "http://localhost:9000";
const bucket = process.env.BUCKET || "testbucket";
const prefix = `wasm-test-${crypto.randomUUID().slice(0, 8)}/`;

console.log(`S3: ${endpoint}/${bucket}/${prefix}`);

const s3 = createS3Adapter({
  endpoint,
  bucket,
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "rustfsadmin",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "rustfsadmin",
  },
});

// Expose S3 adapter to Go before starting WASM
globalThis.__loophole_s3 = {
  get: (key) => s3.get(prefix + key),
  getRange: (key, offset, length) => s3.getRange(prefix + key, offset, length),
  put: (key, data) => s3.put(prefix + key, data),
  putCAS: (key, data, etag) => s3.putCAS(prefix + key, data, etag),
  putIfNotExists: (key, data) => s3.putIfNotExists(prefix + key, data),
  del: (key) => s3.del(prefix + key),
  list: (pfx) => s3.list(prefix + pfx),
};

// --- Initialize wa-sqlite bridge ---
console.log("Initializing SQLite...");
await initSQLite();

// --- Start WASM ---
console.log("Loading Go WASM...");
const wasmBytes = fs.readFileSync(wasmPath);
const go = new globalThis.Go();
const { instance } = await WebAssembly.instantiate(wasmBytes, go.importObject);

const runPromise = go.run(instance);

// Give the Go runtime a tick to initialize and set globalThis.loophole
await new Promise((r) => setTimeout(r, 100));

const lh = globalThis.loophole;
if (!lh) {
  console.error("globalThis.loophole not set — WASM init failed");
  process.exit(1);
}

// --- Test ---
async function test() {
  console.log("\n=== Create volume ===");
  await lh.create("vol1", 256 * 1024 * 1024);
  console.log("OK: created vol1");

  console.log("\n=== Mount volume ===");
  await lh.mount("vol1", "/mnt");
  console.log("OK: mounted at /mnt");

  console.log("\n=== Write files ===");
  await lh.writeFile("/mnt", "hello.txt", new TextEncoder().encode("hello from wasm!\n"));
  await lh.mkdirAll("/mnt", "subdir/nested");
  await lh.writeFile("/mnt", "subdir/nested/deep.txt", new TextEncoder().encode("deep file\n"));
  console.log("OK: wrote files");

  console.log("\n=== Read files ===");
  const hello = await lh.readFile("/mnt", "hello.txt");
  const helloStr = new TextDecoder().decode(hello);
  console.log(`  hello.txt: ${JSON.stringify(helloStr)}`);
  if (helloStr !== "hello from wasm!\n") throw new Error("content mismatch");

  const deep = await lh.readFile("/mnt", "subdir/nested/deep.txt");
  console.log(`  deep.txt: ${JSON.stringify(new TextDecoder().decode(deep))}`);

  console.log("\n=== ReadDir ===");
  const entries = await lh.readDir("/mnt", "/");
  console.log(`  /: ${JSON.stringify(entries)}`);

  console.log("\n=== Snapshot ===");
  await lh.snapshot("/mnt", "snap1");
  console.log("OK: snapshot snap1");

  console.log("\n=== Write more after snapshot ===");
  await lh.writeFile("/mnt", "after-snap.txt", new TextEncoder().encode("written after snapshot\n"));
  console.log("OK: wrote after-snap.txt");

  console.log("\n=== Clone from snapshot ===");
  await lh.clone("/mnt", "vol1-clone", "/mnt2");
  console.log("OK: cloned to vol1-clone at /mnt2");

  console.log("\n=== Verify clone has original files ===");
  const cloneHello = await lh.readFile("/mnt2", "hello.txt");
  const cloneHelloStr = new TextDecoder().decode(cloneHello);
  console.log(`  clone hello.txt: ${JSON.stringify(cloneHelloStr)}`);
  if (cloneHelloStr !== "hello from wasm!\n") throw new Error("clone content mismatch");

  console.log("\n=== Verify clone has post-snapshot file (clone is from live, not snap) ===");
  const afterSnap = await lh.readFile("/mnt2", "after-snap.txt");
  console.log(`  clone after-snap.txt: ${JSON.stringify(new TextDecoder().decode(afterSnap))}`);

  console.log("\n=== Unmount ===");
  await lh.unmount("/mnt");
  await lh.unmount("/mnt2");
  console.log("OK: unmounted");

  console.log("\n=== ALL TESTS PASSED ===");
}

try {
  await test();
  process.exit(0);
} catch (e) {
  console.error("\nTEST FAILED:", e);
  process.exit(1);
}
