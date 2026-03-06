// setup.ts — Shared WASM + S3 + SQLite initialization for vitest.
// Imported by tests to get a handle to the loophole API.

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { createS3Adapter } from "./s3.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

let lh: any;
let initialized = false;

export async function getLoophole(): Promise<any> {
  if (initialized) return lh;
  initialized = true;

  // Load wasm_exec.js (Go WASM support).
  const wasmExecPath = path.join(__dirname, "wasm_exec.js");
  if (!fs.existsSync(wasmExecPath)) {
    throw new Error("wasm_exec.js not found");
  }
  await import("./wasm_exec.js");

  const wasmPath = path.join(__dirname, "..", "..", "..", "bin", "loophole.wasm");
  if (!fs.existsSync(wasmPath)) {
    throw new Error("loophole.wasm not found. Run: make wasm");
  }

  // Configure S3.
  const endpoint = process.env.S3_ENDPOINT || "http://localhost:9000";
  const bucket = process.env.BUCKET || "testbucket";
  const prefix = `wasm-test-${crypto.randomUUID().slice(0, 8)}/`;

  const s3 = createS3Adapter({
    endpoint,
    bucket,
    region: process.env.AWS_REGION || "us-east-1",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "rustfsadmin",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "rustfsadmin",
    },
  });

  (globalThis as any).__loophole_s3 = {
    get: (key: string) => s3.get(prefix + key),
    getRange: (key: string, offset: number, length: number) =>
      s3.getRange(prefix + key, offset, length),
    put: (key: string, data: Uint8Array) => s3.put(prefix + key, data),
    putCAS: (key: string, data: Uint8Array, etag: string) =>
      s3.putCAS(prefix + key, data, etag),
    putIfNotExists: (key: string, data: Uint8Array) =>
      s3.putIfNotExists(prefix + key, data),
    del: (key: string) => s3.del(prefix + key),
    list: (pfx: string) => s3.list(prefix + pfx),
  };

  // Start Go WASM.
  const wasmBytes = fs.readFileSync(wasmPath);
  const go = new (globalThis as any).Go();
  const { instance } = await WebAssembly.instantiate(wasmBytes, go.importObject);
  go.run(instance);

  // Wait for Go runtime to initialize.
  await new Promise((r) => setTimeout(r, 100));

  lh = (globalThis as any).loophole;
  if (!lh) {
    throw new Error("globalThis.loophole not set — WASM init failed");
  }

  return lh;
}
