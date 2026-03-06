#!/usr/bin/env tsx
// cli.ts — Minimal Node CLI for mounting a loophole JuiceFS volume and browsing files.
//
// Usage:
//   tsx cli.ts [--prefix <s3prefix>] <command> [args...]
//
// Commands:
//   mount <volume>         Mount a volume (must be called first)
//   ls [path]              List directory (default: /)
//   cat <path>             Print file contents
//   stat <path>            Show file info
//
// Environment:
//   S3_ENDPOINT            (default: http://localhost:9000)
//   BUCKET                 (default: testbucket)
//   AWS_ACCESS_KEY_ID      (default: rustfsadmin)
//   AWS_SECRET_ACCESS_KEY  (default: rustfsadmin)

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

import fsNode from "node:fs";
import pathNode from "node:path";
import { fileURLToPath } from "node:url";
import { createS3Adapter } from "./s3.mjs";
import { initSQLite } from "./sqlite-bridge.ts";

const __dirname = pathNode.dirname(fileURLToPath(import.meta.url));

// --- Parse args ---
const args = process.argv.slice(2);
let prefix = "cross-test";
while (args.length > 0 && args[0].startsWith("--")) {
  if (args[0] === "--prefix" && args.length > 1) {
    args.shift();
    prefix = args.shift()!;
  } else {
    console.error(`Unknown flag: ${args[0]}`);
    process.exit(1);
  }
}

if (args.length === 0) {
  console.error("Usage: tsx cli.ts [--prefix <s3prefix>] mount <volume>");
  console.error("       tsx cli.ts ls [path]");
  console.error("       tsx cli.ts cat <path>");
  process.exit(1);
}

// --- Init WASM ---
const wasmExecPath = pathNode.join(__dirname, "wasm_exec.js");
if (!fsNode.existsSync(wasmExecPath)) {
  console.error("wasm_exec.js not found.");
  process.exit(1);
}
await import("./wasm_exec.js");

const wasmPath = pathNode.join(__dirname, "..", "..", "..", "bin", "loophole.wasm");
if (!fsNode.existsSync(wasmPath)) {
  console.error("loophole.wasm not found. Run: make wasm");
  process.exit(1);
}

// --- Configure S3 ---
const endpoint = process.env.S3_ENDPOINT || "http://localhost:9000";
const bucket = process.env.BUCKET || "testbucket";

const s3 = createS3Adapter({
  endpoint,
  bucket,
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "rustfsadmin",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "rustfsadmin",
  },
});

const s3prefix = prefix + "/";
(globalThis as any).__loophole_s3 = {
  get: (key: string) => s3.get(s3prefix + key),
  getRange: (key: string, offset: number, length: number) =>
    s3.getRange(s3prefix + key, offset, length),
  put: (key: string, data: Uint8Array) => s3.put(s3prefix + key, data),
  putCAS: (key: string, data: Uint8Array, etag: string) =>
    s3.putCAS(s3prefix + key, data, etag),
  putIfNotExists: (key: string, data: Uint8Array) =>
    s3.putIfNotExists(s3prefix + key, data),
  del: (key: string) => s3.del(s3prefix + key),
  list: (pfx: string) => s3.list(s3prefix + pfx),
};

await initSQLite();

const wasmBytes = fsNode.readFileSync(wasmPath);
const go = new (globalThis as any).Go();
const { instance } = await WebAssembly.instantiate(wasmBytes, go.importObject);
go.run(instance);
await new Promise((r) => setTimeout(r, 100));

const lh = (globalThis as any).loophole;
if (!lh) {
  console.error("globalThis.loophole not set — WASM init failed");
  process.exit(1);
}

// --- Run commands sequentially ---
const mountpoint = "/mnt";
let mounted = false;

async function ensureMounted(volume: string) {
  if (mounted) return;
  await lh.mount(volume, mountpoint);
  mounted = true;
}

const cmd = args.shift()!;

if (cmd === "mount") {
  const volume = args.shift();
  if (!volume) {
    console.error("Usage: mount <volume>");
    process.exit(1);
  }
  await ensureMounted(volume);
  console.log(`Mounted ${volume} at ${mountpoint}`);

  // Interactive-ish: process remaining args as subcommands
  // e.g.: tsx cli.ts mount crossvol ls / cat hello.txt
  while (args.length > 0) {
    const sub = args.shift()!;
    await runCommand(sub, args);
  }

  await lh.unmount(mountpoint);
} else {
  console.error(`Unknown command: ${cmd}. Must start with 'mount <volume>'.`);
  process.exit(1);
}

async function runCommand(sub: string, args: string[]) {
  switch (sub) {
    case "ls": {
      const dir = args.shift() || "/";
      const entries: string[] = await lh.readDir(mountpoint, dir);
      for (const e of entries) console.log(e);
      break;
    }
    case "cat": {
      const file = args.shift();
      if (!file) {
        console.error("cat: missing path");
        return;
      }
      const data = await lh.readFile(mountpoint, file);
      process.stdout.write(new Uint8Array(data));
      break;
    }
    case "stat": {
      const file = args.shift();
      if (!file) {
        console.error("stat: missing path");
        return;
      }
      // readFile to check existence and size
      const data = await lh.readFile(mountpoint, file);
      console.log(`${file}: ${data.byteLength} bytes`);
      break;
    }
    default:
      console.error(`Unknown subcommand: ${sub}`);
  }
}

process.exit(0);
