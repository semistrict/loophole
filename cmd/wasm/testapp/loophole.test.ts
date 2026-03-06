import { describe, test, expect, beforeAll } from "vitest";
import { getLoophole } from "./setup.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

let lh: any;

beforeAll(async () => {
  lh = await getLoophole();
}, 30_000);

describe("volume lifecycle", () => {
  test("create and mount volume", async () => {
    await lh.create("v1", 256 * 1024 * 1024);
    await lh.mount("v1", "/m1");
  });

  test("write and read back a file", async () => {
    await lh.writeFile("/m1", "hello.txt", enc.encode("hello from wasm!\n"));

    const data = await lh.readFile("/m1", "hello.txt");
    expect(dec.decode(data)).toBe("hello from wasm!\n");
  });

  test("write and read nested file", async () => {
    await lh.mkdirAll("/m1", "subdir/nested");
    await lh.writeFile("/m1", "subdir/nested/deep.txt", enc.encode("deep file\n"));

    const data = await lh.readFile("/m1", "subdir/nested/deep.txt");
    expect(dec.decode(data)).toBe("deep file\n");
  });

  test("readDir lists files and directories", async () => {
    const entries: string[] = await lh.readDir("/m1", "/");
    expect(entries).toContain("hello.txt");
    expect(entries).toContain("subdir");
  });

  test("binary data round-trips correctly", async () => {
    // 24-byte buffer (tests BLOB integrity).
    const blob = new Uint8Array(24);
    const view = new DataView(blob.buffer);
    view.setUint32(0, 0, true); // pos
    view.setBigUint64(4, 42n, true); // id
    view.setUint32(12, 1024, true); // size
    view.setUint32(16, 0, true); // off
    view.setUint32(20, 1024, true); // len

    await lh.writeFile("/m1", "binary.bin", blob);
    const read = await lh.readFile("/m1", "binary.bin");
    expect(new Uint8Array(read)).toEqual(blob);
  });

  test("snapshot and clone", async () => {
    await lh.snapshot("/m1", "snap1");

    await lh.writeFile("/m1", "after-snap.txt", enc.encode("written after snapshot\n"));

    await lh.clone("/m1", "v1-clone", "/m2");

    // Clone should have the original file.
    const cloneHello = await lh.readFile("/m2", "hello.txt");
    expect(dec.decode(cloneHello)).toBe("hello from wasm!\n");

    // Clone should have the post-snapshot file (clone is from live state).
    const afterSnap = await lh.readFile("/m2", "after-snap.txt");
    expect(dec.decode(afterSnap)).toBe("written after snapshot\n");

    await lh.unmount("/m2");
  });

  test("unmount", async () => {
    await lh.unmount("/m1");
  });
});
