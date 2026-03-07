import { describe, test, expect, beforeAll } from "vitest";
import git from "isomorphic-git";
import { getLoophole } from "./setup.ts";
import { createLoopholeFS } from "./loophole-fs.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

let lh: any;
let fs: ReturnType<typeof createLoopholeFS>;

beforeAll(async () => {
  lh = await getLoophole();
  await lh.create("gitv", 64 * 1024 * 1024);
  await lh.mount("gitv", "/git");
  fs = createLoopholeFS(lh, "/git");
}, 30_000);

describe("isomorphic-git on loophole", () => {
  test("git init", async () => {
    await git.init({ fs, dir: "/repo" });

    // HEAD should exist.
    const head = await fs.promises.readFile("/repo/.git/HEAD", {
      encoding: "utf8",
    });
    expect(head).toContain("ref: refs/heads/");
  });

  test("git add and commit", async () => {
    await fs.promises.writeFile("/repo/hello.txt", enc.encode("hello world\n"));

    await git.add({ fs, dir: "/repo", filepath: "hello.txt" });
    const sha = await git.commit({
      fs,
      dir: "/repo",
      message: "initial commit",
      author: { name: "Test", email: "test@test.com" },
    });
    expect(sha).toBeTruthy();
    expect(sha.length).toBe(40);
  });

  test("git log", async () => {
    const commits = await git.log({ fs, dir: "/repo" });
    expect(commits.length).toBe(1);
    expect(commits[0].commit.message).toBe("initial commit\n");
  });

  test("git status after commit is clean", async () => {
    const status = await git.statusMatrix({ fs, dir: "/repo" });
    // statusMatrix returns [filepath, head, workdir, stage]
    // [1, 1, 1] means unchanged
    for (const [filepath, head, workdir, stage] of status) {
      expect([head, workdir, stage]).toEqual([1, 1, 1]);
    }
  });

  test("modify file shows in status", async () => {
    await fs.promises.writeFile(
      "/repo/hello.txt",
      enc.encode("hello modified\n"),
    );

    const status = await git.status({
      fs,
      dir: "/repo",
      filepath: "hello.txt",
    });
    expect(status).toBe("*modified");
  });

  test("second commit", async () => {
    await git.add({ fs, dir: "/repo", filepath: "hello.txt" });
    const sha = await git.commit({
      fs,
      dir: "/repo",
      message: "second commit",
      author: { name: "Test", email: "test@test.com" },
    });
    expect(sha).toBeTruthy();

    const commits = await git.log({ fs, dir: "/repo" });
    expect(commits.length).toBe(2);
    expect(commits[0].commit.message).toBe("second commit\n");
  });
});
