// loophole-fs.ts — Node fs-like adapter for isomorphic-git.
// Wraps the loophole WASM API into the fs interface that isomorphic-git expects.

export function createLoopholeFS(lh: any, mountpoint: string) {
  const promises = {
    async readFile(filepath: string, opts?: { encoding?: string }) {
      const data: Uint8Array = await wrapENOENT(() =>
        lh.readFile(mountpoint, filepath),
      );
      if (opts?.encoding === "utf8") {
        return new TextDecoder().decode(data);
      }
      return new Uint8Array(data);
    },

    async writeFile(
      filepath: string,
      data: Uint8Array | string,
      opts?: { mode?: number },
    ) {
      let buf: Uint8Array;
      if (typeof data === "string") {
        buf = new TextEncoder().encode(data);
      } else {
        buf = data;
      }
      if (opts?.mode !== undefined) {
        await lh.writeFileMode(mountpoint, filepath, buf, opts.mode);
      } else {
        await lh.writeFile(mountpoint, filepath, buf);
      }
    },

    async readdir(filepath: string) {
      return wrapENOENT(() => lh.readDir(mountpoint, filepath));
    },

    async mkdir(filepath: string, _opts?: { recursive?: boolean }) {
      await lh.mkdirAll(mountpoint, filepath);
    },

    async rmdir(filepath: string) {
      await wrapENOENT(() => lh.remove(mountpoint, filepath));
    },

    async unlink(filepath: string) {
      await wrapENOENT(() => lh.remove(mountpoint, filepath));
    },

    async stat(filepath: string) {
      const info = await wrapENOENT(() => lh.stat(mountpoint, filepath));
      return wrapStat(info);
    },

    async lstat(filepath: string) {
      const info = await wrapENOENT(() => lh.lstat(mountpoint, filepath));
      return wrapStat(info);
    },

    async readlink(filepath: string) {
      return wrapENOENT(() => lh.readlink(mountpoint, filepath));
    },

    async symlink(target: string, filepath: string) {
      await lh.symlink(mountpoint, target, filepath);
    },

    async chmod(filepath: string, mode: number) {
      await lh.chmod(mountpoint, filepath, mode);
    },
  };

  return { promises };
}

// lwext4 errno 44 = ENOENT (ext4-specific, not POSIX 2).
// isomorphic-git checks err.code === 'ENOENT' to handle missing files.
async function wrapENOENT<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err: any) {
    if (err?.message?.includes("errno 44") || err?.message?.includes("ENOENT")) {
      const e: any = new Error(err.message);
      e.code = "ENOENT";
      throw e;
    }
    throw err;
  }
}

function wrapStat(info: any) {
  return {
    type: info.isDirectory ? "dir" : info.isSymbolicLink ? "symlink" : "file",
    mode: info.mode,
    size: info.size,
    mtimeMs: info.mtimeMs,
    ctimeMs: info.mtimeMs, // ext4 ctime ≈ mtime for our purposes
    dev: 0,
    ino: 0,
    uid: 0,
    gid: 0,
    isFile: () => !info.isDirectory && !info.isSymbolicLink,
    isDirectory: () => info.isDirectory,
    isSymbolicLink: () => info.isSymbolicLink,
  };
}
