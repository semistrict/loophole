// loophole-fs.ts — IFileSystem implementation for just-bash,
// backed by loophole's WASM API (lwext4 volume).

import type { IFileSystem, FsStat, MkdirOptions, RmOptions, CpOptions, DirentEntry } from "just-bash/browser";

// The loophole WASM API exposed as globalThis.loophole after Go init.
interface LoopholeAPI {
  readFile(mp: string, path: string): Promise<Uint8Array>;
  writeFile(mp: string, path: string, data: Uint8Array): Promise<void>;
  writeFileMode(mp: string, path: string, data: Uint8Array, mode: number): Promise<void>;
  readDir(mp: string, path: string): Promise<string[]>;
  readDirWithTypes(mp: string, path: string): Promise<Array<{ name: string; isFile: boolean; isDirectory: boolean; isSymbolicLink: boolean }>>;
  mkdirAll(mp: string, path: string): Promise<void>;
  remove(mp: string, path: string): Promise<void>;
  removeAll(mp: string, path: string): Promise<void>;
  stat(mp: string, path: string): Promise<{ name: string; size: number; mode: number; isDirectory: boolean; isSymbolicLink: boolean; mtimeMs: number }>;
  lstat(mp: string, path: string): Promise<{ name: string; size: number; mode: number; isDirectory: boolean; isSymbolicLink: boolean; mtimeMs: number }>;
  symlink(mp: string, target: string, linkPath: string): Promise<void>;
  readlink(mp: string, path: string): Promise<string>;
  link(mp: string, existingPath: string, newPath: string): Promise<void>;
  chmod(mp: string, path: string, mode: number): Promise<void>;
  rename(mp: string, oldPath: string, newPath: string): Promise<void>;
  utimes(mp: string, path: string, mtimeMs: number): Promise<void>;
}

function isENOENT(err: any): boolean {
  const msg = err?.message || "";
  return msg.includes("errno 44") || msg.includes("ENOENT") || msg.includes("not found");
}

function makeENOENT(err: any): Error {
  const e: any = new Error(err?.message || "ENOENT");
  e.code = "ENOENT";
  return e;
}

async function wrapENOENT<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err: any) {
    if (isENOENT(err)) throw makeENOENT(err);
    throw err;
  }
}

function toFsStat(info: { size: number; mode: number; isDirectory: boolean; isSymbolicLink: boolean; mtimeMs: number }): FsStat {
  return {
    isFile: !info.isDirectory && !info.isSymbolicLink,
    isDirectory: info.isDirectory,
    isSymbolicLink: info.isSymbolicLink,
    mode: info.mode,
    size: info.size,
    mtime: new Date(info.mtimeMs),
  };
}

function normPath(p: string): string {
  // Normalize: collapse //, resolve . and .., ensure leading /
  const parts = p.split("/").filter(Boolean);
  const resolved: string[] = [];
  for (const part of parts) {
    if (part === ".") continue;
    if (part === "..") { resolved.pop(); continue; }
    resolved.push(part);
  }
  return "/" + resolved.join("/");
}

export class LoopholeFs implements IFileSystem {
  constructor(private lh: LoopholeAPI, private mp: string) {}

  async readFile(path: string): Promise<string> {
    const data = await wrapENOENT(() => this.lh.readFile(this.mp, normPath(path)));
    return new TextDecoder().decode(data);
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    return wrapENOENT(() => this.lh.readFile(this.mp, normPath(path)));
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    const buf = typeof content === "string" ? new TextEncoder().encode(content) : content;
    await this.lh.writeFile(this.mp, normPath(path), buf);
  }

  async appendFile(path: string, content: string | Uint8Array): Promise<void> {
    const np = normPath(path);
    let existing = new Uint8Array(0);
    try {
      existing = await this.lh.readFile(this.mp, np);
    } catch (err: any) {
      if (!isENOENT(err)) throw err;
    }
    const appendBuf = typeof content === "string" ? new TextEncoder().encode(content) : content;
    const merged = new Uint8Array(existing.length + appendBuf.length);
    merged.set(existing);
    merged.set(appendBuf, existing.length);
    await this.lh.writeFile(this.mp, np, merged);
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.lh.stat(this.mp, normPath(path));
      return true;
    } catch {
      return false;
    }
  }

  async stat(path: string): Promise<FsStat> {
    const info = await wrapENOENT(() => this.lh.stat(this.mp, normPath(path)));
    return toFsStat(info);
  }

  async mkdir(path: string, options?: MkdirOptions): Promise<void> {
    const np = normPath(path);
    if (options?.recursive) {
      await this.lh.mkdirAll(this.mp, np);
    } else {
      // mkdirAll with a single dir
      await this.lh.mkdirAll(this.mp, np);
    }
  }

  async readdir(path: string): Promise<string[]> {
    return wrapENOENT(() => this.lh.readDir(this.mp, normPath(path)));
  }

  async readdirWithFileTypes(path: string): Promise<DirentEntry[]> {
    return wrapENOENT(() => this.lh.readDirWithTypes(this.mp, normPath(path)));
  }

  async rm(path: string, options?: RmOptions): Promise<void> {
    const np = normPath(path);
    try {
      if (options?.recursive) {
        await this.lh.removeAll(this.mp, np);
      } else {
        await this.lh.remove(this.mp, np);
      }
    } catch (err: any) {
      if (options?.force && isENOENT(err)) return;
      if (isENOENT(err)) throw makeENOENT(err);
      throw err;
    }
  }

  async cp(src: string, dest: string, options?: CpOptions): Promise<void> {
    const srcPath = normPath(src);
    const destPath = normPath(dest);
    const info = await wrapENOENT(() => this.lh.stat(this.mp, srcPath));

    if (info.isDirectory) {
      if (!options?.recursive) throw new Error("cp: -r not specified; omitting directory");
      await this._cpDir(srcPath, destPath);
    } else {
      const data = await this.lh.readFile(this.mp, srcPath);
      await this.lh.writeFile(this.mp, destPath, data);
    }
  }

  private async _cpDir(src: string, dest: string): Promise<void> {
    await this.lh.mkdirAll(this.mp, dest);
    const entries = await this.lh.readDir(this.mp, src);
    for (const name of entries) {
      const srcChild = src + "/" + name;
      const destChild = dest + "/" + name;
      const info = await this.lh.lstat(this.mp, srcChild);
      if (info.isDirectory) {
        await this._cpDir(srcChild, destChild);
      } else if (info.isSymbolicLink) {
        const target = await this.lh.readlink(this.mp, srcChild);
        await this.lh.symlink(this.mp, target, destChild);
      } else {
        const data = await this.lh.readFile(this.mp, srcChild);
        await this.lh.writeFile(this.mp, destChild, data);
      }
    }
  }

  async mv(src: string, dest: string): Promise<void> {
    await wrapENOENT(() => this.lh.rename(this.mp, normPath(src), normPath(dest)));
  }

  resolvePath(base: string, path: string): string {
    if (path.startsWith("/")) return normPath(path);
    return normPath(base + "/" + path);
  }

  getAllPaths(): string[] {
    return [];
  }

  async chmod(path: string, mode: number): Promise<void> {
    await wrapENOENT(() => this.lh.chmod(this.mp, normPath(path), mode));
  }

  async symlink(target: string, linkPath: string): Promise<void> {
    await this.lh.symlink(this.mp, target, normPath(linkPath));
  }

  async link(existingPath: string, newPath: string): Promise<void> {
    await wrapENOENT(() => this.lh.link(this.mp, normPath(existingPath), normPath(newPath)));
  }

  async readlink(path: string): Promise<string> {
    return wrapENOENT(() => this.lh.readlink(this.mp, normPath(path)));
  }

  async lstat(path: string): Promise<FsStat> {
    const info = await wrapENOENT(() => this.lh.lstat(this.mp, normPath(path)));
    return toFsStat(info);
  }

  async realpath(path: string): Promise<string> {
    // Follow symlinks iteratively
    let current = normPath(path);
    const seen = new Set<string>();
    for (;;) {
      if (seen.has(current)) throw new Error("realpath: symlink loop");
      seen.add(current);
      const info = await wrapENOENT(() => this.lh.lstat(this.mp, current));
      if (!info.isSymbolicLink) return current;
      const target = await this.lh.readlink(this.mp, current);
      if (target.startsWith("/")) {
        current = normPath(target);
      } else {
        const parent = current.substring(0, current.lastIndexOf("/")) || "/";
        current = normPath(parent + "/" + target);
      }
    }
  }

  async utimes(path: string, _atime: Date, mtime: Date): Promise<void> {
    await wrapENOENT(() => this.lh.utimes(this.mp, normPath(path), mtime.getTime()));
  }
}
