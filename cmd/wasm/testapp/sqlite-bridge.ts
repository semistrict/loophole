// sqlite-bridge.ts — Bridges wa-sqlite to Go WASM via globalThis.__loophole_sql.
//
// Sets up wa-sqlite with a volume-backed VFS that calls into Go for mainDB I/O,
// then exposes a simple SQL API that the Go wasqlite driver calls through syscall/js.

import fs from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import { Factory } from "wa-sqlite/src/sqlite-api.js";
import * as VFS from "wa-sqlite/src/VFS.js";
import {
  SQLITE_ROW,
  SQLITE_OPEN_CREATE,
  SQLITE_OPEN_READWRITE,
  SQLITE_OPEN_URI,
} from "wa-sqlite/src/sqlite-constants.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// File kind constants (matching Go's fileKind).
const FILE_KINDS: Record<string, string> = {
  "main.db": "mainDB",
  "main.db-journal": "journal",
  "main.db-wal": "wal",
  "main.db-shm": "shm",
};

function formatOpenFlags(flags: number): string {
  const names: string[] = [];
  const known: Array<[number, string]> = [
    [0x00000001, "READONLY"],
    [0x00000002, "READWRITE"],
    [0x00000004, "CREATE"],
    [0x00000008, "DELETEONCLOSE"],
    [0x00000010, "EXCLUSIVE"],
    [0x00000020, "AUTOPROXY"],
    [0x00000040, "URI"],
    [0x00000080, "MEMORY"],
    [0x00000100, "MAIN_DB"],
    [0x00000200, "TEMP_DB"],
    [0x00000400, "TRANSIENT_DB"],
    [0x00000800, "MAIN_JOURNAL"],
    [0x00001000, "TEMP_JOURNAL"],
    [0x00002000, "SUBJOURNAL"],
    [0x00004000, "SUPER_JOURNAL"],
    [0x00008000, "NOMUTEX"],
    [0x00010000, "FULLMUTEX"],
    [0x00020000, "SHARED_CACHE"],
    [0x00040000, "PRIVATE_CACHE"],
    [0x00080000, "WAL"],
    [0x00100000, "NOFOLLOW"],
    [0x00200000, "EXRESCODE"],
  ];
  for (const [bit, name] of known) {
    if (flags & bit) names.push(name);
  }
  return names.length > 0 ? `${flags} (${names.join("|")})` : String(flags);
}

// LoopholeVFS: volume-backed VFS for wa-sqlite.
// JS only routes callbacks to the Go VFS implementation.
class LoopholeVFS extends VFS.Base {
  name = "loophole";

  // Open file handles routed to a specific Go VFS instance.
  goVFS: Map<number, any> = new Map();

  // Resolve VFS name from filename by checking registered Go VFS objects.
  private resolveGoVFS(vfsName: string): any {
    const registry = (globalThis as any).__loophole_vfs;
    if (!registry) return null;
    return registry[vfsName] || null;
  }

  private resolveGoVFSFromName(name: string): any {
    const vfsName = this.extractVFSName(name);
    return vfsName ? this.resolveGoVFS(vfsName) : null;
  }

  xOpen(
    name: string,
    fileId: number,
    flags: number,
    pOutFlags: DataView
  ): number {
    return this.handleAsync(async () => {
      const goVFS = this.resolveGoVFSFromName(name);
      const bareFilename = this.bareFilename(name);
      if (!FILE_KINDS[bareFilename] || !goVFS) {
        console.error(
          `[loophole-vfs] xOpen unroutable name=${name} bare=${bareFilename} flags=${formatOpenFlags(flags)}`
        );
        return VFS.SQLITE_CANTOPEN;
      }

      console.log(
        `[loophole-vfs] xOpen name=${name} bare=${bareFilename} flags=${formatOpenFlags(flags)}`
      );

      try {
        const outFlags = await goVFS.xOpen(fileId, bareFilename, flags);
        this.goVFS.set(fileId, goVFS);
        pOutFlags.setInt32(0, outFlags, true);
        return VFS.SQLITE_OK;
      } catch (e: any) {
        console.error(
          `[loophole-vfs] xOpen error name=${name} bare=${bareFilename} flags=${formatOpenFlags(flags)}:`,
          e?.message ?? e
        );
        return VFS.SQLITE_CANTOPEN;
      }
    });
  }

  xClose(fileId: number): number {
    return this.handleAsync(async () => {
      const goVFS = this.goVFS.get(fileId);
      if (goVFS) {
        try {
          await goVFS.xClose(fileId);
        } catch (e: any) {
          console.warn(`[loophole-vfs] xClose error:`, e.message);
        }
      }
      this.goVFS.delete(fileId);
      return VFS.SQLITE_OK;
    });
  }

  xRead(fileId: number, pData: Uint8Array, iOffset: number): number {
    return this.handleAsync(async () => {
      const goVFS = this.goVFS.get(fileId);
      if (!goVFS) return VFS.SQLITE_IOERR_READ;
      try {
        console.log(`[loophole-vfs] xRead fileId=${fileId} off=${iOffset} len=${pData.byteLength}`);
        const result = await goVFS.xRead(fileId, iOffset, pData.byteLength);
        const data = new Uint8Array(result.data);
        if (iOffset === 0) {
          console.log(
            `[loophole-vfs] xRead page0 head=${Buffer.from(data.subarray(0, 32)).toString("hex")} ascii=${JSON.stringify(new TextDecoder().decode(data.subarray(0, 16)))}`
          );
        }
        pData.set(data);
        if (result.shortRead) {
          return VFS.SQLITE_IOERR_SHORT_READ;
        }
        return VFS.SQLITE_OK;
      } catch (e: any) {
        console.error(`[loophole-vfs] xRead error:`, e.message);
        return VFS.SQLITE_IOERR_READ;
      }
    });
  }

  xWrite(fileId: number, pData: Uint8Array, iOffset: number): number {
    return this.handleAsync(async () => {
      const goVFS = this.goVFS.get(fileId);
      if (!goVFS) return VFS.SQLITE_IOERR_WRITE;
      try {
        await goVFS.xWrite(fileId, pData.slice(), iOffset);
        return VFS.SQLITE_OK;
      } catch (e: any) {
        console.error(`[loophole-vfs] xWrite error:`, e.message);
        return VFS.SQLITE_IOERR_WRITE;
      }
    });
  }

  xTruncate(fileId: number, iSize: number): number {
    return this.handleAsync(async () => {
      const goVFS = this.goVFS.get(fileId);
      if (!goVFS) return VFS.SQLITE_IOERR_TRUNCATE;
      try {
        await goVFS.xTruncate(fileId, iSize);
        return VFS.SQLITE_OK;
      } catch {
        return VFS.SQLITE_IOERR_TRUNCATE;
      }
    });
  }

  xSync(fileId: number, flags: number): number {
    return this.handleAsync(async () => {
      const goVFS = this.goVFS.get(fileId);
      if (goVFS) {
        try {
          await goVFS.xSync(fileId);
        } catch (e: any) {
          console.error(`[loophole-vfs] xSync error:`, e.message);
          return VFS.SQLITE_IOERR_FSYNC;
        }
      }
      return VFS.SQLITE_OK;
    });
  }

  xFileSize(fileId: number, pSize64: DataView): number {
    return this.handleAsync(async () => {
      const goVFS = this.goVFS.get(fileId);
      if (!goVFS) return VFS.SQLITE_IOERR_FSTAT;
      try {
        const size = await goVFS.xFileSize(fileId);
        pSize64.setBigInt64(0, BigInt(size), true);
        return VFS.SQLITE_OK;
      } catch {
        return VFS.SQLITE_IOERR_FSTAT;
      }
    });
  }

  xLock(fileId: number, flags: number): number {
    console.log(`[loophole-vfs] xLock fileId=${fileId} flags=${flags}`);
    return VFS.SQLITE_OK;
  }

  xUnlock(fileId: number, flags: number): number {
    console.log(`[loophole-vfs] xUnlock fileId=${fileId} flags=${flags}`);
    return VFS.SQLITE_OK;
  }

  xCheckReservedLock(fileId: number, pResOut: DataView): number {
    console.log(`[loophole-vfs] xCheckReservedLock fileId=${fileId}`);
    pResOut.setInt32(0, 0, true);
    return VFS.SQLITE_OK;
  }

  xFileControl(fileId: number, op: number, pArg: DataView): number {
    console.log(`[loophole-vfs] xFileControl fileId=${fileId} op=${op}`);
    return VFS.SQLITE_NOTFOUND;
  }

  xSectorSize(fileId: number): number {
    return 4096;
  }

  xDeviceCharacteristics(fileId: number): number {
    return (
      VFS.SQLITE_IOCAP_ATOMIC4K |
      VFS.SQLITE_IOCAP_SAFE_APPEND |
      VFS.SQLITE_IOCAP_SEQUENTIAL
    );
  }

  xDelete(name: string, syncDir: number): number {
    return this.handleAsync(async () => {
      const goVFS = this.resolveGoVFSFromName(name);
      const bareFilename = this.bareFilename(name);
      if (!goVFS) {
        return VFS.SQLITE_IOERR_DELETE;
      }
      try {
        await goVFS.xDelete(bareFilename);
        return VFS.SQLITE_OK;
      } catch {
        return VFS.SQLITE_IOERR_DELETE;
      }
    });
  }

  xAccess(name: string, flags: number, pResOut: DataView): number {
    return this.handleAsync(async () => {
      const goVFS = this.resolveGoVFSFromName(name);
      const bareFilename = this.bareFilename(name);
      if (!goVFS) {
        pResOut.setInt32(0, 0, true);
        return VFS.SQLITE_OK;
      }
      try {
        const exists = await goVFS.xAccess(bareFilename, flags);
        console.log(
          `[loophole-vfs] xAccess name=${name} bare=${bareFilename} exists=${exists}`
        );
        pResOut.setInt32(0, exists ? 1 : 0, true);
        return VFS.SQLITE_OK;
      } catch {
        pResOut.setInt32(0, 0, true);
        return VFS.SQLITE_OK;
      }
    });
  }

  // Extract Go VFS name from a URI or scoped path like
  // "file:jfs-vol/main.db?__govfs=jfs-vol" or "jfs-vol/main.db-wal".
  private extractVFSName(uri: string): string | null {
    const qIdx = uri.indexOf("?");
    if (qIdx >= 0) {
      const params = new URLSearchParams(uri.substring(qIdx + 1));
      const tagged = params.get("__govfs");
      if (tagged) return tagged;
    }

    let name = qIdx >= 0 ? uri.substring(0, qIdx) : uri;
    if (name.startsWith("file:")) {
      name = name.substring(5);
    }
    while (name.startsWith("/") && !name.startsWith("//")) {
      name = name.substring(1);
    }
    const slash = name.indexOf("/");
    return slash > 0 ? name.substring(0, slash) : null;
  }

  // Extract bare filename from URI/path: "file:jfs-vol/main.db?..." → "main.db"
  private bareFilename(uri: string): string {
    let name = uri;
    if (name.startsWith("file:")) {
      name = name.substring(5);
    }
    const qIdx = name.indexOf("?");
    if (qIdx >= 0) {
      name = name.substring(0, qIdx);
    }
    while (name.startsWith("/") && !name.startsWith("//")) {
      name = name.substring(1);
    }
    const slash = name.lastIndexOf("/");
    return slash >= 0 ? name.substring(slash + 1) : name;
  }
}

let sqlite3: any;
const openDBs = new Map<number, number>();
const openStmts = new Map<number, { stmt: number; db: number }>();
let nextDBId = 1;
let nextStmtId = 1;

export async function initSQLite(): Promise<void> {
  const wasmPath = path.join(
    __dirname,
    "node_modules",
    "wa-sqlite",
    "dist",
    "wa-sqlite-async.wasm"
  );
  const wasmBinary = fs.readFileSync(wasmPath);
  const module = await SQLiteAsyncESMFactory({ wasmBinary });
  sqlite3 = Factory(module);

  const vfs = new LoopholeVFS();
  sqlite3.vfs_register(vfs, false);

  (globalThis as any).__loophole_sql = {
    open: async (dsn: string): Promise<number> => {
      let vfsName: string | null = null;
      let filename = dsn;

      const qIdx = dsn.indexOf("?");
      if (qIdx >= 0) {
        const params = new URLSearchParams(dsn.substring(qIdx + 1));
        vfsName = params.get("vfs");

        // Strip go-sqlite3 specific params.
        for (const key of [...params.keys()]) {
          if (key.startsWith("_") || key === "vfs" || key === "cache") {
            params.delete(key);
          }
        }

        const remaining = params.toString();
        filename = remaining
          ? dsn.substring(0, qIdx) + "?" + remaining
          : dsn.substring(0, qIdx);
      }

      // Route jfs-* VFS names to our loophole VFS, but pass the original
      // name as a custom param and in the filename path so follow-up
      // journal/WAL callbacks stay routable to the same Go VFS.
      if (vfsName && vfsName.startsWith("jfs-")) {
        const q = filename.indexOf("?");
        let base = q >= 0 ? filename.substring(0, q) : filename;
        const suffix = q >= 0 ? filename.substring(q + 1) : "";
        if (base.startsWith("file:")) {
          base = base.substring(5);
        }
        while (base.startsWith("/") && !base.startsWith("//")) {
          base = base.substring(1);
        }
        const slash = base.lastIndexOf("/");
        if (slash >= 0) {
          base = base.substring(slash + 1);
        }
        filename = `file:${vfsName}/${base}?__govfs=${vfsName}`;
        if (suffix) {
          filename += `&${suffix}`;
        }
        vfsName = "loophole";
      }

      const flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI;
      const db = await sqlite3.open_v2(filename, flags, vfsName);

      const id = nextDBId++;
      openDBs.set(id, db);
      return id;
    },

    close: async (dbID: number): Promise<void> => {
      const db = openDBs.get(dbID);
      if (db !== undefined) {
        await sqlite3.close(db);
        openDBs.delete(dbID);
      }
    },

    exec: async (
      dbID: number,
      sql: string
    ): Promise<{ changes: number; lastID: number }> => {
      const db = openDBs.get(dbID)!;
      await sqlite3.exec(db, sql);
      return {
        changes: sqlite3.changes(db),
        lastID: 0,
      };
    },

    prepare: async (dbID: number, sql: string): Promise<number> => {
      const db = openDBs.get(dbID)!;
      const str = sqlite3.str_new(db, sql);
      try {
        const prepared = await sqlite3.prepare_v2(db, sqlite3.str_value(str));
        if (!prepared) {
          throw new Error(`failed to prepare: ${sql}`);
        }
        const id = nextStmtId++;
        openStmts.set(id, { stmt: prepared.stmt, db });
        return id;
      } finally {
        sqlite3.str_finish(str);
      }
    },

    stmtExec: async (
      stmtID: number,
      params: any[]
    ): Promise<{ changes: number; lastID: number }> => {
      const entry = openStmts.get(stmtID);
      if (!entry) throw new Error(`unknown stmt ${stmtID}`);
      const { stmt, db } = entry;

      await sqlite3.reset(stmt);
      if (params && params.length > 0) {
        for (let i = 0; i < params.length; i++) {
          bindValue(stmt, i + 1, params[i]);
        }
      }
      while ((await sqlite3.step(stmt)) === SQLITE_ROW) {}
      return {
        changes: sqlite3.changes(db),
        lastID: 0,
      };
    },

    stmtQuery: async (
      stmtID: number,
      params: any[]
    ): Promise<{ columns: string[]; rows: any[][] }> => {
      const entry = openStmts.get(stmtID);
      if (!entry) throw new Error(`unknown stmt ${stmtID}`);
      const { stmt } = entry;

      await sqlite3.reset(stmt);
      if (params && params.length > 0) {
        for (let i = 0; i < params.length; i++) {
          bindValue(stmt, i + 1, params[i]);
        }
      }

      const nCols = sqlite3.column_count(stmt);
      const columns: string[] = [];
      for (let i = 0; i < nCols; i++) {
        columns.push(sqlite3.column_name(stmt, i));
      }

      const rows: any[][] = [];
      while ((await sqlite3.step(stmt)) === SQLITE_ROW) {
        const row: any[] = [];
        for (let i = 0; i < nCols; i++) {
          row.push(readColumn(stmt, i));
        }
        rows.push(row);
      }

      return { columns, rows };
    },

    stmtClose: async (stmtID: number): Promise<void> => {
      const entry = openStmts.get(stmtID);
      if (entry) {
        await sqlite3.finalize(entry.stmt);
        openStmts.delete(stmtID);
      }
    },
  };

  console.log("sqlite-bridge: ready");
}

function bindValue(stmt: number, idx: number, value: any): void {
  if (value === null || value === undefined) {
    sqlite3.bind(stmt, idx, null);
  } else if (typeof value === "number") {
    sqlite3.bind(stmt, idx, value);
  } else if (typeof value === "string") {
    sqlite3.bind(stmt, idx, value);
  } else if (value instanceof Uint8Array) {
    sqlite3.bind(stmt, idx, value);
  } else {
    sqlite3.bind(stmt, idx, String(value));
  }
}

function readColumn(stmt: number, idx: number): any {
  const type = sqlite3.column_type(stmt, idx);
  switch (type) {
    case 1: // SQLITE_INTEGER
      return sqlite3.column_int(stmt, idx);
    case 2: // SQLITE_FLOAT
      return sqlite3.column_double(stmt, idx);
    case 3: // SQLITE_TEXT
      return sqlite3.column_text(stmt, idx);
    case 4: // SQLITE_BLOB
      // column_blob returns a subarray VIEW into WASM linear memory.
      // We must copy it immediately — the view is invalidated by any
      // subsequent SQLite call (step, reset, finalize, etc.).
      return new Uint8Array(sqlite3.column_blob(stmt, idx));
    case 5: // SQLITE_NULL
    default:
      return null;
  }
}
