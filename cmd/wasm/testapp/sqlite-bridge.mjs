// sqlite-bridge.mjs — Bridges wa-sqlite to Go WASM via globalThis.__loophole_sql.
//
// Sets up wa-sqlite with an async VFS (initially memory-backed), then exposes
// a simple SQL API that the Go wasqlite driver calls through syscall/js.

import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import SQLiteAsyncESMFactory from 'wa-sqlite/dist/wa-sqlite-async.mjs';
import { Factory } from 'wa-sqlite/src/sqlite-api.js';
import * as VFS from 'wa-sqlite/src/VFS.js';
import { SQLITE_ROW, SQLITE_DONE } from 'wa-sqlite/src/sqlite-constants.js';
import { MemoryVFS } from 'wa-sqlite/src/examples/MemoryVFS.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// LoopholeVFS: an async VFS backed by in-memory storage.
// TODO: Replace with volume-backed VFS that calls into Go for I/O.
class LoopholeVFS extends MemoryVFS {
  name = 'loophole';

  xOpen(name, fileId, flags, pOutFlags) {
    return this.handleAsync(async () => super.xOpen(name, fileId, flags, pOutFlags));
  }
  xClose(fileId) {
    return this.handleAsync(async () => super.xClose(fileId));
  }
  xRead(fileId, pData, iOffset) {
    return this.handleAsync(async () => super.xRead(fileId, pData, iOffset));
  }
  xWrite(fileId, pData, iOffset) {
    return this.handleAsync(async () => super.xWrite(fileId, pData, iOffset));
  }
  xTruncate(fileId, iSize) {
    return this.handleAsync(async () => super.xTruncate(fileId, iSize));
  }
  xFileSize(fileId, pSize64) {
    return this.handleAsync(async () => super.xFileSize(fileId, pSize64));
  }
  xSync(fileId, flags) {
    return this.handleAsync(async () => VFS.SQLITE_OK);
  }
  xDelete(name, syncDir) {
    return this.handleAsync(async () => super.xDelete(name, syncDir));
  }
  xAccess(name, flags, pResOut) {
    return this.handleAsync(async () => super.xAccess(name, flags, pResOut));
  }
}

let sqlite3;
const openDBs = new Map();   // dbID → db handle
const openStmts = new Map(); // stmtID → {stmt, db}
let nextDBId = 1;
let nextStmtId = 1;

export async function initSQLite() {
  // Pre-load the WASM binary so we don't need fetch() (which doesn't support file:// in Node).
  const wasmPath = path.join(__dirname, 'node_modules', 'wa-sqlite', 'dist', 'wa-sqlite-async.wasm');
  const wasmBinary = fs.readFileSync(wasmPath);
  const module = await SQLiteAsyncESMFactory({ wasmBinary });
  sqlite3 = Factory(module);

  // Register our VFS.
  const vfs = new LoopholeVFS();
  sqlite3.vfs_register(vfs, false);

  // Also register as 'default' so wa-sqlite has a default VFS.
  const defaultVFS = new MemoryVFS();
  // MemoryVFS is sync but we need it registered for default opens.
  // Actually the async module's default VFS should work. Let's just
  // make our loophole VFS available.

  // Expose SQL bridge to Go.
  globalThis.__loophole_sql = {
    open: async (dsn) => {
      // Parse DSN: file:main.db?vfs=loophole&...
      // Extract VFS name if specified.
      let vfsName = null;
      let filename = dsn;

      const qIdx = dsn.indexOf('?');
      if (qIdx >= 0) {
        const params = new URLSearchParams(dsn.substring(qIdx + 1));
        vfsName = params.get('vfs');
        // Remove vfs param from the filename — wa-sqlite uses open_v2's vfs arg instead.
        params.delete('vfs');
        const remaining = params.toString();
        filename = remaining ? dsn.substring(0, qIdx) + '?' + remaining : dsn.substring(0, qIdx);
      }

      // If the VFS name starts with "jfs-", use our loophole VFS.
      if (vfsName && vfsName.startsWith('jfs-')) {
        vfsName = 'loophole';
      }

      const flags = VFS.SQLITE_OPEN_CREATE | VFS.SQLITE_OPEN_READWRITE | VFS.SQLITE_OPEN_URI;
      const db = await sqlite3.open_v2(filename, flags, vfsName);
      const id = nextDBId++;
      openDBs.set(id, db);
      return id;
    },

    close: async (dbID) => {
      const db = openDBs.get(dbID);
      if (db !== undefined) {
        await sqlite3.close(db);
        openDBs.delete(dbID);
      }
    },

    exec: async (dbID, sql) => {
      const db = openDBs.get(dbID);
      await sqlite3.exec(db, sql);
      return {
        changes: sqlite3.changes(db),
        lastID: sqlite3.lastInsertRowId(db) ?? 0,
      };
    },

    prepare: async (dbID, sql) => {
      const db = openDBs.get(dbID);
      const prepared = await sqlite3.prepare_v2(db, sql);
      if (!prepared) {
        throw new Error(`failed to prepare: ${sql}`);
      }
      const id = nextStmtId++;
      openStmts.set(id, { stmt: prepared.stmt, db });
      return id;
    },

    stmtExec: async (stmtID, params) => {
      const entry = openStmts.get(stmtID);
      if (!entry) throw new Error(`unknown stmt ${stmtID}`);
      const { stmt, db } = entry;

      await sqlite3.reset(stmt);
      if (params && params.length > 0) {
        for (let i = 0; i < params.length; i++) {
          bindValue(stmt, i + 1, params[i]);
        }
      }
      while (await sqlite3.step(stmt) === SQLITE_ROW) {}
      return {
        changes: sqlite3.changes(db),
        lastID: sqlite3.lastInsertRowId(db) ?? 0,
      };
    },

    stmtQuery: async (stmtID, params) => {
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
      const columns = [];
      for (let i = 0; i < nCols; i++) {
        columns.push(sqlite3.column_name(stmt, i));
      }

      const rows = [];
      while (await sqlite3.step(stmt) === SQLITE_ROW) {
        const row = [];
        for (let i = 0; i < nCols; i++) {
          row.push(readColumn(stmt, i));
        }
        rows.push(row);
      }

      return { columns, rows };
    },

    stmtClose: async (stmtID) => {
      const entry = openStmts.get(stmtID);
      if (entry) {
        await sqlite3.finalize(entry.stmt);
        openStmts.delete(stmtID);
      }
    },
  };

  console.log('sqlite-bridge: ready');
}

function bindValue(stmt, idx, value) {
  if (value === null || value === undefined) {
    sqlite3.bind(stmt, idx, null);
  } else if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      sqlite3.bind(stmt, idx, value);
    } else {
      sqlite3.bind(stmt, idx, value);
    }
  } else if (typeof value === 'string') {
    sqlite3.bind(stmt, idx, value);
  } else if (value instanceof Uint8Array) {
    sqlite3.bind(stmt, idx, value);
  } else {
    sqlite3.bind(stmt, idx, String(value));
  }
}

function readColumn(stmt, idx) {
  const type = sqlite3.column_type(stmt, idx);
  switch (type) {
    case 1: // SQLITE_INTEGER
      return sqlite3.column_int(stmt, idx);
    case 2: // SQLITE_FLOAT
      return sqlite3.column_double(stmt, idx);
    case 3: // SQLITE_TEXT
      return sqlite3.column_text(stmt, idx);
    case 4: // SQLITE_BLOB
      return sqlite3.column_blob(stmt, idx);
    case 5: // SQLITE_NULL
    default:
      return null;
  }
}
