declare module "wa-sqlite/dist/wa-sqlite-async.mjs" {
  export default function SQLiteAsyncESMFactory(config?: {
    wasmBinary?: ArrayBuffer | Uint8Array;
  }): Promise<any>;
}

declare module "wa-sqlite/src/sqlite-api.js" {
  export function Factory(module: any): any;
}

declare module "wa-sqlite/src/VFS.js" {
  export * from "wa-sqlite/src/sqlite-constants.js";
  export class Base {
    mxPathName: number;
    handleAsync(fn: () => Promise<number>): number;
    xOpen(name: string, fileId: number, flags: number, pOutFlags: any): number;
    xClose(fileId: number): number;
    xRead(fileId: number, pData: any, iOffset: number): number;
    xWrite(fileId: number, pData: any, iOffset: number): number;
    xTruncate(fileId: number, iSize: number): number;
    xFileSize(fileId: number, pSize64: any): number;
    xSync(fileId: number, flags: number): number;
    xDelete(name: string, syncDir: number): number;
    xAccess(name: string, flags: number, pResOut: any): number;
  }
}

declare module "wa-sqlite/src/sqlite-constants.js" {
  export const SQLITE_OK: number;
  export const SQLITE_ERROR: number;
  export const SQLITE_BUSY: number;
  export const SQLITE_ROW: number;
  export const SQLITE_DONE: number;
  export const SQLITE_OPEN_READONLY: number;
  export const SQLITE_OPEN_READWRITE: number;
  export const SQLITE_OPEN_CREATE: number;
  export const SQLITE_OPEN_URI: number;
}

declare module "wa-sqlite/src/examples/MemoryVFS.js" {
  import { Base } from "wa-sqlite/src/VFS.js";
  export class MemoryVFS extends Base {
    name: string;
  }
}
