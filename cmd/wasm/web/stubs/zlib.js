// Stub for node:zlib in browser — gzip/gunzip commands won't work but everything else will.
const notSupported = () => { throw new Error("gzip/gunzip not supported in browser"); };
export const gunzipSync = notSupported;
export const gzipSync = notSupported;
export const constants = { Z_BEST_COMPRESSION: 9, Z_BEST_SPEED: 1, Z_DEFAULT_COMPRESSION: -1 };
export default { gunzipSync, gzipSync, constants };
