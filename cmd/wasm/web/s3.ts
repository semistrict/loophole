// S3 adapter for loophole WASM — browser-compatible.

import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

export interface S3Config {
  endpoint: string;
  bucket: string;
  region?: string;
  credentials: { accessKeyId: string; secretAccessKey: string };
  prefix?: string;
}

export function createS3Adapter(cfg: S3Config) {
  const client = new S3Client({
    endpoint: cfg.endpoint,
    region: cfg.region || "us-east-1",
    credentials: cfg.credentials,
    forcePathStyle: true,
  });
  const bucket = cfg.bucket;
  const pfx = cfg.prefix || "";

  return {
    async get(key: string) {
      try {
        const resp = await client.send(
          new GetObjectCommand({ Bucket: bucket, Key: pfx + key }),
        );
        const body = new Uint8Array(await resp.Body!.transformToByteArray());
        return { body, etag: resp.ETag || "" };
      } catch (e: any) {
        if (e.name === "NoSuchKey" || e.$metadata?.httpStatusCode === 404)
          return null;
        throw e;
      }
    },

    async getRange(key: string, offset: number, length: number) {
      try {
        const range = `bytes=${offset}-${offset + length - 1}`;
        const resp = await client.send(
          new GetObjectCommand({ Bucket: bucket, Key: pfx + key, Range: range }),
        );
        const body = new Uint8Array(await resp.Body!.transformToByteArray());
        return { body, etag: resp.ETag || "" };
      } catch (e: any) {
        if (e.name === "NoSuchKey" || e.$metadata?.httpStatusCode === 404)
          return null;
        throw e;
      }
    },

    async put(key: string, data: Uint8Array) {
      await client.send(
        new PutObjectCommand({ Bucket: bucket, Key: pfx + key, Body: data }),
      );
    },

    async putCAS(key: string, data: Uint8Array, _etag: string) {
      const resp = await client.send(
        new PutObjectCommand({ Bucket: bucket, Key: pfx + key, Body: data }),
      );
      return resp.ETag || "";
    },

    async putIfNotExists(key: string, data: Uint8Array) {
      try {
        await client.send(
          new HeadObjectCommand({ Bucket: bucket, Key: pfx + key }),
        );
        return false;
      } catch (e: any) {
        if (e.name === "NotFound" || e.$metadata?.httpStatusCode === 404) {
          await client.send(
            new PutObjectCommand({ Bucket: bucket, Key: pfx + key, Body: data }),
          );
          return true;
        }
        throw e;
      }
    },

    async del(key: string) {
      await client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: pfx + key }),
      );
    },

    async list(prefix: string) {
      const items: Array<{ key: string; size: number }> = [];
      let token: string | undefined;
      do {
        const resp = await client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: pfx + prefix,
            ContinuationToken: token,
          }),
        );
        for (const obj of resp.Contents || []) {
          // Strip the adapter prefix so Go receives keys relative to it
          const key = obj.Key!.startsWith(pfx) ? obj.Key!.slice(pfx.length) : obj.Key!;
          items.push({ key, size: obj.Size || 0 });
        }
        token = resp.NextContinuationToken;
      } while (token);
      return items;
    },
  };
}
