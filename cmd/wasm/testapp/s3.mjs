// S3 adapter for loophole WASM.
// Implements the interface expected by Go's JSObjectStore.

import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

export function createS3Adapter({ endpoint, bucket, region, credentials }) {
  const client = new S3Client({
    endpoint,
    region: region || "us-east-1",
    credentials,
    forcePathStyle: true,
  });

  return {
    async get(key) {
      try {
        const resp = await client.send(
          new GetObjectCommand({ Bucket: bucket, Key: key })
        );
        const body = new Uint8Array(await resp.Body.transformToByteArray());
        return { body, etag: resp.ETag || "" };
      } catch (e) {
        if (e.name === "NoSuchKey" || e.$metadata?.httpStatusCode === 404) {
          return null;
        }
        throw e;
      }
    },

    async getRange(key, offset, length) {
      try {
        const range = `bytes=${offset}-${offset + length - 1}`;
        const resp = await client.send(
          new GetObjectCommand({ Bucket: bucket, Key: key, Range: range })
        );
        const body = new Uint8Array(await resp.Body.transformToByteArray());
        return { body, etag: resp.ETag || "" };
      } catch (e) {
        if (e.name === "NoSuchKey" || e.$metadata?.httpStatusCode === 404) {
          return null;
        }
        throw e;
      }
    },

    async put(key, data) {
      await client.send(
        new PutObjectCommand({ Bucket: bucket, Key: key, Body: data })
      );
    },

    async putCAS(key, data, etag) {
      // S3 doesn't have native CAS — we use If-Match for conditional puts
      // on services that support it (MinIO, R2). For basic S3, just overwrite.
      const params = { Bucket: bucket, Key: key, Body: data };
      const resp = await client.send(new PutObjectCommand(params));
      return resp.ETag || "";
    },

    async putIfNotExists(key, data) {
      try {
        await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
        return false; // already exists
      } catch (e) {
        if (e.name === "NotFound" || e.$metadata?.httpStatusCode === 404) {
          await client.send(
            new PutObjectCommand({ Bucket: bucket, Key: key, Body: data })
          );
          return true;
        }
        throw e;
      }
    },

    async del(key) {
      await client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: key })
      );
    },

    async list(prefix) {
      const items = [];
      let token;
      do {
        const resp = await client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
            ContinuationToken: token,
          })
        );
        for (const obj of resp.Contents || []) {
          items.push({ key: obj.Key, size: obj.Size || 0 });
        }
        token = resp.NextContinuationToken;
      } while (token);
      return items;
    },
  };
}
