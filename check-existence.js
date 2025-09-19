import {
  S3Client,
  GetBucketLocationCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";

const BUCKET = "supervize-internal-calls";

export async function existsWithReason(fullKey, clientRegion = "us-east-1") {
  const probeClient = new S3Client({ region: clientRegion });
  let bucketRegion = "us-east-1";
  try {
    const loc = await probeClient.send(new GetBucketLocationCommand({ Bucket: BUCKET }));
    bucketRegion = loc.LocationConstraint || "us-east-1";
  } catch (e) {
  }

  const s3 = new S3Client({ region: bucketRegion });

  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: fullKey }));
    return { exists: true, reason: "ok" };
  } catch (err) {
    const code = err?.$metadata?.httpStatusCode;

    if (code === 404 || err?.name === "NotFound") {
      return { exists: false, reason: "not_found" };
    }

    if (code === 403) {

      const prefix = fullKey.slice(0, fullKey.lastIndexOf("/") + 1);
      try {
        const listRes = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: prefix, MaxKeys: 1000 }));
        const found = (listRes.Contents || []).some(o => o.Key === fullKey);
        return {
          exists: found,
          reason: "forbidden",
          details: found ? "Object exists but GetObject/HeadObject is denied" : "Cannot confirm existence (no ListBucket or object not present)",
        };
      } catch (listErr) {
        return {
          exists: false,
          reason: "forbidden",
          details: "Head denied; also cannot list prefix (missing ListBucket or policy restriction)",
        };
      }
    }

    if (clientRegion !== bucketRegion) {
      return {
        exists: false,
        reason: "region_mismatch",
        details: `Client region=${clientRegion}, bucket region=${bucketRegion}. Recreate S3Client with bucket region.`,
      };
    }

    return { exists: false, reason: "unknown", details: err?.message || String(err) };
  }
}

const key = "welldyne/audio/connect/prod-exl-wdcc-use-1/CallRecordings/2025/08/05/90be0039-76e8-40fb-9765-09f89e40eeaf_20250805T12:15_UTC.wav";

const exists = await existsWithReason(key);
console.log(JSON.stringify(exists, null, 2)); 

