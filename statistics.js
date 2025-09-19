import path from "path";
import fs from "fs";
import { Client as EsClient } from "@elastic/elasticsearch";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const debugDir = path.join(process.cwd(), "debug-keys");
const statisticsDir = path.join(process.cwd(), "statistics");

const QUEUES = {
  welldyne: process.env.WELLDYNE_SQS_URL,
};

const S3_BUCKET = "supervize-internal-calls";

const OVERWRITE_ORPHANS = true;
const MAX_OVERWRITES   = 500;
const DRY_RUN          = false;
const PUSH_MISSING_TO_SQS = true; 

const z2 = (n) => String(n).padStart(2, "0");

function isFifo(url) {
  return !!url && url.endsWith(".fifo");
}

async function pushMissingEvalToSQS({
  clientId,
  contactId,
  recordingPath,
  transcriptId,
  reason = "MISSING_EVALUATION",
}) {
  if (!PUSH_MISSING_TO_SQS) return;
  if (!clientId) {
    console.warn("[SQS] Skip push: no clientId");
    return;
  }
  const queueUrl = QUEUES[clientId];
  if (!queueUrl || queueUrl.startsWith("<PUT_")) {
    console.warn("[SQS] Skip push: queue URL not configured for", clientId);
    return;
  }

  const body = {
    clientId,
    contactId,
    recordingPath,
    transcriptId,
    reason,
    ts: Date.now(),
  };

  const params = {
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(body),
  };

  if (isFifo(queueUrl)) {
    params.MessageGroupId = clientId;
    params.MessageDeduplicationId =
      (contactId || transcriptId || `${clientId}:${recordingPath || ""}`) +
      `:${new Date().toISOString().slice(0, 13)}`;
  }

  if (DRY_RUN) {
    console.log("[DRY_RUN][SQS] Would send:", params);
    return;
  }

  try {
    const res = await sqs.send(new SendMessageCommand(params));
    console.log("[SQS] Sent missing-eval message", {
      messageId: res.MessageId,
      contactId,
      transcriptId,
    });
  } catch (e) {
    console.warn("[SQS] Failed to send:", e?.message || e);
  }
}

async function listKeysForPrefix({ bucket, prefix }) {
  let ContinuationToken;
  const keys = [];
  do {
    const res = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken,
      })
    );
    for (const obj of res.Contents || []) {
      if (obj.Key) keys.push(obj.Key);
    }
    ContinuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (ContinuationToken);
  return keys;
}

async function listS3KeysForRange({ bucket, client, from, to }) {
  const start = new Date(from);
  const end = new Date(to);
  let d = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate()));
  const endDay = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate()));

  const all = new Set();
  while (d <= endDay) {
    const y = d.getUTCFullYear();
    const m = z2(d.getUTCMonth() + 1);
    const day = z2(d.getUTCDate());
    const prefix = `${client}/audio/connect/prod-exl-wdcc-use-1/CallRecordings/${y}/${m}/${day}/`;
    const keys = await listKeysForPrefix({ bucket, prefix });
    for (const k of keys) all.add(k);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return all;
}

let RESUME_AFTER = null;
const MAX_RESTARTS = 10;
let restartCount = 0;

function updateResumeCursorFromHit(hit) {
  const src = hit?._source || {};
  const lastDate = src?.[PLAN.dateField];
  const lastId = src?.transcript_id;
  if (lastDate && lastId) RESUME_AFTER = { date: lastDate, id: lastId };
}

function buildAfterCursorClause(after) {
  if (!after?.date || !after?.id) return null;
  return {
    bool: {
      should: [
        { range: { [PLAN.dateField]: { gt: after.date } } },
        {
          bool: {
            filter: [
              { range: { [PLAN.dateField]: { gte: after.date } } },
              { range: { transcript_id: { gt: after.id } } },
            ],
          },
        },
      ],
      minimum_should_match: 1,
    },
  };
}

const INDEX = {
  TRANSCRIPTS: "transcripts",
  CONTACTS: "contact___welldyne",
  EVALUATIONS: "contact_evaluation___welldyne",
};

const FILTERS = {
  clientId: "welldyne",
  from: "2025-08-05T00:00:00Z",
  to: "2025-08-05T23:59:59Z",
  limit: 10,
};

fs.mkdirSync(statisticsDir, { recursive: true });

const OUTPUT = {
  jsonlPath: path.join(statisticsDir, `hello-report-${Date.now()}.jsonl`),
  writeFile: true,
};

const DEBUG = false;
const dlog = (...args) => DEBUG && console.log("[DBG]", ...args);
const jline = (o) => JSON.stringify(o);
const baseName = (p) => (p ? path.posix.basename(p) : null);

const AUTOPROBE = true;
let PLAN = { dateField: "created_at", useClientFilter: true, useDate: true };

async function testTranscriptQueryVariant({ useClientFilter = true, dateField = "created_at", useDate = true, size = 1 }) {
  const f = [];
  if (useDate && (FILTERS.from || FILTERS.to)) {
    const range = {};
    if (FILTERS.from) range.gte = FILTERS.from;
    if (FILTERS.to) range.lte = FILTERS.to;
    f.push({ range: { [dateField]: range } });
  }
  if (useClientFilter && FILTERS.clientId) {
    f.push({ term: { external_client_id: FILTERS.clientId } });
  }
  const q = {
    index: INDEX.TRANSCRIPTS,
    size,
    query: f.length ? { bool: { filter: f } } : { match_all: {} },
  };
  const res = await client.search(q);
  return { hits: res.hits?.total?.value ?? res.hits?.hits?.length ?? 0, sample: res.hits?.hits?.[0]?._source, query: q };
}

async function chooseTranscriptQueryPlan() {
  if (!AUTOPROBE) return PLAN;
  const probes = [
    { label: "created_at + clientId", args: { dateField: "created_at", useClientFilter: true, useDate: true } },
    { label: "created_at (no clientId)", args: { dateField: "created_at", useClientFilter: false, useDate: true } },
    { label: "updated_at + clientId", args: { dateField: "updated_at", useClientFilter: true, useDate: true } },
    { label: "updated_at (no clientId)", args: { dateField: "updated_at", useClientFilter: false, useDate: true } },
    { label: "NO DATE + clientId", args: { dateField: "created_at", useClientFilter: true, useDate: false } },
    { label: "NO DATE (no clientId)", args: { dateField: "created_at", useClientFilter: false, useDate: false } },
  ];
  for (const p of probes) {
    try {
      const res = await testTranscriptQueryVariant(p.args);
      if (res.hits > 0) {
        PLAN = { dateField: p.args.dateField, useClientFilter: p.args.useClientFilter, useDate: p.args.useDate };
        return PLAN;
      }
    } catch {}
  }
  return PLAN;
}

function buildTranscriptQuery(afterCursor) {
  const f = [];
  if (PLAN.useDate && (FILTERS.from || FILTERS.to)) {
    const range = {};
    if (FILTERS.from) range.gte = FILTERS.from;
    if (FILTERS.to) range.lte = FILTERS.to;
    f.push({ range: { [PLAN.dateField]: range } });
  }
  if (PLAN.useClientFilter && FILTERS.clientId) {
    f.push({ term: { external_client_id: FILTERS.clientId } });
  }
  const afterClause = buildAfterCursorClause(afterCursor);
  if (afterClause) f.push(afterClause);
  const query = f.length ? { bool: { filter: f } } : { match_all: {} };
  dlog("buildTranscriptQuery()", jline(query));
  return query;
}

function buildTranscriptScrollParams(afterCursor) {
  const params = {
    index: INDEX.TRANSCRIPTS,
    scroll: "5m",
    size: Math.min(FILTERS.limit || 500, 100),
    _source: [
      "transcript_id",
      "source_file_path",
      "status",
      "external_client_id",
      "created_at",
      "updated_at",
      "original_filename",
      "detected_language",
    ],
    query: buildTranscriptQuery(afterCursor),
    sort: [{ [PLAN.dateField]: "asc" }, { transcript_id: "asc" }],
  };
  dlog(
    "buildTranscriptScrollParams()",
    jline({
      index: params.index,
      size: params.size,
      _source: params._source,
      sort: params.sort,
      query: params.query,
    })
  );
  return params;
}

async function findContactsByRecordingPath(sourcePath) {
  if (!sourcePath) return [];
  const q = {
    index: INDEX.CONTACTS,
    size: 10,
    _source: true,
    query: { match_phrase: { recordingPath: sourcePath } },
  };
  const { hits } = await client.search(q);
  return hits.hits;
}

async function findContactsFallback(tDoc) {
  const fname = baseName(tDoc._source?.source_file_path);
  const tries = [];
  if (fname) {
    tries.push({
      index: INDEX.CONTACTS,
      size: 10,
      _source: true,
      query: { term: { "externalId.keyword": fname } },
    });
  }
  const transcriptId = tDoc._source?.transcript_id;
  if (transcriptId) {
    tries.push({
      index: INDEX.CONTACTS,
      size: 10,
      _source: true,
      query: { term: { "externalId.keyword": String(transcriptId) } },
    });
  }
  const results = [];
  for (const query of tries) {
    const { hits } = await client.search(query);
    results.push(...hits.hits);
  }
  const seen = new Set();
  return results.filter((h) => (seen.has(h._id) ? false : (seen.add(h._id), true)));
}

async function findEvaluationsByContactId(contactId) {
  if (!contactId) return [];
  const q = {
    index: INDEX.EVALUATIONS,
    size: 10,
    _source: true,
    query: { match_phrase: { contactId: contactId } },
  };
  const { hits } = await client.search(q);
  return hits.hits;
}

const toTime = (s) => (s ? Date.parse(s) || 0 : 0);

function pickBestEvaluation(evals, { tPath, tid, contactId }) {
  const fname = baseName(tPath || "") || baseName(tid || "");

  const byContact = contactId
    ? evals.filter((e) => String(e?._source?.contactId || "") === String(contactId))
    : evals.slice();

  if (!byContact.length) return { chosen: null, reason: "no_contact_match" };

  if (tPath) {
    const exact = byContact.filter((e) => String(e?._source?.transcript || "") === String(tPath));
    if (exact.length) {
      exact.sort(
        (a, b) =>
          toTime(b?._source?.createdAt) - toTime(a?._source?.createdAt) ||
          String(b?._id).localeCompare(String(a?._id))
      );
      return { chosen: exact[0], reason: "exact_transcript_match" };
    }
  }

  if (fname) {
    const ends = byContact.filter((e) => String(e?._source?.transcript || "").endsWith(fname));
    if (ends.length) {
      ends.sort(
        (a, b) =>
          toTime(b?._source?.createdAt) - toTime(a?._source?.createdAt) ||
          String(b?._id).localeCompare(String(a?._id))
      );
      return { chosen: ends[0], reason: "filename_match" };
    }
  }

  byContact.sort(
    (a, b) =>
      toTime(b?._source?.createdAt) - toTime(a?._source?.createdAt) ||
      String(b?._id).localeCompare(String(a?._id))
  );
  return { chosen: byContact[0], reason: "newest" };
}

function asSummaryLine(kind, payload) {
  return {
    kind,
    transcript: {
      id: payload.tid,
      source_file_path: payload.tPath,
      created_at: payload.tCreatedAt,
      updated_at: payload.tUpdatedAt,
      external_client_id: payload.tClientId,
    },
    contact: payload.contact
      ? {
          id: payload.contact._source?.id,
          recordingPath: payload.contact._source?.recordingPath,
          clientId: payload.contact._source?.clientId,
          agentId: payload.contact._source?.agentId,
          timestamp: payload.contact._source?.timestamp,
        }
      : null,
    evaluation: payload.evaluation
      ? {
          id: payload.evaluation._source?.id,
          contactId: payload.evaluation._source?.contactId,
          score: payload.evaluation._source?.score,
          npsScore: payload.evaluation._source?.npsScore,
          createdAt: payload.evaluation._source?.createdAt,
        }
      : null,
    meta: payload.meta || undefined,
  };
}

async function overwriteSameKey(bucket, key) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Would overwrite same key: s3://${bucket}/${key}`);
    return;
  }
  const getRes = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = await streamToBuffer(getRes.Body);

  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: getRes.ContentType,
      Metadata: getRes.Metadata,
    })
  );

  console.log(`[OVERWRITE] Re-uploaded ${key} to same key (ObjectCreated:Put).`);
}

function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    if (stream && typeof stream.on === "function") {
      const chunks = [];
      stream.on("data", (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      stream.on("end", () => resolve(Buffer.concat(chunks)));
      stream.on("error", reject);
      return;
    }
    if (stream && typeof stream.arrayBuffer === "function") {
      stream
        .arrayBuffer()
        .then((ab) => resolve(Buffer.from(ab)))
        .catch(reject);
      return;
    }
    if (Buffer.isBuffer(stream)) return resolve(stream);
    reject(new Error("Unsupported GetObject Body type"));
  });
}

async function run() {
  const out = OUTPUT.writeFile ? fs.createWriteStream(OUTPUT.jsonlPath, { flags: "a" }) : null;
  const write = (obj) => {
    const line = jline(obj);
    console.log(line);
    if (out) out.write(line + "\n");
  };

  dlog("FILTERS:", jline(FILTERS));
  dlog("INDEX:", jline(INDEX));

  await chooseTranscriptQueryPlan();

  let s3Keys = new Set();
  const s3Enabled = S3_BUCKET && !S3_BUCKET.startsWith("<");
  if (s3Enabled) {
    try {
      s3Keys = await listS3KeysForRange({
        bucket: S3_BUCKET,
        client: FILTERS.clientId,
        from: FILTERS.from,
        to: FILTERS.to,
      });
    } catch (e) {
      console.warn("S3 listing failed; coverage will be omitted:", e?.message || e);
    }
  } else {
    console.warn("S3_BUCKET not set; skipping S3 coverage.");
  }

  const transcriptIds = new Set();
  const transcriptKeys = new Set();
  const transcriptKeysWithContact = new Set();
  const transcriptKeysWithEval = new Set();

  const counters = {
    scannedTranscripts: 0,
    ok: 0,
    missingContact: 0,
    missingEvaluation: 0,
    duplicateContacts: 0,
    duplicateEvaluations: 0,
    mismatch: 0,
  };

  let res = await client.search(buildTranscriptScrollParams(RESUME_AFTER));
  let scrollId = res._scroll_id;

  while (true) {
    const hits = res.hits?.hits || [];
    if (hits.length === 0) break;

    for (const t of hits) {
      updateResumeCursorFromHit(t);

      try {
        if (FILTERS.limit && counters.scannedTranscripts >= FILTERS.limit) break;

        counters.scannedTranscripts += 1;

        const tid = t._source?.transcript_id || t._id;
        const tPath = t._source?.source_file_path || null;
        const tCreatedAt = t._source?.created_at || null;
        const tUpdatedAt = t._source?.updated_at || null;
        const tClientId = t._source?.external_client_id || null;

        if (tid) transcriptIds.add(tid);
        if (tPath) transcriptKeys.add(tPath);

        let contacts = await findContactsByRecordingPath(tPath);
        if (contacts.length === 0) contacts = await findContactsFallback(t);

        if (contacts.length === 0) {
          counters.missingContact += 1;
          write(
            asSummaryLine("MISSING_CONTACT", {
              tid,
              tPath,
              tCreatedAt,
              tUpdatedAt,
              tClientId,
              contact: null,
              evaluation: null,
            })
          );
          continue;
        }

        if (contacts.length > 1) {
          counters.duplicateContacts += 1;
          write(
            asSummaryLine("DUPLICATE_CONTACTS", {
              tid,
              tPath,
              tCreatedAt,
              tUpdatedAt,
              tClientId,
              contact: contacts[0],
              evaluation: null,
            })
          );
          continue;
        }

        const contact = contacts[0];
        const cPath = contact._source?.recordingPath || null;
        if (tPath && cPath && tPath !== cPath) {
          counters.mismatch += 1;
          write(
            asSummaryLine("MISMATCH", {
              tid,
              tPath,
              tCreatedAt,
              tUpdatedAt,
              tClientId,
              contact,
              evaluation: null,
            })
          );
          continue;
        }

        const contactId = contact._source?.id;

        if (tPath) transcriptKeysWithContact.add(tPath);

        const evals = await findEvaluationsByContactId(contactId);

        if (evals.length === 0) {
          counters.missingEvaluation += 1;

          await pushMissingEvalToSQS({
            clientId: tClientId,
            contactId: (contact && contact._source && contact._source.id) || null,
            recordingPath: tPath,
            transcriptId: tid || null,
            reason: "MISSING_EVALUATION",
          });

          write(
            asSummaryLine("MISSING_EVALUATION", {
              tid,
              tPath,
              tCreatedAt,
              tUpdatedAt,
              tClientId,
              contact,
              evaluation: null,
            })
          );
          continue;
        }

        if (evals.length > 1) {
          if (tPath) transcriptKeysWithEval.add(tPath);

          const pick = pickBestEvaluation(evals, { tPath, tid, contactId });
          if (pick.chosen) {
            counters.ok += 1;
            write(
              asSummaryLine("OK", {
                tid,
                tPath,
                tCreatedAt,
                tUpdatedAt,
                tClientId,
                contact,
                evaluation: pick.chosen,
                meta: { evalPick: pick.reason, dupCount: evals.length },
              })
            );
          } else {
            counters.duplicateEvaluations += 1;
            write(
              asSummaryLine("DUPLICATE_EVALUATIONS", {
                tid,
                tPath,
                tCreatedAt,
                tUpdatedAt,
                tClientId,
                contact,
                evaluation: evals[0],
                meta: { note: "no clear best evaluation", dupCount: evals.length },
              })
            );
          }
          continue;
        }

        if (tPath) transcriptKeysWithEval.add(tPath);
        counters.ok += 1;
        write(
          asSummaryLine("OK", {
            tid,
            tPath,
            tCreatedAt,
            tUpdatedAt,
            tClientId,
            contact,
            evaluation: evals[0],
          })
        );
      } catch (docErr) {
        console.warn("Doc processing error:", docErr?.message || docErr);
        write({ kind: "DOC_ERROR", error: String(docErr), cursor: RESUME_AFTER });
        continue;
      }
    }

    if (FILTERS.limit && counters.scannedTranscripts >= FILTERS.limit) break;

    try {
      res = await client.scroll({ scroll_id: scrollId, scroll: "5m" });
      scrollId = res._scroll_id;
    } catch (e) {
      const et = e?.meta?.body?.error?.type;
      console.warn("Scroll error:", et || e?.message);

      if (restartCount < MAX_RESTARTS) {
        restartCount += 1;
        console.warn(`Restarting scroll from cursor (attempt ${restartCount}/${MAX_RESTARTS})`, RESUME_AFTER);
        try {
          res = await client.search(buildTranscriptScrollParams(RESUME_AFTER));
          scrollId = res._scroll_id;
          continue;
        } catch (restartErr) {
          console.error(
            "Failed to restart scroll:",
            restartErr?.meta?.body?.error || restartErr?.message || restartErr
          );
        }
      }
      break;
    }
  }

  if (scrollId) {
    try {
      await client.clearScroll({ scroll_id: scrollId });
    } catch {}
  }

  let summaryFields = {
    countS3Files: 0,
    countTranscripts: transcriptKeys.size,
    transcriptsMatchS3: 0,
    NotEvalOrContact: 0,
    OK: 0,
  };

  try {
    const s3KeysArr = Array.from(s3Keys);

    const s3HasTranscriptKeys = s3KeysArr.filter((k) => transcriptKeys.has(k));
    const s3NoTranscriptKeys  = s3KeysArr.filter((k) => !transcriptKeys.has(k));

    const s3HasTranscriptAndEvalKeys = s3HasTranscriptKeys.filter((k) => transcriptKeysWithEval.has(k));
    const s3HasTranscriptNoContactKeys = s3HasTranscriptKeys.filter((k) => !transcriptKeysWithContact.has(k));
    const s3HasTranscriptWithContactNoEvalKeys = s3HasTranscriptKeys.filter(
      (k) => transcriptKeysWithContact.has(k) && !transcriptKeysWithEval.has(k)
    );

    if (OVERWRITE_ORPHANS) {
      let done = 0;
      for (const key of s3NoTranscriptKeys) {
        if (done >= MAX_OVERWRITES) {
          console.warn(`[OVERWRITE] Hit MAX_OVERWRITES=${MAX_OVERWRITES}; stopping.`);
          break;
        }
        try {
          await overwriteSameKey(S3_BUCKET, key);
          done += 1;
        } catch (e) {
          console.warn(`[OVERWRITE] Failed for ${key}:`, e?.message || e);
        }
      }
      console.log(`[OVERWRITE] Completed ${done} same-key overwrites.`);
    }

    summaryFields = {
      countS3Files: s3Keys.size,
      countTranscripts: transcriptKeys.size,
      transcriptsMatchS3: s3HasTranscriptKeys.length,
      NotEvalOrContact: s3HasTranscriptNoContactKeys.length + s3HasTranscriptWithContactNoEvalKeys.length,
      OK: s3HasTranscriptAndEvalKeys.length,
    };

    console.log(JSON.stringify({ QUICK_SUMMARY: summaryFields }));
  } catch (e) {
    console.warn("Failed to compute summary fields:", e?.message || e);
  }

  const summary = { SUMMARY: summaryFields };
  if (OUTPUT.writeFile) fs.appendFileSync(OUTPUT.jsonlPath, jline(summary) + "\n");
  console.log(jline(summary));
}

run().catch((err) => {
  console.error("FATAL", err);
  if (err?.meta?.body?.error) console.error("ES error:", jline(err.meta.body.error));
  process.exit(1);
});
