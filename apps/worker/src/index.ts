import { createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Worker } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "./config/env.js";
import { prisma } from "./lib/prisma.js";
import { deleteObjectIfExists, getObjectStream, putObject } from "./lib/s3.js";
import { generatePeaks, probeAudio } from "./lib/media.js";
import { buildUserExport } from "./lib/export.js";
import { deleteSessionAndTombstoneObjects, sweepPendingObjects } from "./lib/object-cleanup.js";
import { enqueueObjectSweep } from "./lib/queue.js";

const config = getConfig();
const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: null });
const log = (level: "info" | "error" | "warn", data: Record<string, unknown>, message: string) => {
  const output = JSON.stringify({ timestamp: new Date().toISOString(), level, service: "worker", ...data, message });
  if (level === "error") console.error(output);
  else if (level === "warn") console.warn(output);
  else console.log(output);
};

async function processMedia(mediaId: string) {
  const media = await prisma.mediaAsset.findUnique({ where: { id: mediaId } });
  if (!media) return;
  await prisma.mediaAsset.update({
    where: { id: mediaId },
    data: { status: "PROCESSING", failureCode: null, failureMessage: null },
  });

  const workDir = await mkdtemp(path.join(tmpdir(), "practice-media-"));
  const extension = path.extname(media.originalName).slice(0, 12);
  const localPath = path.join(workDir, `audio${extension}`);
  try {
    const stream = await getObjectStream(media.objectKey);
    await pipeline(stream, createWriteStream(localPath));
    const [probe, peaks] = await Promise.all([probeAudio(localPath), generatePeaks(localPath)]);
    await prisma.$transaction(async (tx) => {
      await tx.mediaAsset.update({
        where: { id: mediaId },
        data: {
          status: "READY",
          durationMs: probe.durationMs,
          codec: probe.codec,
          sampleRate: probe.sampleRate,
          channels: probe.channels,
          peaks,
          processedAt: new Date(),
          expiresAt: null,
          failureCode: null,
          failureMessage: null,
        },
      });
      await tx.practiceSession.updateMany({
        where: { id: media.sessionId, userId: media.userId, status: "DRAFT" },
        data: { status: "IN_REVIEW", version: { increment: 1 } },
      });
    });
    log("info", { mediaId, durationMs: Number(probe.durationMs) }, "media probe completed");
  } catch (error) {
    const message = error instanceof Error ? error.message : "UNKNOWN_MEDIA_ERROR";
    const code = message === "NO_AUDIO_STREAM" ? "NO_AUDIO_STREAM" : message === "INVALID_DURATION" ? "INVALID_DURATION" : "MEDIA_PROBE_FAILED";
    await prisma.mediaAsset.update({
      where: { id: mediaId },
      data: {
        status: "FAILED",
        failureCode: code,
        failureMessage: message === "NO_AUDIO_STREAM" ? "文件中没有可用的音轨" : "音频无法解析，请替换文件后重试",
        processedAt: new Date(),
      },
    });
    log("error", { mediaId, err: message }, "media probe failed");
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

const SWEEP_BATCH_SIZE = 50;
const SWEEP_INTERVAL_MS = 5 * 60_000;

async function cleanupSession(sessionId: string, job?: { attemptsMade: number; opts: { attempts?: number } }) {
  const session = await prisma.practiceSession.findUnique({
    where: { id: sessionId },
    select: { id: true, status: true },
  });
  if (!session) return; // 已被前一轮（重试）删除，任务天然幂等。
  if (session.status !== "DELETING") return; // 已恢复的练习不处理，避免误删。

  try {
    const tombstonedKeys = await prisma.$transaction((tx) => deleteSessionAndTombstoneObjects(tx, sessionId));
    log(
      "info",
      { sessionId, pendingObjectCount: tombstonedKeys.length },
      "session cleanup completed",
    );
    if (tombstonedKeys.length > 0) {
      // 物理删除移交给可重试的清扫流程：此处失败也不会留下孤儿对象，
      // 因为墓碑已随事务持久化，定时清扫 / 下次清扫任务必然会处理。
      await enqueueObjectSweep().catch((error) =>
        log("warn", { err: error instanceof Error ? error.message : String(error) }, "sweep enqueue failed"),
      );
    }
  } catch (error) {
    // 只在最后一次重试仍失败时落 DELETE_FAILED，避免重试间隙用户看到抖动状态。
    const attempts = job?.opts.attempts ?? 1;
    const isLastAttempt = (job?.attemptsMade ?? 0) + 1 >= attempts;
    if (isLastAttempt) {
      await prisma.practiceSession.updateMany({ where: { id: sessionId }, data: { status: "DELETE_FAILED" } });
    }
    throw error;
  }
}

async function sweepObjects() {
  const result = await sweepPendingObjects(prisma, deleteObjectIfExists, { limit: SWEEP_BATCH_SIZE });
  if (result.removed > 0 || result.resurrected > 0) {
    log(
      "info",
      { removed: result.removed, resurrected: result.resurrected, skipped: result.skipped },
      "pending media objects swept",
    );
  }
  // 本批取满说明可能还有积压，立即再清扫一轮。
  if (result.removed + result.skipped + result.resurrected >= SWEEP_BATCH_SIZE) {
    await enqueueObjectSweep().catch(() => undefined);
  }
}

async function exportData(exportId: string) {
  const task = await prisma.dataExport.findUnique({ where: { id: exportId } });
  if (!task || !task.objectKey) return;
  await prisma.dataExport.update({ where: { id: exportId }, data: { status: "PROCESSING" } });
  try {
    const output = await buildUserExport(task.userId, task.format);
    await putObject(task.objectKey, output.body, output.contentType);
    await prisma.dataExport.update({ where: { id: exportId }, data: { status: "READY", failure: null } });
  } catch (error) {
    await prisma.dataExport.update({
      where: { id: exportId },
      data: { status: "FAILED", failure: error instanceof Error ? error.message.slice(0, 500) : "EXPORT_FAILED" },
    });
    throw error;
  }
}

async function scanOverdueGoals() {
  const startOfToday = new Date();
  startOfToday.setUTCHours(0, 0, 0, 0);
  const result = await prisma.goal.updateMany({
    where: {
      dueDate: { lt: startOfToday },
      status: { in: ["OPEN", "IN_PROGRESS"] },
    },
    data: { status: "MISSED" },
  });
  if (result.count > 0) log("info", { count: result.count }, "overdue goals marked missed");
}

const worker = new Worker(
  "media-processing",
  async (job) => {
    if (job.name === "probe-media") return processMedia(String(job.data.mediaId));
    if (job.name === "cleanup-session") return cleanupSession(String(job.data.sessionId), job);
    if (job.name === "export-data") return exportData(String(job.data.exportId));
    if (job.name === "sweep-objects") return sweepObjects();
    throw new Error(`Unknown job: ${job.name}`);
  },
  { connection: redis, concurrency: config.WORKER_CONCURRENCY },
);

worker.on("failed", (job, error) => log("error", { jobId: job?.id, jobName: job?.name, err: error.message }, "job failed"));
worker.on("error", (error) => log("error", { err: error.message }, "worker error"));

const heartbeat = setInterval(async () => {
  await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
}, 10_000);
await redis.set("worker:heartbeat", new Date().toISOString(), "EX", 30);
await scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
// 启动即补扫历史遗留墓碑（如上次清扫时 S3 不可用或 Worker 崩溃）。
await sweepObjects().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "startup sweep failed"));
const overdueInterval = setInterval(() => {
  void scanOverdueGoals().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "overdue scan failed"));
}, 24 * 60 * 60_000);
// 定时兜底：清理重试持续失败的墓碑，以及任何残留孤儿对象。
const sweepInterval = setInterval(() => {
  void sweepObjects().catch((error) => log("error", { err: error instanceof Error ? error.message : String(error) }, "object sweep failed"));
}, SWEEP_INTERVAL_MS);

async function shutdown(signal: string) {
  log("info", { signal }, "shutting down worker");
  clearInterval(heartbeat);
  clearInterval(overdueInterval);
  clearInterval(sweepInterval);
  await worker.close();
  await redis.quit();
  await prisma.$disconnect();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
log("info", {}, "worker started");
