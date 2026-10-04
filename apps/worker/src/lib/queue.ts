import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { getConfig } from "../config/env.js";

let queue: Queue | undefined;
let redis: Redis | undefined;

export function getRedis(): Redis {
  if (!redis) {
    redis = new Redis(getConfig().REDIS_URL, { maxRetriesPerRequest: null });
  }
  return redis;
}

export function getMediaQueue(): Queue {
  if (!queue) {
    queue = new Queue("media-processing", { connection: getRedis().duplicate() });
  }
  return queue;
}
export async function enqueueProbe(mediaId: string): Promise<void> {
  await getMediaQueue().add(
    "probe-media",
    { mediaId },
    {
      jobId: `probe:${mediaId}:${Date.now()}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueCleanup(sessionId: string): Promise<void> {
  await getMediaQueue().add(
    "cleanup-session",
    { sessionId },
    {
      // 稳定 jobId：重复请求删除同一练习（含 DELETE_FAILED 后用户重试）不会产生并发清理任务。
      // 上一个任务执行结束后同名任务仍可重新入队。
      jobId: `cleanup:${sessionId}`,
      attempts: 5,
      backoff: { type: "exponential", delay: 5000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

/**
 * 清扫任务做按时间桶去重：30 秒窗口内只保留一个清扫任务，
 * 既能在练习删除后尽快处理墓碑，又不会在批量删除时刷爆队列。
 */
export async function enqueueObjectSweep(): Promise<void> {
  const bucket = Math.floor(Date.now() / 30_000);
  await getMediaQueue().add(
    "sweep-objects",
    { bucket },
    {
      jobId: `sweep:${bucket}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function enqueueExport(exportId: string): Promise<void> {
  await getMediaQueue().add(
    "export-data",
    { exportId },
    {
      jobId: `export:${exportId}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 3000 },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}

export async function closeQueue(): Promise<void> {
  if (queue) {
    await queue.close();
    queue = undefined;
  }
  if (redis) {
    await redis.quit();
    redis = undefined;
  }
}
