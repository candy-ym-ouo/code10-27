import type { Prisma, PrismaClient } from "@prisma/client";
import { lockObjectKeys, sortedUniqueObjectKeys } from "./object-locks.js";

/**
 * 删除清理的核心逻辑。
 *
 * 背景：同一段音频（同一个 S3 objectKey）可被同一用户的多个练习复用
 * （media_assets 多行共享 object_key）。两个练习并发删除时，如果先各自
 * “数引用、再删 S3、最后删 DB”，会出现双方都看到对方仍在引用而双双
 * 跳过对象删除，数据库记录清空后 S3 对象成为永久孤儿；反之数引用与删
 * 库不原子也可能误删仍被引用的对象。
 *
 * 做法：
 * 1. 删除练习的整个数据库操作在一个事务内完成，并对涉及的每个
 *    objectKey 按全局统一顺序加 Postgres 事务级咨询锁，使并发删除 /
 *    新建复用引用在数引用时串行化；
 * 2. 事务内删除练习（media_assets 随级联删除）后再统计剩余引用，引用
 *    归零的对象只写入一张持久化“待删除对象”表（删除意图）；
 * 3. S3 对象在事务外删除。这样数据库永远不会先于对象消失而留下无法
 *    重试的孤儿；对象删除失败时意图仍在，扫描器可随时重试；删除前还
 *    会再次确认没有新引用，绝不误删被其他练习引用的对象。
 */

export type CleanupPrisma = PrismaClient;
export type CleanupPrismaTx = Prisma.TransactionClient;

export interface CleanupDeps {
  db: CleanupPrisma;
  deleteObject: (objectKey: string) => Promise<void>;
  /** 删除数据库事务失败时标记练习为 DELETE_FAILED（沿用原有的可审计语义）。 */
  markDeleteFailed: (sessionId: string) => Promise<void>;
  /** 单条待删除对象的认领租约，必须大于一次 S3 删除的最长耗时。 */
  leaseMs?: number;
  /** 单次扫描最多处理多少条待删除对象。 */
  batchSize?: number;
  now?: () => Date;
}

const DEFAULT_LEASE_MS = 60_000;
const DEFAULT_BATCH_SIZE = 100;
const MAX_BACKOFF_MS = 60 * 60_000;

/** 指数退避（10s、20s、40s……），上限 1 小时。 */
export function backoffDelayMs(attempts: number): number {
  return Math.min(10_000 * 2 ** Math.max(0, attempts - 1), MAX_BACKOFF_MS);
}

/**
 * 删除单个练习。幂等：练习已不存在时直接成功返回（可能是重复任务或
 * 之前已删除完成）。
 */
export async function cleanupSession(deps: CleanupDeps, sessionId: string): Promise<void> {
  const session = await deps.db.practiceSession.findUnique({
    where: { id: sessionId },
    select: { id: true, userId: true, mediaAssets: { select: { userId: true, objectKey: true } } },
  });
  if (!session) return;

  // 同一练习内的多个素材可能指向同一对象，去重后按字典序作为加锁顺序。
  const objectKeys = sortedUniqueObjectKeys(session.mediaAssets.map((media) => media.objectKey));
  try {
    await deps.db.$transaction(async (tx) => {
      await lockObjectKeys(tx, objectKeys);
      // media_assets 随练习外键级联删除；删除后再统计，保证计数反映的是
      // “本练习的引用消失之后”的最终状态。
      await tx.practiceSession.delete({ where: { id: sessionId } });
      const toDelete: Prisma.PendingObjectDeletionCreateManyInput[] = [];
      for (const objectKey of objectKeys) {
        const remaining = await tx.mediaAsset.count({
          where: { objectKey, userId: session.userId },
        });
        if (remaining === 0) {
          // 只登记删除意图；对象真正删除发生在事务外，失败可重试。
          // skipDuplicates 保证并发路径下同一对象只有一条意图。
          toDelete.push({ userId: session.userId, objectKey });
        }
      }
      if (toDelete.length > 0) {
        await tx.pendingObjectDeletion.createMany({ data: toDelete, skipDuplicates: true });
      }
    });
  } catch (error) {
    await deps.markDeleteFailed(sessionId);
    throw error;
  }

  // 事务提交后立刻尝试删除，避免等待下一轮扫描；失败交给扫描器重试。
  await reapPendingDeletions(deps).catch(() => undefined);
}

/**
 * 认领一条到期的待删除对象：把 nextAttemptAt 推到租约之后。
 * 返回 true 表示当前调用者抢到了该条记录。租约保证多 Worker / 多任务
 * 不会并发删除同一对象；若处理进程崩溃，租约到期后记录可被再次认领。
 */
async function claimPendingDeletion(
  deps: CleanupDeps,
  row: { id: string; nextAttemptAt: Date },
  now: Date,
): Promise<boolean> {
  const leaseMs = deps.leaseMs ?? DEFAULT_LEASE_MS;
  const result = await deps.db.pendingObjectDeletion.updateMany({
    where: { id: row.id, nextAttemptAt: row.nextAttemptAt },
    data: {
      attempts: { increment: 1 },
      nextAttemptAt: new Date(now.getTime() + leaseMs),
    },
  });
  return result.count === 1;
}

/**
 * 扫描并删除到期的待删除对象。可重复调用、可重试：
 * - 删除前重新统计引用数，对象若已被新练习复用则直接丢弃删除意图；
 * - S3 删除成功才移除意图记录；
 * - S3 删除失败按指数退避安排下一次重试，意图持久保留，不产生孤儿。
 */
export async function reapPendingDeletions(deps: CleanupDeps): Promise<void> {
  const nowFn = deps.now ?? (() => new Date());
  const batchSize = deps.batchSize ?? DEFAULT_BATCH_SIZE;

  for (let processed = 0; processed < batchSize; processed += 1) {
    const now = nowFn();
    const row = await deps.db.pendingObjectDeletion.findFirst({
      where: { nextAttemptAt: { lte: now } },
      orderBy: { nextAttemptAt: "asc" },
    });
    if (!row) return;

    const claimed = await claimPendingDeletion(deps, row, now);
    if (!claimed) continue; // 被其他 Worker 抢先认领，跳过。

    // 关键的二次确认：删除意图创建后可能有新练习重新复用了该对象。
    const references = await deps.db.mediaAsset.count({
      where: { objectKey: row.objectKey, userId: row.userId },
    });
    if (references > 0) {
      await deps.db.pendingObjectDeletion.delete({ where: { id: row.id } });
      continue;
    }

    try {
      await deps.deleteObject(row.objectKey);
      await deps.db.pendingObjectDeletion.delete({ where: { id: row.id } });
    } catch (error) {
      // attempts 在认领时已经自增（崩溃导致只认领未删除的记录同样计入），
      // 这里只需按当前 attempts 安排指数退避，避免重复计数。安排好下一次
      // 重试时间后结束本轮扫描：租约/退避窗口内不再重复尝试同一对象。
      const delayMs = backoffDelayMs(row.attempts + 1);
      await deps.db.pendingObjectDeletion.update({
        where: { id: row.id },
        data: {
          lastError: error instanceof Error ? error.message.slice(0, 500) : "OBJECT_DELETE_FAILED",
          nextAttemptAt: new Date(now.getTime() + delayMs),
        },
      });
      return;
    }
  }
}
