import { Prisma, type PrismaClient } from "@prisma/client";

export interface SweepResult {
  removed: number;
  resurrected: number;
  skipped: number;
}

/**
 * 第一阶段删除：在同一个事务里对本练习涉及的每个 object_key 登记行加行锁，
 * 级联删除练习（连带 media_assets 引用），随后只把"全库再无任何 media_assets 引用"
 * 的 key 标记为 DELETE_PENDING，交给 {@link sweepPendingObjects} 做可重试的物理删除。
 *
 * 行锁串行化了同一 key 上的并发清理：两个复用同一音频的练习即使同时删除，
 * 后进入的事务必须等先进入的事务提交后才能读到最新引用计数，因此最后一个删除者
 * 一定会把登记行置为 DELETE_PENDING，不会两个都误判"仍被引用"而留下孤儿对象；
 * 而只要还有别的练习引用，计数 > 0，登记行保持 ACTIVE，物理对象绝不会被误删。
 *
 * 该函数幂等：练习已不存在时返回空数组；重复执行只会再次得到 0 引用键集合。
 */
export async function deleteSessionAndTombstoneObjects(
  tx: Prisma.TransactionClient,
  sessionId: string,
): Promise<string[]> {
  const keys = (
    await tx.mediaAsset.findMany({
      where: { sessionId },
      select: { objectKey: true },
      distinct: ["objectKey"],
    })
  ).map((media) => media.objectKey).sort();
  if (keys.length === 0) {
    // 没有引用也要删练习（可能练习本来就没有音频）。
    await tx.practiceSession.deleteMany({ where: { id: sessionId } });
    return [];
  }

  // 对每个 key 的登记行加 FOR UPDATE 行锁（与媒体上传复用、单条媒体删除路径共用同一锁），
  // 锁会持续到事务提交。按排序后的固定顺序加锁，避免两个多 key 清理事务交叉持锁造成死锁。
  await tx.$executeRaw`SELECT "id" FROM "media_objects" WHERE "object_key" IN (${Prisma.join(keys)}) FOR UPDATE`;

  // practice_sessions 删除会级联删除其全部 media_assets 引用。
  await tx.practiceSession.deleteMany({ where: { id: sessionId } });

  // 此时统计的是该事务快照 + 本事务改动之后的真实剩余引用：
  // 并发删除者尚未提交的删除对本事务不可见，其引用仍计入，不会提前墓碑化。
  const stillReferenced = new Set(
    (
      await tx.mediaAsset.findMany({
        where: { objectKey: { in: keys } },
        select: { objectKey: true },
        distinct: ["objectKey"],
      })
    ).map((media) => media.objectKey),
  );

  const orphanedKeys = keys.filter((key) => !stillReferenced.has(key));
  if (orphanedKeys.length > 0) {
    await tx.mediaObject.updateMany({
      where: { objectKey: { in: orphanedKeys }, status: "ACTIVE" },
      data: { status: "DELETE_PENDING", deleteRequestedAt: new Date(), lastDeleteError: null },
    });
  }
  return orphanedKeys;
}

/**
 * 第二阶段删除：逐条处理 DELETE_PENDING 登记行。
 *
 * 每条 key 在独立事务中再次加行锁并复核引用数后才删除 S3 对象与登记行，
 * 因此即便删除请求提交后用户又上传了内容相同的音频（复用同一 key，登记行被复活），
 * 清扫也会跳过而非误删正在使用的对象。
 *
 * S3 删除本身幂等：对象不存在视为成功；任何其他失败都会累加重试次数并保留墓碑，
 * 下一次清扫（清理后触发 / 定时扫描 / Worker 启动）继续重试。
 */
export async function sweepPendingObjects(
  prisma: PrismaClient,
  deleteObject: (objectKey: string) => Promise<void>,
  opts: { limit?: number } = {},
): Promise<SweepResult> {
  const limit = opts.limit ?? 50;
  const result: SweepResult = { removed: 0, resurrected: 0, skipped: 0 };

  const pending = await prisma.mediaObject.findMany({
    where: { status: "DELETE_PENDING" },
    orderBy: { deleteRequestedAt: "asc" },
    take: limit,
  });

  for (const object of pending) {
    try {
      const outcome = await prisma.$transaction(async (tx) => {
        // SKIP LOCKED：多实例 Worker / 并发清扫不会处理同一条墓碑。
        const locked = await tx.$queryRaw<Array<{ id: string }>>`
          SELECT "id" FROM "media_objects"
          WHERE "id" = ${object.id} AND "status" = 'DELETE_PENDING'
          FOR UPDATE SKIP LOCKED`;
        if (locked.length === 0) return "skipped" as const;

        const referenceCount = await tx.mediaAsset.count({ where: { objectKey: object.objectKey } });
        if (referenceCount > 0) {
          // 被新的复用上传复活：恢复 ACTIVE，绝不能删除物理对象。
          await tx.mediaObject.update({
            where: { id: object.id },
            data: { status: "ACTIVE", deleteRequestedAt: null, deleteAttempts: 0, lastDeleteError: null },
          });
          return "resurrected" as const;
        }

        // 行锁仍在本事务内，物理删除与登记行删除原子提交。
        // 期间任何想复用该 key 的上传都必须等待本事务结束，
        // 从而不可能出现"上传复用了对象但对象刚好被删"的窗口。
        await deleteObject(object.objectKey);
        await tx.mediaObject.delete({ where: { id: object.id } });
        return "removed" as const;
      });
      result[outcome] += 1;
    } catch (error) {
      const message = error instanceof Error ? error.message : "SWEEP_FAILED";
      await prisma.mediaObject.update({
        where: { id: object.id },
        data: { deleteAttempts: { increment: 1 }, lastDeleteError: message.slice(0, 500) },
      });
      result.skipped += 1;
    }
  }

  return result;
}
