/**
 * 对象存储清理需要跨事务串行化的命名空间。与 Worker 端的值保持一致，
 * 保证“创建共享引用”和“删除练习”即使发生在不同进程，也会争夺同一把
 * Postgres 事务级咨询锁。
 */
export const OBJECT_LOCK_NAMESPACE = 0x50_43_4c_4e; // "PCLN"

/** 可以获取咨询锁的最小事务接口（Prisma 事务客户端天然满足）。 */
export interface AdvisoryLockClient {
  $executeRaw: (query: TemplateStringsArray, ...values: readonly unknown[]) => PromiseLike<unknown>;
}

/**
 * FNV-1a 32 位哈希。Postgres 的 pg_advisory_xact_lock(int4, int4) 接收
 * int32，因此对哈希结果做带符号转换。
 */
export function hashObjectKey(objectKey: string): number {
  let hash = 0x81_1c_9d_c5;
  for (let index = 0; index < objectKey.length; index += 1) {
    hash ^= objectKey.charCodeAt(index);
    hash = Math.imul(hash, 0x01_00_01_93);
  }
  return hash | 0;
}

/**
 * 在当前事务内按给定顺序获取对象键对应的排他咨询锁。
 *
 * - 必须在事务回调内调用，锁随事务提交/回滚自动释放，不会泄漏。
 * - 调用方需先对 objectKey 去重并排序，所有调用方按相同顺序加锁，
 *   从根本上避免多键交叉持锁导致的死锁。
 */
export async function lockObjectKeys(
  tx: AdvisoryLockClient,
  objectKeys: readonly string[],
): Promise<void> {
  for (const objectKey of objectKeys) {
    await tx.$executeRaw`SELECT pg_advisory_xact_lock(${OBJECT_LOCK_NAMESPACE}, ${hashObjectKey(objectKey)})`;
  }
}

/** objectKey 去重并排序，作为统一的加锁顺序。 */
export function sortedUniqueObjectKeys(objectKeys: readonly string[]): string[] {
  return [...new Set(objectKeys)].sort();
}
