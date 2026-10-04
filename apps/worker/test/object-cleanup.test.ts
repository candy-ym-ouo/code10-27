import { beforeEach, describe, expect, it, vi } from "vitest";
import { deleteSessionAndTombstoneObjects, sweepPendingObjects } from "../src/lib/object-cleanup.js";

/**
 * 构造一个最小可用的 Prisma 模拟，验证两阶段删除协议的关键不变量：
 *  1. 事务内先对 media_objects 行加锁，再删 practice_sessions；
 *  2. 只把删除后零引用的 key 墓碑化，被其他练习引用的 key 保持 ACTIVE；
 *  3. 清扫阶段再次复核引用数，被复活的 key 不删 S3；
 *  4. 物理删除与登记行删除原子进行，失败可重试且幂等。
 */

interface AssetRow {
  id: string;
  sessionId: string;
  objectKey: string;
}
interface ObjectRow {
  id: string;
  objectKey: string;
  status: "ACTIVE" | "DELETE_PENDING";
  deleteAttempts: number;
  lastDeleteError: string | null;
  deleteRequestedAt: Date | null;
}

function createDb(initialAssets: AssetRow[], initialObjects: ObjectRow[], sessions: Array<{ id: string }>) {
  const assets = [...initialAssets];
  const objects = [...initialObjects];
  const sessionRows = sessions.map((session) => ({ ...session }));
  const lockedKeys = new Set<string>();
  const calls: string[] = [];

  const makeTx = () => ({
    mediaAsset: {
      findMany: vi.fn(async ({ where, distinct }: { where: { sessionId?: string; objectKey?: { in: string[] } }; distinct?: string[] }) => {
        void distinct;
        let rows = assets;
        if (where.sessionId) rows = rows.filter((row) => row.sessionId === where.sessionId);
        if (where.objectKey?.in) rows = rows.filter((row) => where.objectKey!.in.includes(row.objectKey));
        return rows.map((row) => ({ objectKey: row.objectKey }));
      }),
      count: vi.fn(async ({ where }: { where: { objectKey: string } }) => assets.filter((row) => row.objectKey === where.objectKey).length),
      delete: vi.fn(async ({ where }: { where: { id: string } }) => {
        const index = assets.findIndex((row) => row.id === where.id);
        if (index >= 0) assets.splice(index, 1);
      }),
    },
    practiceSession: {
      deleteMany: vi.fn(async ({ where }: { where: { id: string } }) => {
        calls.push(`deleteSession:${where.id}`);
        const index = sessionRows.findIndex((row) => row.id === where.id);
        if (index >= 0) {
          sessionRows.splice(index, 1);
          // 模拟 schema 中 practice_sessions -> media_assets 的 ON DELETE CASCADE。
          for (let i = assets.length - 1; i >= 0; i -= 1) {
            if (assets[i]!.sessionId === where.id) assets.splice(i, 1);
          }
        }
        return { count: index >= 0 ? 1 : 0 };
      }),
    },
    mediaObject: {
      findMany: vi.fn(async ({ where, orderBy: _orderBy, take }: { where: { status: string }; orderBy?: unknown; take?: number }) => {
        void where;
        return objects.filter((row) => row.status === "DELETE_PENDING").slice(0, take ?? 50);
      }),
      update: vi.fn(async ({ where, data }: { where: { id?: string; objectKey?: string }; data: Partial<ObjectRow> & { deleteAttempts?: number | { increment: number } } }) => {
        const row = objects.find((item) => (where.id ? item.id === where.id : item.objectKey === where.objectKey));
        if (!row) throw new Error("not found");
        if (typeof data.deleteAttempts === "object") {
          row.deleteAttempts += data.deleteAttempts.increment;
        } else if (data.deleteAttempts !== undefined) {
          row.deleteAttempts = data.deleteAttempts;
        }
        const { deleteAttempts: _ignored, ...rest } = data;
        void _ignored;
        Object.assign(row, rest);
        calls.push(`updateObject:${row.objectKey}:${row.status}`);
        return row;
      }),
      updateMany: vi.fn(async ({ where, data }: { where: { objectKey: { in: string[] }; status: string }; data: Partial<ObjectRow> }) => {
        let count = 0;
        for (const row of objects) {
          if (where.objectKey.in.includes(row.objectKey) && row.status === where.status) {
            Object.assign(row, data);
            calls.push(`tombstone:${row.objectKey}`);
            count += 1;
          }
        }
        return { count };
      }),
      delete: vi.fn(async ({ where }: { where: { id: string } }) => {
        calls.push(`deleteObjectRow:${objects.find((row) => row.id === where.id)?.objectKey}`);
        const index = objects.findIndex((row) => row.id === where.id);
        if (index >= 0) objects.splice(index, 1);
      }),
    },
    $executeRaw: vi.fn(async (fragment: unknown, ...rest: unknown[]) => {
      // 模拟行锁：记录被锁的 key。
      // Prisma 客户端会把标签模板组合成 Sql 片段（{ strings, values }）；
      // 模拟函数收到的是原始模板：首个参数为字符串数组，其余参数为表达式
      // （这里是 Prisma.join 产生的嵌套 Sql 片段）。
      const collect = (value: unknown) => {
        if (typeof value === "string") {
          lockedKeys.add(value);
        } else if (Array.isArray(value)) {
          for (const item of value) collect(item);
        } else if (value && typeof value === "object" && Array.isArray((value as { values?: unknown }).values)) {
          for (const item of (value as { values: unknown[] }).values) collect(item);
        }
      };
      collect(fragment);
      for (const value of rest) collect(value);
      calls.push(`lock:${[...lockedKeys].join("|")}`);
      return lockedKeys.size;
    }),
    $queryRaw: vi.fn(async (_template: unknown, ...values: unknown[]) => {
      // 模拟 FOR UPDATE SKIP LOCKED：按 id + DELETE_PENDING 命中。
      const id = String(values[0]);
      const row = objects.find((item) => item.id === id && item.status === "DELETE_PENDING");
      return row ? [{ id: row.id }] : [];
    }),
  });

  const tx = makeTx();
  const prisma = {
    ...tx,
    $transaction: vi.fn(async (arg: unknown) => {
      if (typeof arg === "function") return arg(makeTx());
      return undefined;
    }),
  };
  return { prisma: prisma as unknown as ReturnType<typeof makeTx> & { $transaction: typeof prisma.$transaction }, assets, objects, sessionRows, lockedKeys, calls };
}

describe("deleteSessionAndTombstoneObjects", () => {
  it("locks object rows, deletes session, tombstones only unreferenced keys", async () => {
    const db = createDb(
      [
        { id: "m1", sessionId: "s1", objectKey: "k-shared" },
        { id: "m2", sessionId: "s1", objectKey: "k-private" },
        { id: "m3", sessionId: "s2", objectKey: "k-shared" },
      ],
      [
        { id: "o1", objectKey: "k-shared", status: "ACTIVE", deleteAttempts: 0, lastDeleteError: null, deleteRequestedAt: null },
        { id: "o2", objectKey: "k-private", status: "ACTIVE", deleteAttempts: 0, lastDeleteError: null, deleteRequestedAt: null },
      ],
      [{ id: "s1" }, { id: "s2" }],
    );

    const tombstoned = await deleteSessionAndTombstoneObjects(db.prisma as never, "s1");

    expect(tombstoned.sort()).toEqual(["k-private"]);
    expect(db.objects.find((row) => row.objectKey === "k-private")?.status).toBe("DELETE_PENDING");
    // 被 s2 引用的共享 key 绝不能墓碑化（更不能误删物理对象）。
    expect(db.objects.find((row) => row.objectKey === "k-shared")?.status).toBe("ACTIVE");
    expect(db.sessionRows.some((row) => row.id === "s1")).toBe(false);
    // 先加锁、后删会话。
    expect(db.calls.indexOf("lock:k-shared|k-private")).toBeLessThan(db.calls.indexOf("deleteSession:s1"));
    expect(db.lockedKeys.has("k-shared")).toBe(true);
  });

  it("simulates concurrent cleanup of two sessions sharing one key: last deleter tombstones", async () => {
    // 并发事务 A(s1)、B(s2) 都持有行锁地串行执行。模拟 A 先提交：
    // A 删除 s1 后 k-shared 仍被 s2 引用 -> 不墓碑；
    // 之后 B 删除 s2 -> 零引用 -> 墓碑。不会遗留孤儿对象。
    const dbA = createDb(
      [
        { id: "m1", sessionId: "s1", objectKey: "k-shared" },
        { id: "m2", sessionId: "s2", objectKey: "k-shared" },
      ],
      [{ id: "o1", objectKey: "k-shared", status: "ACTIVE", deleteAttempts: 0, lastDeleteError: null, deleteRequestedAt: null }],
      [{ id: "s1" }, { id: "s2" }],
    );
    const resultA = await deleteSessionAndTombstoneObjects(dbA.prisma as never, "s1");
    expect(resultA).toEqual([]);
    expect(dbA.objects[0]!.status).toBe("ACTIVE");

    // B 看到的是 A 提交后的状态（共享同一模拟存储）。
    const resultB = await deleteSessionAndTombstoneObjects(dbA.prisma as never, "s2");
    expect(resultB).toEqual(["k-shared"]);
    expect(dbA.objects[0]!.status).toBe("DELETE_PENDING");
  });

  it("is idempotent when session was already deleted", async () => {
    const db = createDb([], [], []);
    const result = await deleteSessionAndTombstoneObjects(db.prisma as never, "gone");
    expect(result).toEqual([]);
  });
});

describe("sweepPendingObjects", () => {
  beforeEach(() => vi.clearAllMocks());

  it("deletes S3 object and registry row for unreferenced tombstones", async () => {
    const db = createDb(
      [],
      [{ id: "o1", objectKey: "k-orphan", status: "DELETE_PENDING", deleteAttempts: 1, lastDeleteError: "boom", deleteRequestedAt: new Date(0) }],
      [],
    );
    const deleteObject = vi.fn(async () => undefined);

    const result = await sweepPendingObjects(db.prisma as never, deleteObject);

    expect(deleteObject).toHaveBeenCalledWith("k-orphan");
    expect(db.objects.some((row) => row.objectKey === "k-orphan")).toBe(false);
    expect(result).toEqual({ removed: 1, resurrected: 0, skipped: 0 });
  });

  it("resurrects tombstones that gained a new reference and keeps the S3 object", async () => {
    const db = createDb(
      [{ id: "m1", sessionId: "s9", objectKey: "k-reused" }],
      [{ id: "o1", objectKey: "k-reused", status: "DELETE_PENDING", deleteAttempts: 2, lastDeleteError: "x", deleteRequestedAt: new Date(0) }],
      [{ id: "s9" }],
    );
    const deleteObject = vi.fn(async () => undefined);

    const result = await sweepPendingObjects(db.prisma as never, deleteObject);

    expect(deleteObject).not.toHaveBeenCalled();
    const row = db.objects.find((item) => item.objectKey === "k-reused");
    expect(row?.status).toBe("ACTIVE");
    expect(row?.deleteAttempts).toBe(0);
    expect(row?.deleteRequestedAt).toBeNull();
    expect(result).toEqual({ removed: 0, resurrected: 1, skipped: 0 });
  });

  it("keeps tombstone and records attempt when S3 deletion fails, so it can retry", async () => {
    const db = createDb(
      [],
      [{ id: "o1", objectKey: "k-fail", status: "DELETE_PENDING", deleteAttempts: 0, lastDeleteError: null, deleteRequestedAt: new Date(0) }],
      [],
    );
    const deleteObject = vi.fn(async () => {
      throw new Error("S3 unavailable");
    });

    const result = await sweepPendingObjects(db.prisma as never, deleteObject);

    expect(result).toEqual({ removed: 0, resurrected: 0, skipped: 1 });
    const row = db.objects.find((item) => item.objectKey === "k-fail");
    expect(row?.status).toBe("DELETE_PENDING");
    expect(row?.deleteAttempts).toBe(1);
    expect(row?.lastDeleteError).toBe("S3 unavailable");

    // 重试成功：对象仍可被清理，幂等。
    deleteObject.mockImplementation(async () => undefined);
    const retry = await sweepPendingObjects(db.prisma as never, deleteObject);
    expect(retry.removed).toBe(1);
    expect(db.objects).toHaveLength(0);
  });
});
