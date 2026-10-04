import type { CleanupPrisma } from "../src/lib/cleanup.js";

export interface FakeMediaRow {
  id: string;
  userId: string;
  sessionId: string;
  objectKey: string;
}

export interface FakeSessionRow {
  id: string;
  userId: string;
  status: string;
}

export interface FakePendingRow {
  id: string;
  userId: string;
  objectKey: string;
  attempts: number;
  lastError: string | null;
  nextAttemptAt: Date;
}

/** 简单互斥锁：后一个 acquire 必须等前一个 release，模拟会话级咨询锁。 */
class Mutex {
  private chain: Promise<void> = Promise.resolve();

  acquire(): Promise<() => void> {
    const previous = this.chain;
    let release!: () => void;
    this.chain = new Promise<void>((resolve) => {
      release = resolve;
    });
    return previous.then(() => release);
  }
}

let idSequence = 0;
function nextId(prefix: string): string {
  idSequence += 1;
  return `${prefix}-${idSequence}`;
}

export interface FakeDbOptions {
  failSessionDelete?: (sessionId: string) => boolean;
  now?: () => Date;
}

/**
 * 内存版 Prisma 替身：
 * - 共享状态 + 撤销日志模拟事务提交/回滚；
 * - 解析 pg_advisory_xact_lock 调用，按 key 加互斥锁并在事务结束释放，
 *   真实复现两个删除事务在同一对象上的串行化。
 */
export function createFakeDb(
  initialSessions: FakeSessionRow[],
  initialMedia: FakeMediaRow[],
  options: FakeDbOptions = {},
) {
  const sessions = new Map<string, FakeSessionRow>(initialSessions.map((session) => [session.id, { ...session }]));
  const media: FakeMediaRow[] = initialMedia.map((item) => ({ ...item }));
  const pending = new Map<string, FakePendingRow>();
  const mutexes = new Map<string, Mutex>();
  const now = options.now ?? (() => new Date());

  function mutexFor(key: string): Mutex {
    let mutex = mutexes.get(key);
    if (!mutex) {
      mutex = new Mutex();
      mutexes.set(key, mutex);
    }
    return mutex;
  }

  function countMedia(objectKey: string, userId: string): number {
    return media.filter((item) => item.objectKey === objectKey && item.userId === userId).length;
  }

  function createTx() {
    const undo: Array<() => void> = [];
    const releases: Array<() => void> = [];

    const tx = {
      async $executeRaw(strings: TemplateStringsArray, ...values: readonly unknown[]) {
        if (String(strings[0]).includes("pg_advisory_xact_lock")) {
          const lockKey = String(values[1]);
          const release = await mutexFor(lockKey).acquire();
          releases.push(release);
        }
        return 1;
      },
      practiceSession: {
        async delete(args: { where: { id: string } }) {
          const session = sessions.get(args.where.id);
          if (!session) {
            const error = new Error("session not found");
            (error as { code?: string }).code = "P2025";
            throw error;
          }
          if (options.failSessionDelete?.(session.id)) throw new Error("DB_UNAVAILABLE");
          const removedMedia = media.filter((item) => item.sessionId === session.id);
          undo.push(() => {
            sessions.set(session.id, session);
            media.push(...removedMedia);
          });
          sessions.delete(session.id);
          for (let index = media.length - 1; index >= 0; index -= 1) {
            if (media[index]!.sessionId === session.id) media.splice(index, 1);
          }
          return session;
        },
      },
      mediaAsset: {
        async count(args: { where: { objectKey: string; userId: string } }) {
          return countMedia(args.where.objectKey, args.where.userId);
        },
      },
      pendingObjectDeletion: {
        async createMany(args: {
          data: Array<{ userId: string; objectKey: string }>;
          skipDuplicates: boolean;
        }) {
          let count = 0;
          for (const input of args.data) {
            const existing = [...pending.values()].find((row) => row.objectKey === input.objectKey);
            if (existing) {
              if (args.skipDuplicates) continue;
              throw new Error("unique violation");
            }
            const row: FakePendingRow = {
              id: nextId("pending"),
              userId: input.userId,
              objectKey: input.objectKey,
              attempts: 0,
              lastError: null,
              nextAttemptAt: now(),
            };
            undo.push(() => pending.delete(row.id));
            pending.set(row.id, row);
            count += 1;
          }
          return { count };
        },
      },
    };
    return {
      tx,
      release() {
        undo.splice(0).reverse().forEach((rollback) => rollback());
        releases.splice(0).reverse().forEach((release) => release());
      },
      commit() {
        releases.splice(0).reverse().forEach((release) => release());
      },
    };
  }

  const db = {
    practiceSession: {
      async findUnique(args: { where: { id: string } }) {
        const session = sessions.get(args.where.id);
        if (!session) return null;
        return {
          id: session.id,
          userId: session.userId,
          mediaAssets: media
            .filter((item) => item.sessionId === session.id)
            .map((item) => ({ userId: item.userId, objectKey: item.objectKey })),
        };
      },
    },
    mediaAsset: {
      async count(args: { where: { objectKey: string; userId: string } }) {
        return countMedia(args.where.objectKey, args.where.userId);
      },
    },
    pendingObjectDeletion: {
      async findFirst(args: {
        where: { nextAttemptAt: { lte: Date } };
        orderBy?: { nextAttemptAt: "asc" };
      }) {
        const due = [...pending.values()].filter((row) => row.nextAttemptAt.getTime() <= args.where.nextAttemptAt.lte.getTime());
        due.sort((a, b) => a.nextAttemptAt.getTime() - b.nextAttemptAt.getTime());
        // 返回副本：真实 Prisma 每次查询都会返回新对象，调用方持有的
        // row 不会被随后的 update/updateMany 原地修改。
        return due[0] ? { ...due[0], nextAttemptAt: new Date(due[0]!.nextAttemptAt.getTime()) } : null;
      },
      async updateMany(args: {
        where: { id: string; nextAttemptAt: Date };
        data: { attempts?: number | { increment: number }; nextAttemptAt?: Date };
      }) {
        const row = pending.get(args.where.id);
        if (!row || row.nextAttemptAt.getTime() !== args.where.nextAttemptAt.getTime()) return { count: 0 };
        if (typeof args.data.attempts === "object") row.attempts += args.data.attempts.increment;
        else if (typeof args.data.attempts === "number") row.attempts = args.data.attempts;
        if (args.data.nextAttemptAt) row.nextAttemptAt = args.data.nextAttemptAt;
        return { count: 1 };
      },
      async update(args: {
        where: { id: string };
        data: { attempts?: number; lastError?: string | null; nextAttemptAt?: Date };
      }) {
        const row = pending.get(args.where.id);
        if (!row) throw new Error("pending not found");
        if (typeof args.data.attempts === "number") row.attempts = args.data.attempts;
        if (args.data.lastError !== undefined) row.lastError = args.data.lastError;
        if (args.data.nextAttemptAt) row.nextAttemptAt = args.data.nextAttemptAt;
        return row;
      },
      async delete(args: { where: { id: string } }) {
        if (!pending.delete(args.where.id)) throw new Error("pending not found");
        return { id: args.where.id };
      },
    },
    async $transaction<T>(callback: (tx: ReturnType<typeof createTx>["tx"]) => Promise<T>): Promise<T> {
      const handle = createTx();
      try {
        const result = await callback(handle.tx);
        handle.commit();
        return result;
      } catch (error) {
        handle.release();
        throw error;
      }
    },
  };

  return {
    db: db as unknown as CleanupPrisma,
    state: { sessions, media, pending },
    addMedia(item: Omit<FakeMediaRow, "id">): FakeMediaRow {
      const row = { ...item, id: nextId("media") };
      media.push(row);
      return row;
    },
  };
}

export interface FakeS3 {
  store: Set<string>;
  calls: string[];
  deleteObject: (objectKey: string) => Promise<void>;
}

export function createFakeS3(initialKeys: string[], shouldFail: () => boolean = () => false): FakeS3 {
  const store = new Set(initialKeys);
  const calls: string[] = [];
  return {
    store,
    calls,
    async deleteObject(objectKey: string) {
      calls.push(objectKey);
      if (shouldFail()) throw new Error("S3_UNAVAILABLE");
      store.delete(objectKey); // S3 删除天然幂等，缺失对象也算成功
    },
  };
}
