import { describe, expect, it, vi } from "vitest";
import { backoffDelayMs, cleanupSession, reapPendingDeletions } from "../src/lib/cleanup.js";
import { createFakeDb, createFakeS3 } from "./fake-db.js";

const USER = "user-1";
const SHARED_KEY = "users/user-1/shared/audio.m4a";
const OTHER_KEY = "users/user-1/other/audio.m4a";

function baseSetup(now: () => Date = () => new Date(0)) {
  const fake = createFakeDb(
    [
      { id: "session-a", userId: USER, status: "DELETING" },
      { id: "session-b", userId: USER, status: "DELETING" },
      { id: "session-c", userId: USER, status: "DELETING" },
    ],
    [
      { id: "media-a", userId: USER, sessionId: "session-a", objectKey: SHARED_KEY },
      { id: "media-b", userId: USER, sessionId: "session-b", objectKey: SHARED_KEY },
      { id: "media-c", userId: USER, sessionId: "session-c", objectKey: OTHER_KEY },
    ],
    { now },
  );
  const s3 = createFakeS3([SHARED_KEY, OTHER_KEY]);
  const markDeleteFailed = vi.fn(async (_sessionId: string) => {});
  const deps = {
    db: fake.db,
    deleteObject: s3.deleteObject,
    markDeleteFailed,
    leaseMs: 60_000,
    now,
  };
  return { fake, s3, deps, markDeleteFailed };
}

describe("cleanupSession - 并发删除共享音频", () => {
  it("两个练习并发删除同一对象时，对象只被删除一次且不留孤儿", async () => {
    const { fake, s3, deps } = baseSetup();

    await Promise.all([cleanupSession(deps, "session-a"), cleanupSession(deps, "session-b")]);

    expect(fake.state.sessions.has("session-a")).toBe(false);
    expect(fake.state.sessions.has("session-b")).toBe(false);
    expect(fake.state.media.length).toBe(1); // session-c 的素材保留
    expect(fake.state.pending.size).toBe(0); // 意图在对象删除后清空
    expect(s3.store.has(SHARED_KEY)).toBe(false); // 关键：对象没有成为孤儿
    expect(s3.calls.filter((key) => key === SHARED_KEY)).toHaveLength(1);
  });

  it("仍被其他练习引用的对象不会被误删", async () => {
    const { fake, s3, deps } = baseSetup();

    await cleanupSession(deps, "session-a");

    // session-b 仍引用共享对象：不能登记删除意图，也不能触碰 S3 对象
    expect(fake.state.media.filter((item) => item.objectKey === SHARED_KEY)).toHaveLength(1);
    expect(fake.state.pending.size).toBe(0);
    expect(s3.store.has(SHARED_KEY)).toBe(true);
    expect(s3.calls).not.toContain(SHARED_KEY);

    // 同时独立的 session-c 对象也保持不动（它没被删除）
    expect(s3.store.has(OTHER_KEY)).toBe(true);
  });

  it("互不共享对象的练习并发删除互不影响", async () => {
    const { fake, s3, deps } = baseSetup();

    await Promise.all([cleanupSession(deps, "session-b"), cleanupSession(deps, "session-c")]);

    expect(fake.state.sessions.size).toBe(1);
    expect(fake.state.sessions.has("session-a")).toBe(true);
    expect(fake.state.pending.size).toBe(0);
    expect(s3.store.has(SHARED_KEY)).toBe(true); // a 仍在引用
    expect(s3.store.has(OTHER_KEY)).toBe(false); // c 的对象被清理
  });

  it("删除不存在的练习是幂等的（重复任务直接成功）", async () => {
    const { s3, deps } = baseSetup();

    await expect(cleanupSession(deps, "session-gone")).resolves.toBeUndefined();
    expect(s3.calls).toEqual([]);
  });

  it("数据库事务失败时保留练习并标记 DELETE_FAILED，且不登记删除意图", async () => {
    const fakeWithFailure = createFakeDb(
      [{ id: "session-a", userId: USER, status: "DELETING" }],
      [{ id: "media-a", userId: USER, sessionId: "session-a", objectKey: SHARED_KEY }],
      { failSessionDelete: (id) => id === "session-a" },
    );
    const s3 = createFakeS3([SHARED_KEY]);
    const markDeleteFailed = vi.fn(async (_id: string) => {});
    const deps = { db: fakeWithFailure.db, deleteObject: s3.deleteObject, markDeleteFailed };

    await expect(cleanupSession(deps, "session-a")).rejects.toThrow("DB_UNAVAILABLE");

    expect(fakeWithFailure.state.sessions.has("session-a")).toBe(true); // 事务回滚
    expect(fakeWithFailure.state.media).toHaveLength(1);
    expect(fakeWithFailure.state.pending.size).toBe(0);
    expect(s3.store.has(SHARED_KEY)).toBe(true);
    expect(markDeleteFailed).toHaveBeenCalledWith("session-a");
  });
});

describe("待删除对象清理 - 可重试", () => {
  it("S3 删除失败时保留意图并指数退避，重试后成功，不留孤儿", async () => {
    let currentTime = 0;
    const now = () => new Date(currentTime);
    const { fake, deps } = baseSetup(now);

    let shouldFail = true;
    fake; // setup 中的 s3 不用，这里替换 deleteObject
    const failingS3 = createFakeS3([SHARED_KEY, OTHER_KEY], () => shouldFail);
    const failingDeps = { ...deps, deleteObject: failingS3.deleteObject };

    // 先删 session-c（OTHER_KEY），S3 调用失败
    await cleanupSession(failingDeps, "session-c");
    expect(failingS3.store.has(OTHER_KEY)).toBe(true); // 对象仍在
    expect(fake.state.pending.size).toBe(1);
    const pendingRow = [...fake.state.pending.values()][0]!;
    expect(pendingRow.objectKey).toBe(OTHER_KEY);
    expect(pendingRow.attempts).toBe(1);
    expect(pendingRow.nextAttemptAt.getTime()).toBe(backoffDelayMs(1));
    expect(pendingRow.lastError).toContain("S3_UNAVAILABLE");

    // 未到退避时间，扫描器不会处理
    currentTime = backoffDelayMs(1) - 1;
    await reapPendingDeletions({ ...failingDeps, now });
    expect(failingS3.store.has(OTHER_KEY)).toBe(true);
    expect(fake.state.pending.size).toBe(1);

    // 时间推进到到期，S3 恢复后重试成功
    shouldFail = false;
    currentTime = backoffDelayMs(1);
    await reapPendingDeletions({ ...failingDeps, now });
    expect(failingS3.store.has(OTHER_KEY)).toBe(false);
    expect(fake.state.pending.size).toBe(0);
  });

  it("重试删除前发现对象已被新练习复用时取消删除，绝不误删", async () => {
    let currentTime = 0;
    const now = () => new Date(currentTime);
    const { fake, deps } = baseSetup(now);

    let shouldFail = true;
    const failingS3 = createFakeS3([SHARED_KEY, OTHER_KEY], () => shouldFail);
    const failingDeps = { ...deps, deleteObject: failingS3.deleteObject };

    await cleanupSession(failingDeps, "session-c");
    expect(fake.state.pending.size).toBe(1);

    // 退避期间，用户在一个新练习里复用了同一个对象
    currentTime = backoffDelayMs(1);
    fake.addMedia({ userId: USER, sessionId: "session-new", objectKey: OTHER_KEY });

    shouldFail = false;
    await reapPendingDeletions({ ...failingDeps, now });

    expect(failingS3.store.has(OTHER_KEY)).toBe(true); // 对象保留
    expect(fake.state.pending.size).toBe(0); // 意图取消
  });

  it("清理流程重复执行结果一致（崩溃恢复后重放）", async () => {
    const { fake, s3, deps } = baseSetup();

    await cleanupSession(deps, "session-a"); // b 仍引用，不删
    await cleanupSession(deps, "session-b"); // 最后一个引用消失，删除对象
    await reapPendingDeletions(deps);
    await reapPendingDeletions(deps); // 重复扫描

    expect(s3.store.has(SHARED_KEY)).toBe(false);
    expect(s3.calls.filter((key) => key === SHARED_KEY)).toHaveLength(1);
    expect(fake.state.pending.size).toBe(0);
  });
});
