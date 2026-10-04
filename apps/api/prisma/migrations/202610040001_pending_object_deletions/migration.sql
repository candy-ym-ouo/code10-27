-- PendingObjectDeletion：持久化 S3 对象删除意图，使共享音频的练习可以
-- 并发删除，并在 Worker 重试/崩溃后安全清理，避免遗留孤儿对象或误删。
CREATE TABLE "pending_object_deletions" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "user_id" UUID NOT NULL,
    "object_key" TEXT NOT NULL,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "last_error" VARCHAR(500),
    "next_attempt_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "pending_object_deletions_pkey" PRIMARY KEY ("id")
);

-- 同一对象只允许存在一条待删除记录
CREATE UNIQUE INDEX "pending_object_deletions_object_key_key"
    ON "pending_object_deletions"("object_key");

-- Worker 按到期时间拉取待重试记录
CREATE INDEX "pending_object_deletions_next_attempt_at_idx"
    ON "pending_object_deletions"("next_attempt_at");

ALTER TABLE "pending_object_deletions"
    ADD CONSTRAINT "pending_object_deletions_user_id_fkey"
    FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;
