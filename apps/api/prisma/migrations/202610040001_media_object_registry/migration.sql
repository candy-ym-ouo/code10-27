-- 物理音频对象登记表：一个 object_key 一行，media_assets 仅作为引用。
-- 删除采用"先级联删引用、零引用时将登记行置为 DELETE_PENDING、再由清扫任务删除 S3"的
-- 可重试两阶段流程，避免两个练习复用同一对象时并发删除产生 S3 孤儿对象。

-- CreateEnum
CREATE TYPE "MediaObjectStatus" AS ENUM ('ACTIVE', 'DELETE_PENDING');

-- CreateTable
CREATE TABLE "media_objects" (
    "id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "object_key" TEXT NOT NULL,
    "sha256" CHAR(64) NOT NULL,
    "status" "MediaObjectStatus" NOT NULL DEFAULT 'ACTIVE',
    "delete_attempts" INTEGER NOT NULL DEFAULT 0,
    "last_delete_error" VARCHAR(500),
    "delete_requested_at" TIMESTAMPTZ(6),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "media_objects_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "media_objects_object_key_key" ON "media_objects"("object_key");

-- CreateIndex
CREATE INDEX "media_objects_user_id_status_idx" ON "media_objects"("user_id", "status");

-- CreateIndex
CREATE INDEX "media_objects_status_delete_requested_at_idx" ON "media_objects"("status", "delete_requested_at");

-- AddForeignKey
ALTER TABLE "media_objects" ADD CONSTRAINT "media_objects_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- 从现有引用回填登记表。同一 object_key 只登记一次；
-- 历史数据中 object_key 形如 users/<uid>/sessions/...，所有者取最早创建该引用的用户。
INSERT INTO "media_objects" ("id", "user_id", "object_key", "sha256", "status", "created_at", "updated_at")
SELECT
    gen_random_uuid(),
    (array_agg("user_id" ORDER BY "created_at" ASC))[1],
    "object_key",
    (array_agg("sha256" ORDER BY "created_at" ASC))[1],
    'ACTIVE',
    MIN("created_at"),
    CURRENT_TIMESTAMP
FROM "media_assets"
GROUP BY "object_key";

-- AddForeignKey
-- NO ACTION：只要语句结束时还有 media_assets 引用该 key，登记行（及其物理对象）就不能被删除。
-- 与 RESTRICT 的区别是检查推迟到级联全部完成之后，这样用户级联删除（同时级联两张表）不会被立即检查阻断；
-- 清扫路径先删完所有引用再删登记行，行为不变，仍能防止误删被引用的对象。
ALTER TABLE "media_assets" ADD CONSTRAINT "media_assets_object_key_fkey" FOREIGN KEY ("object_key") REFERENCES "media_objects"("object_key") ON DELETE NO ACTION ON UPDATE NO ACTION;
