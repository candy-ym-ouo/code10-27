import { randomUUID } from "node:crypto";
import path from "node:path";
import type { FastifyPluginAsync } from "fastify";
import { z } from "zod";
import { getConfig } from "../config/env.js";
import { AppError, notFound } from "../lib/errors.js";
import { prisma } from "../lib/prisma.js";
import { enqueueObjectSweep, enqueueProbe } from "../lib/queue.js";
import { createPlaybackUrl, createUploadUrl, verifyObject } from "../lib/s3.js";
import { parseOrThrow } from "../lib/validation.js";
import { audit } from "../lib/audit.js";

const ALLOWED_MIME_TYPES = new Set([
  "audio/mpeg",
  "audio/mp4",
  "audio/m4a",
  "audio/x-m4a",
  "audio/aac",
  "audio/wav",
  "audio/x-wav",
  "audio/ogg",
  "audio/flac",
  "audio/webm",
]);
const uploadSessionSchema = z.object({
  originalName: z.string().trim().min(1).max(255),
  mimeType: z.string().trim().min(1).max(100),
  sizeBytes: z.coerce.bigint().positive(),
  sha256: z.string().regex(/^[a-fA-F0-9]{64}$/, "SHA-256 摘要格式不正确"),
});

function safeFileName(input: string): string {
  const base = path.basename(input).replace(/[^\p{L}\p{N}._-]+/gu, "_").slice(0, 180);
  return base || "audio";
}

const mediaRoutes: FastifyPluginAsync = async (app) => {
  app.addHook("preHandler", app.authenticate);

  app.post("/sessions/:sessionId/media/uploads", async (request, reply) => {
    const config = getConfig();
    const { sessionId } = request.params as { sessionId: string };
    const input = parseOrThrow(uploadSessionSchema, request.body);
    if (!ALLOWED_MIME_TYPES.has(input.mimeType.toLowerCase())) {
      throw new AppError(415, "UNSUPPORTED_MEDIA", "仅支持常见音频格式");
    }
    const maxBytes = BigInt(config.MAX_MEDIA_SIZE_MB) * 1024n * 1024n;
    if (input.sizeBytes > maxBytes) {
      throw new AppError(413, "FILE_TOO_LARGE", `单个音频不能超过 ${config.MAX_MEDIA_SIZE_MB} MB`);
    }

    const session = await prisma.practiceSession.findFirst({
      where: { id: sessionId, userId: request.authUser!.id },
      include: {
        _count: { select: { mediaAssets: true } },
        mediaAssets: { select: { sizeBytes: true } },
      },
    });
    if (!session) throw notFound();
    if (!["DRAFT", "IN_REVIEW"].includes(session.status)) {
      throw new AppError(409, "INVALID_SESSION_STATE", "当前练习状态不能继续上传音频");
    }
    if (session._count.mediaAssets >= config.MAX_MEDIA_PER_SESSION) {
      throw new AppError(400, "MEDIA_LIMIT_REACHED", `每个练习最多 ${config.MAX_MEDIA_PER_SESSION} 个音频`);
    }
    const total = session.mediaAssets.reduce((sum, item) => sum + item.sizeBytes, 0n);
    const maxTotal = BigInt(config.MAX_SESSION_TOTAL_MB) * 1024n * 1024n;
    if (total + input.sizeBytes > maxTotal) {
      throw new AppError(413, "SESSION_SIZE_LIMIT_REACHED", `单次练习音频总量不能超过 ${config.MAX_SESSION_TOTAL_MB} MB`);
    }

    const reusable = await prisma.mediaAsset.findFirst({
      where: { userId: request.authUser!.id, sha256: input.sha256.toLowerCase(), status: "READY" },
      orderBy: { processedAt: "desc" },
    });
    if (reusable) {
      // 与练习删除 / 单条删除共用 media_objects 行锁：事务开始即 FOR UPDATE，
      // 若该 key 正处于删除事务中，本事务等待其提交后再读到最终状态，
      // 消除"删除事务已墓碑化、复用事务却插入引用"的交错窗口。
      const media = await prisma.$transaction(async (tx) => {
        const locked = await tx.$queryRaw<Array<{ id: string; status: string }>>`
          SELECT "id", "status" FROM "media_objects"
          WHERE "object_key" = ${reusable.objectKey}
          FOR UPDATE`;
        const object = locked[0];
        if (!object) throw new AppError(503, "MEDIA_TEMPORARILY_UNAVAILABLE", "音频正在删除收尾，请稍后重试");
        if (object.status === "DELETE_PENDING") {
          // 对象在上一事务中被墓碑化但尚未物理删除，新引用到达则将其复活；
          // 清扫任务持有的也是行锁，必须等本事务提交后才能继续，
          // 届时会因引用数 > 0 跳过删除。
          await tx.mediaObject.update({
            where: { id: object.id },
            data: { status: "ACTIVE", deleteRequestedAt: null, deleteAttempts: 0, lastDeleteError: null },
          });
        }
        return tx.mediaAsset.create({
          data: {
            userId: request.authUser!.id,
            sessionId,
            status: "READY",
            objectKey: reusable.objectKey,
            originalName: input.originalName,
            mimeType: input.mimeType,
            sizeBytes: input.sizeBytes,
            sha256: reusable.sha256,
            durationMs: reusable.durationMs,
            codec: reusable.codec,
            sampleRate: reusable.sampleRate,
            channels: reusable.channels,
            peaks: reusable.peaks ?? undefined,
            uploadedAt: new Date(),
            processedAt: new Date(),
          },
          select: {
            id: true,
            status: true,
            originalName: true,
            mimeType: true,
            sizeBytes: true,
            durationMs: true,
            codec: true,
            sampleRate: true,
            channels: true,
            peaks: true,
            failureCode: true,
            failureMessage: true,
            createdAt: true,
          },
        });
      });
      return reply.status(201).send({ media, reused: true, uploadUrl: null, requiredHeaders: {}, expiresAt: null });
    }

    const mediaId = randomUUID();
    const objectKey = `users/${request.authUser!.id}/sessions/${sessionId}/${mediaId}/${safeFileName(input.originalName)}`;
    const uploadUrl = await createUploadUrl(objectKey, input.mimeType, input.sha256.toLowerCase());
    const media = await prisma.$transaction(async (tx) => {
      // 先登记物理对象，media_assets 通过外键引用它（NO ACTION 防止有引用时误删登记行）。
      await tx.mediaObject.create({
        data: {
          id: mediaId,
          userId: request.authUser!.id,
          objectKey,
          sha256: input.sha256.toLowerCase(),
        },
      });
      return tx.mediaAsset.create({
        data: {
          id: mediaId,
          userId: request.authUser!.id,
          sessionId,
          status: "PENDING_UPLOAD",
          objectKey,
          originalName: input.originalName,
          mimeType: input.mimeType,
          sizeBytes: input.sizeBytes,
          sha256: input.sha256.toLowerCase(),
          expiresAt: new Date(Date.now() + 24 * 60 * 60_000),
        },
        select: { id: true, status: true, originalName: true, sizeBytes: true, createdAt: true },
      });
    });

    return reply.status(201).send({
      media,
      reused: false,
      uploadUrl,
      requiredHeaders: {
        "Content-Type": input.mimeType,
        "x-amz-meta-sha256": input.sha256.toLowerCase(),
      },
      expiresAt: new Date(Date.now() + config.UPLOAD_URL_TTL_SECONDS * 1000),
    });
  });

  app.post("/media/:mediaId/complete-upload", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    if (media.status === "READY") return { media };
    if (!["PENDING_UPLOAD", "UPLOADING", "UPLOADED"].includes(media.status)) {
      throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频状态不能确认上传");
    }
    if (media.expiresAt && media.expiresAt < new Date()) {
      await prisma.mediaAsset.update({ where: { id: media.id }, data: { status: "FAILED", failureCode: "UPLOAD_SESSION_EXPIRED" } });
      throw new AppError(409, "UPLOAD_SESSION_EXPIRED", "上传会话已过期，请重新创建");
    }

    await verifyObject(media.objectKey, media.sizeBytes, media.sha256);
    const updated = await prisma.mediaAsset.update({
      where: { id: media.id },
      data: {
        status: "UPLOADED",
        uploadedAt: new Date(),
        expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60_000),
        failureCode: null,
        failureMessage: null,
      },
    });
    try {
      await enqueueProbe(media.id);
    } catch {
      throw new AppError(503, "PROCESSING_UNAVAILABLE", "文件已上传，但音频解析服务暂不可用，可稍后重试");
    }
    await audit(request, "MEDIA_UPLOADED", "MEDIA_ASSET", media.id, "SUCCESS");
    return { media: updated, probeQueued: true };
  });

  app.get("/media/:mediaId", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({
      where: { id: mediaId, userId: request.authUser!.id },
      select: {
        id: true,
        sessionId: true,
        status: true,
        originalName: true,
        mimeType: true,
        sizeBytes: true,
        sha256: true,
        durationMs: true,
        codec: true,
        sampleRate: true,
        channels: true,
        peaks: true,
        failureCode: true,
        failureMessage: true,
        uploadedAt: true,
        processedAt: true,
        createdAt: true,
        updatedAt: true,
      },
    });
    if (!media) throw notFound();
    return { media };
  });

  app.get("/media/:mediaId/playback-url", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    if (media.status !== "READY") throw new AppError(409, "MEDIA_NOT_READY", "音频尚未完成校验");
    const url = await createPlaybackUrl(media.objectKey, media.originalName, media.mimeType);
    return { url, expiresIn: getConfig().PLAYBACK_URL_TTL_SECONDS };
  });

  app.post("/media/:mediaId/retry-probe", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    if (!["FAILED", "UPLOADED"].includes(media.status)) {
      throw new AppError(409, "INVALID_MEDIA_STATE", "当前音频不需要重试解析");
    }
    await prisma.mediaAsset.update({
      where: { id: media.id },
      data: { status: "UPLOADED", failureCode: null, failureMessage: null },
    });
    await enqueueProbe(media.id);
    return { success: true, status: "UPLOADED" };
  });

  app.delete("/media/:mediaId", async (request) => {
    const { mediaId } = request.params as { mediaId: string };
    const media = await prisma.mediaAsset.findFirst({ where: { id: mediaId, userId: request.authUser!.id } });
    if (!media) throw notFound();
    // 与练习清理相同的两阶段协议：行锁 + 引用计数在同一事务内完成，
    // 修复原先"先 count 再删"在并发删除 / 并发复用上传下的竞态
    // （两个删除者都看到 refcount=1 → 都不删 S3 → 孤儿对象）。
    let tombstoned = false;
    await prisma.$transaction(async (tx) => {
      await tx.$executeRaw`SELECT "id" FROM "media_objects" WHERE "object_key" = ${media.objectKey} FOR UPDATE`;
      await tx.mediaAsset.delete({ where: { id: media.id } });
      const remaining = await tx.mediaAsset.count({ where: { objectKey: media.objectKey } });
      if (remaining === 0) {
        await tx.mediaObject.update({
          where: { objectKey: media.objectKey },
          data: { status: "DELETE_PENDING", deleteRequestedAt: new Date(), lastDeleteError: null },
        });
        tombstoned = true;
      }
    });
    if (tombstoned) {
      // 物理删除失败不影响接口结果：墓碑已持久化，清扫任务会持续重试。
      await enqueueObjectSweep().catch(() => undefined);
    }
    await audit(request, "MEDIA_DELETED", "MEDIA_ASSET", media.id, "SUCCESS");
    return { success: true };
  });
};

export default mediaRoutes;
