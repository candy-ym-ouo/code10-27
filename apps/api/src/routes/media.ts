import { randomUUID } from "node:crypto";
import path from "node:path";
import type { FastifyPluginAsync } from "fastify";
import { z } from "zod";
import { getConfig } from "../config/env.js";
import { AppError, notFound } from "../lib/errors.js";
import { prisma } from "../lib/prisma.js";
import { enqueueProbe } from "../lib/queue.js";
import { createPlaybackUrl, createUploadUrl, verifyObject } from "../lib/s3.js";
import { lockObjectKeys, sortedUniqueObjectKeys } from "../lib/object-locks.js";
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

    // 只复用仍属于“未在删除”的练习的音频：正在删除的练习可能已经登记
    // 对象删除意图，此时复用会把新练习关联到一个即将被回收的对象。
    const reusable = await prisma.mediaAsset.findFirst({
      where: {
        userId: request.authUser!.id,
        sha256: input.sha256.toLowerCase(),
        status: "READY",
        session: { status: { notIn: ["DELETING", "DELETE_FAILED"] } },
      },
      orderBy: { processedAt: "desc" },
    });
    if (reusable) {
      // 与 Worker 删除事务争夺同一把对象咨询锁并在锁内复查来源仍然有效，
      // 关闭“查找到可复用对象”与“创建引用”之间的并发删除窗口。
      const objectKeys = sortedUniqueObjectKeys([reusable.objectKey]);
      const media = await prisma.$transaction(async (tx) => {
        await lockObjectKeys(tx, objectKeys);
        const stillReusable = await tx.mediaAsset.findFirst({
          where: {
            id: reusable.id,
            status: "READY",
            session: { status: { notIn: ["DELETING", "DELETE_FAILED"] } },
          },
        });
        if (!stillReusable) return null;
        return tx.mediaAsset.create({
          data: {
            userId: request.authUser!.id,
            sessionId,
            status: "READY",
            objectKey: stillReusable.objectKey,
            originalName: input.originalName,
            mimeType: input.mimeType,
            sizeBytes: input.sizeBytes,
            sha256: stillReusable.sha256,
            durationMs: stillReusable.durationMs,
            codec: stillReusable.codec,
            sampleRate: stillReusable.sampleRate,
            channels: stillReusable.channels,
            peaks: stillReusable.peaks ?? undefined,
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
      if (media) {
        return reply.status(201).send({ media, reused: true, uploadUrl: null, requiredHeaders: {}, expiresAt: null });
      }
      // 来源恰好在并发删除，退化为普通直传流程，生成全新对象。
    }

    const mediaId = randomUUID();
    const objectKey = `users/${request.authUser!.id}/sessions/${sessionId}/${mediaId}/${safeFileName(input.originalName)}`;
    const uploadUrl = await createUploadUrl(objectKey, input.mimeType, input.sha256.toLowerCase());
    const media = await prisma.mediaAsset.create({
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
    // 与练习删除、复用新建共用同一把对象咨询锁：锁内统计引用、删除素材行，
    // 避免与并发删除互相误判引用计数。对象不在此处直接删除，而是写入待
    // 删除意图，由 Worker 事务外删除并在删除前再次确认引用，失败可重试，
    // 既不会误删被其他练习引用的对象，也不会遗留孤儿对象。
    await prisma.$transaction(async (tx) => {
      await lockObjectKeys(tx, sortedUniqueObjectKeys([media.objectKey]));
      const referenceCount = await tx.mediaAsset.count({
        where: { objectKey: media.objectKey, userId: request.authUser!.id },
      });
      await tx.mediaAsset.delete({ where: { id: media.id } });
      if (referenceCount === 1) {
        await tx.pendingObjectDeletion.upsert({
          where: { objectKey: media.objectKey },
          create: { userId: request.authUser!.id, objectKey: media.objectKey },
          update: {},
        });
      }
    });
    await audit(request, "MEDIA_DELETED", "MEDIA_ASSET", media.id, "SUCCESS");
    return { success: true };
  });
};

export default mediaRoutes;
