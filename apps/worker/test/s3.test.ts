import { describe, expect, it, vi } from "vitest";

process.env.DATABASE_URL = "postgres://localhost";
process.env.REDIS_URL = "redis://localhost";
process.env.S3_ENDPOINT = "http://localhost:9000";
process.env.S3_ACCESS_KEY = "a";
process.env.S3_SECRET_KEY = "b";

const { sendMock } = vi.hoisted(() => ({ sendMock: vi.fn() }));

vi.mock("@aws-sdk/client-s3", () => ({
  DeleteObjectCommand: class {
    constructor(public readonly input: unknown) {}
  },
  GetObjectCommand: class {},
  PutObjectCommand: class {},
  S3Client: class {
    send = sendMock;
  },
}));

const { deleteObjectIfExists } = await import("../src/lib/s3.js");

describe("deleteObjectIfExists", () => {
  it("resolves when object is deleted", async () => {
    sendMock.mockResolvedValue({ DeleteMarker: false });
    await expect(deleteObjectIfExists("users/u/k")).resolves.toBeUndefined();
  });

  it.each([
    [{ name: "NoSuchKey" }],
    [{ name: "NotFound", Code: "NotFound" }],
    [{ name: "SomeServiceError", $metadata: { httpStatusCode: 404 } }],
  ])("treats already-missing object %j as success for retry idempotency", async (error) => {
    sendMock.mockReset().mockRejectedValue(error);
    await expect(deleteObjectIfExists("users/u/k")).resolves.toBeUndefined();
  });

  it("rethrows non-404 errors so the tombstone stays retryable", async () => {
    sendMock.mockReset().mockRejectedValue(Object.assign(new Error("connection reset"), { name: "ECONNRESET" }));
    await expect(deleteObjectIfExists("users/u/k")).rejects.toThrow("connection reset");
  });
});
