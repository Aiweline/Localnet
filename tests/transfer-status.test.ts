import assert from "node:assert/strict";
import test from "node:test";

const statusModule = await import("../src/transfer-status.ts").catch(() => ({}));

function present(transfer: {
  direction: "incoming" | "outgoing";
  fileSize: number;
  transferredBytes: number;
  status: "awaitingAcceptance" | "transferring" | "paused" | "completed" | "cancelled" | "failed";
  error?: string;
}) {
  assert.equal(
    typeof statusModule.transferStatusPresentation,
    "function",
    "the production transfer status presentation must be available",
  );
  return statusModule.transferStatusPresentation(transfer);
}

test("shows the resumable network-waiting label with retained progress and Cancel", () => {
  const presentation = present({
    direction: "outgoing",
    fileSize: 1_000,
    transferredBytes: 425,
    status: "paused",
  });

  assert.deepEqual(presentation, {
    label: "网络中断，等待自动恢复",
    tone: "paused",
    percent: 43,
    showProgress: true,
    showCancel: true,
  });
});

test("keeps an actionable paused destination error while retaining its progress and Cancel", () => {
  const presentation = present({
    direction: "incoming",
    fileSize: 1_000,
    transferredBytes: 420,
    status: "paused",
    error: "目标磁盘可用空间不足，请释放空间后保持应用打开",
  });

  assert.equal(presentation.label, "目标磁盘可用空间不足，请释放空间后保持应用打开");
  assert.equal(presentation.tone, "paused");
  assert.equal(presentation.percent, 42);
  assert.equal(presentation.showProgress, true);
  assert.equal(presentation.showCancel, true);
});

test("keeps persisted network and unknown paused errors on the resumable waiting label", () => {
  for (const error of [
    "  network disconnected  ",
    "connection timed out while reading transfer body",
    "UnexpectedEof while receiving stream",
    "BrokenPipe: peer offline",
    "network permission denied",
    "opaque backend failure 472",
  ]) {
    const presentation = present({
      direction: "outgoing",
      fileSize: 1_000,
      transferredBytes: 420,
      status: "paused",
      error,
    });

    assert.equal(presentation.label, "网络中断，等待自动恢复", error);
  }
});

test("keeps only volume and destination paused errors actionable", () => {
  for (const error of [
    "接收目录位于 NTFS 文件系统，可用空间不足：请选择可用空间更多的目录后重试",
    "接收目录位于 FAT32 文件系统，单个文件最大支持 4294967295 字节",
    "无法访问接收目录 E:\\Downloads：请选择可访问且可写入的目录后重试",
    "无法写入接收目录 E:\\Downloads：请选择可写入的目录后重试",
    "接收目录或磁盘当前不可用",
    "Destination directory is unavailable; choose a writable folder and retry",
    "FAT32 volume does not support this file size",
    "insufficient free disk space on the destination volume",
    "disk is full",
    "filesystem permission denied",
    "read-only volume",
    "missing destination directory",
  ]) {
    const presentation = present({
      direction: "incoming",
      fileSize: 1_000,
      transferredBytes: 420,
      status: "paused",
      error,
    });

    assert.equal(presentation.label, error, error);
  }
});

test("handles a zero-byte paused record without an invalid progress value", () => {
  const presentation = present({
    direction: "incoming",
    fileSize: 0,
    transferredBytes: 0,
    status: "paused",
  });

  assert.equal(presentation.percent, 0);
  assert.equal(presentation.showProgress, true);
});

test("keeps active, awaiting, and terminal presentations unchanged", () => {
  assert.deepEqual(present({ direction: "outgoing", fileSize: 800, transferredBytes: 200, status: "transferring" }), {
    label: "传输中 25%", tone: "active", percent: 25, showProgress: true, showCancel: false,
  });
  assert.deepEqual(present({ direction: "outgoing", fileSize: 800, transferredBytes: 0, status: "awaitingAcceptance" }), {
    label: "等待对方接收", tone: "neutral", percent: 0, showProgress: false, showCancel: true,
  });
  assert.deepEqual(present({ direction: "incoming", fileSize: 800, transferredBytes: 800, status: "completed" }), {
    label: "已完成", tone: "success", percent: 100, showProgress: false, showCancel: false,
  });
  assert.deepEqual(present({ direction: "outgoing", fileSize: 800, transferredBytes: 200, status: "cancelled" }), {
    label: "已取消", tone: "danger", percent: 25, showProgress: false, showCancel: false,
  });
  assert.deepEqual(present({ direction: "outgoing", fileSize: 800, transferredBytes: 200, status: "failed" }), {
    label: "传输失败", tone: "danger", percent: 25, showProgress: false, showCancel: false,
  });
});
