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
