export type TransferDirection = "incoming" | "outgoing";
export type TransferStatus = "awaitingAcceptance" | "transferring" | "paused" | "completed" | "cancelled" | "failed";

export interface TransferStatusInput {
  direction: TransferDirection;
  fileSize: number;
  transferredBytes: number;
  status: TransferStatus;
  error?: string;
}

export interface TransferStatusLabels {
  awaitingAcceptanceIncoming: string;
  awaitingAcceptanceOutgoing: string;
  transferring: (percent: number) => string;
  paused: string;
  completed: string;
  cancelled: string;
  failed: string;
}

export interface TransferStatusPresentation {
  label: string;
  tone: "neutral" | "active" | "paused" | "success" | "danger";
  percent: number;
  showProgress: boolean;
  showCancel: boolean;
}

export const DEFAULT_TRANSFER_STATUS_LABELS: TransferStatusLabels = {
  awaitingAcceptanceIncoming: "等待确认",
  awaitingAcceptanceOutgoing: "等待对方接收",
  transferring: (percent) => `传输中 ${percent}%`,
  paused: "网络中断，等待自动恢复",
  completed: "已完成",
  cancelled: "已取消",
  failed: "传输失败",
};

const DESTINATION_PREFLIGHT_PAUSE_MARKER = "[weline-localnet:destination-preflight:v1]";

export function transferStatusPresentation(
  transfer: TransferStatusInput,
  labels: TransferStatusLabels = DEFAULT_TRANSFER_STATUS_LABELS,
): TransferStatusPresentation {
  const percent = transferPercent(transfer);

  switch (transfer.status) {
    case "awaitingAcceptance":
      return {
        label: transfer.direction === "incoming" ? labels.awaitingAcceptanceIncoming : labels.awaitingAcceptanceOutgoing,
        tone: "neutral",
        percent,
        showProgress: false,
        showCancel: transfer.direction === "outgoing",
      };
    case "transferring":
      return { label: labels.transferring(percent), tone: "active", percent, showProgress: true, showCancel: false };
    case "paused":
      const destinationError = destinationPreflightPauseMessage(transfer);
      return {
        label: destinationError || labels.paused,
        tone: "paused",
        percent,
        showProgress: true,
        showCancel: true,
      };
    case "completed":
      return { label: labels.completed, tone: "success", percent: 100, showProgress: false, showCancel: false };
    case "cancelled":
      return { label: labels.cancelled, tone: "danger", percent, showProgress: false, showCancel: false };
    case "failed":
      return { label: labels.failed, tone: "danger", percent, showProgress: false, showCancel: false };
  }
}

function destinationPreflightPauseMessage(transfer: TransferStatusInput): string {
  if (transfer.direction !== "incoming" || !transfer.error?.startsWith(DESTINATION_PREFLIGHT_PAUSE_MARKER)) {
    return "";
  }
  const message = normalizePausedError(transfer.error.slice(DESTINATION_PREFLIGHT_PAUSE_MARKER.length));
  return message && !message.includes(DESTINATION_PREFLIGHT_PAUSE_MARKER) ? message : "";
}

function normalizePausedError(error: string | undefined): string {
  return error?.replace(/\s+/g, " ").trim() ?? "";
}

function transferPercent(transfer: TransferStatusInput): number {
  if (!Number.isFinite(transfer.fileSize) || transfer.fileSize <= 0) return 0;
  const transferredBytes = Number.isFinite(transfer.transferredBytes) ? Math.max(0, transfer.transferredBytes) : 0;
  return Math.min(100, Math.round((transferredBytes / transfer.fileSize) * 100));
}
