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
  destinationDirectoryUnavailable: string;
  destinationPermissionDenied: string;
  destinationInsufficientSpace: string;
  destinationFilesystemLimit: string;
  destinationUnsupportedFilesystem: string;
  destinationFileTooLarge: string;
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
  destinationDirectoryUnavailable: "接收目录当前不可用，请恢复磁盘或重新选择目录后重试",
  destinationPermissionDenied: "没有权限写入接收目录，请重新选择可写入的目录",
  destinationInsufficientSpace: "接收目录可用空间不足，请释放空间后等待自动恢复",
  destinationFilesystemLimit: "接收目录的磁盘格式不支持这么大的文件，请选择支持大文件的目录",
  destinationUnsupportedFilesystem: "无法安全检查接收目录所在磁盘，请选择本地磁盘目录后重试",
  destinationFileTooLarge: "单个文件不能超过 100 GiB",
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
    case "paused": {
      const destinationError = destinationPreflightPauseMessage(transfer, labels);
      return {
        label: destinationError || labels.paused,
        tone: "paused",
        percent,
        showProgress: true,
        showCancel: true,
      };
    }
    case "completed":
      return { label: labels.completed, tone: "success", percent: 100, showProgress: false, showCancel: false };
    case "cancelled":
      return { label: labels.cancelled, tone: "danger", percent, showProgress: false, showCancel: false };
    case "failed":
      return { label: labels.failed, tone: "danger", percent, showProgress: false, showCancel: false };
  }
}

function destinationPreflightPauseMessage(transfer: TransferStatusInput, labels: TransferStatusLabels): string {
  if (transfer.direction !== "incoming" || !transfer.error?.startsWith(DESTINATION_PREFLIGHT_PAUSE_MARKER)) {
    return "";
  }
  switch (transfer.error.slice(DESTINATION_PREFLIGHT_PAUSE_MARKER.length)) {
    case "directory-unavailable":
      return labels.destinationDirectoryUnavailable;
    case "permission-denied":
      return labels.destinationPermissionDenied;
    case "insufficient-space":
      return labels.destinationInsufficientSpace;
    case "filesystem-limit":
      return labels.destinationFilesystemLimit;
    case "unsupported-filesystem":
      return labels.destinationUnsupportedFilesystem;
    case "file-too-large":
      return labels.destinationFileTooLarge;
    default:
      return "";
  }
}

function transferPercent(transfer: TransferStatusInput): number {
  if (!Number.isFinite(transfer.fileSize) || transfer.fileSize <= 0) return 0;
  const transferredBytes = Number.isFinite(transfer.transferredBytes) ? Math.max(0, transfer.transferredBytes) : 0;
  return Math.min(100, Math.round((transferredBytes / transfer.fileSize) * 100));
}
