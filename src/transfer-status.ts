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
      const pausedError = normalizePausedError(transfer.error);
      return {
        label: isActionableDestinationError(pausedError) ? pausedError : labels.paused,
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

function isActionableDestinationError(error: string): boolean {
  if (!error) return false;

  const normalized = error.toLowerCase();
  if (/fat\s*32|\bmsdos\b/.test(normalized)) return true;

  const destination = /磁盘|磁碟|卷|文件系统|文件系統|接收目录|接收目錄|目标目录|目標目錄|保存目录|保存目錄|目的地目录|目的地目錄|目标位置|目標位置|接收位置|destination|target directory|receive directory|receiving directory|disk|drive|volume|filesystem|file system/i;
  const actionable = /可用空间|可用空間|空间不足|空間不足|可写|可寫|不可写|不可寫|不可用|无法访问|無法訪問|无法写入|無法寫入|无法识别|無法識別|只读|只讀|不可访问|不可訪問|not writable|unavailable|insufficient|free space|no space|read[- ]only|not found|cannot access|unable to access|cannot write|unable to write|unsupported|maximum file size|file size limit/i;
  return destination.test(error) && actionable.test(error);
}

function normalizePausedError(error: string | undefined): string {
  return error?.replace(/\s+/g, " ").trim() ?? "";
}

function transferPercent(transfer: TransferStatusInput): number {
  if (!Number.isFinite(transfer.fileSize) || transfer.fileSize <= 0) return 0;
  const transferredBytes = Number.isFinite(transfer.transferredBytes) ? Math.max(0, transfer.transferredBytes) : 0;
  return Math.min(100, Math.round((transferredBytes / transfer.fileSize) * 100));
}
