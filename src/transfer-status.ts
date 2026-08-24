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
  const nonDestinationContext = /\b(?:source|sender|sending|peer|remote|network|connection)\b|源文件|源目录|源目錄|源磁盘|源磁碟|源驱动器|源驅動器|源卷|源文件系统|源文件系統|来源|來源|发送方|發送方|发送端|發送端|对端|對端|对方|對方|网络|網絡|網路|连接|連接/i;
  if (nonDestinationContext.test(normalized)) return false;

  const explicitDestinationObject = /\b(?:(?:destination|target)(?:\s+(?:disk|drive|volume|filesystem|file system|directory|folder))?|receive folder)\b|目标|目標|目的地|保存目录|保存目錄|接收目录|接收目錄/i;
  const filesystemLimit = /fat\s*32|\bmsdos\b/.test(normalized)
    && /limit|size|file|maximum|too large|限制|过大|過大|大小|最大|文件/.test(normalized);
  const namedFilesystemObject = /^(?:(?:fat\s*32|msdos)\s+(?:disk|drive|volume|filesystem|file system)|(?:disk|drive|volume|filesystem|file system)\s+(?:fat\s*32|msdos))\b|^(?:fat\s*32|msdos)\s*文件系统|^(?:fat\s*32|msdos)\s*文件系統/i;
  if (filesystemLimit && (explicitDestinationObject.test(normalized) || namedFilesystemObject.test(normalized))) {
    return true;
  }

  const actionableCondition = /\b(?:full|no space|insufficient|free space|permission denied|access denied|read[- ]only|not writable|unavailable|not found|missing|not ready|disconnected device|limit|too large|cannot access|unable to access|cannot write|unable to write|unsupported|maximum file size|file size limit)\b|空间不足|空間不足|已满|已滿|不可写|不可寫|权限|權限|只读|只讀|不可用|未找到|未就绪|未就緒|限制|过大|過大|无法访问|無法訪問|无法写入|無法寫入|访问失败|訪問失敗|写入失败|寫入失敗|无法识别|無法識別|不可访问|不可訪問/i;
  if (explicitDestinationObject.test(normalized) && actionableCondition.test(normalized)) return true;

  const genericStorageFailure = /^(?:(?:disk|volume)\s+(?:is full|has no space)|(?:filesystem|file system)\s+(?:permission denied|access denied)|read[- ]only\s+volume|(?:写入失败|寫入失敗)[：:\s]*(?:磁盘|磁碟)(?:空间不足|空間不足|已满|已滿)|(?:磁盘|磁碟)(?:空间不足|空間不足|已满|已滿)|(?:文件系统|文件系統)(?:权限不足|權限不足|权限被拒绝|權限被拒絕|访问被拒绝|訪問被拒絕)|只读(?:磁盘|磁碟|卷)|只讀(?:磁盘|磁碟|卷))[.!。！]?$/i;
  return genericStorageFailure.test(normalized);
}

function normalizePausedError(error: string | undefined): string {
  return error?.replace(/\s+/g, " ").trim() ?? "";
}

function transferPercent(transfer: TransferStatusInput): number {
  if (!Number.isFinite(transfer.fileSize) || transfer.fileSize <= 0) return 0;
  const transferredBytes = Number.isFinite(transfer.transferredBytes) ? Math.max(0, transfer.transferredBytes) : 0;
  return Math.min(100, Math.round((transferredBytes / transfer.fileSize) * 100));
}
