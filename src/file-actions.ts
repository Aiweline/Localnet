export interface AttachmentActionInput {
  kind: "text" | "image" | "file";
  status: "sending" | "delivered" | "failed";
  localPath?: string;
  fileName?: string;
}

export interface AttachmentActionState {
  available: boolean;
  defaultFileName: string;
}

export function attachmentActionState(message: AttachmentActionInput): AttachmentActionState {
  const available = (message.kind === "image" || message.kind === "file")
    && message.status === "delivered"
    && Boolean(message.localPath?.trim());
  return {
    available,
    defaultFileName: available && message.fileName?.trim() ? message.fileName : "attachment",
  };
}
