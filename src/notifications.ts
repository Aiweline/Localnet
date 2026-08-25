export type NotificationPermissionState = "default" | "denied" | "granted";

export interface NotificationPromptStore {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface NotificationPermissionDependencies {
  isPermissionGranted: () => Promise<boolean>;
  requestPermission: () => Promise<NotificationPermissionState>;
  store: NotificationPromptStore;
}

export const NOTIFICATION_PROMPTED_KEY = "weline-localnet.notification-prompted.v1";

export async function initializeNotificationPermission({
  isPermissionGranted,
  requestPermission,
  store,
}: NotificationPermissionDependencies): Promise<"granted" | "denied"> {
  try {
    if (await isPermissionGranted()) return "granted";
  } catch {
    return "denied";
  }
  if (store.getItem(NOTIFICATION_PROMPTED_KEY) === "1") return "denied";
  store.setItem(NOTIFICATION_PROMPTED_KEY, "1");
  try {
    return normalizePermission(await requestPermission());
  } catch {
    return "denied";
  }
}

export async function requestNotificationPermission({
  isPermissionGranted,
  requestPermission,
  store,
}: NotificationPermissionDependencies): Promise<"granted" | "denied"> {
  store.setItem(NOTIFICATION_PROMPTED_KEY, "1");
  try {
    if (await isPermissionGranted()) return "granted";
    return normalizePermission(await requestPermission());
  } catch {
    return "denied";
  }
}

function normalizePermission(permission: NotificationPermissionState): "granted" | "denied" {
  return permission === "granted" ? "granted" : "denied";
}
