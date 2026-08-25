import assert from "node:assert/strict";
import test from "node:test";

import {
  initializeNotificationPermission,
  NOTIFICATION_PROMPTED_KEY,
  requestNotificationPermission,
  type NotificationPromptStore,
} from "../src/notifications.ts";

class MemoryStore implements NotificationPromptStore {
  readonly values = new Map<string, string>();

  getItem(key: string) {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string) {
    this.values.set(key, value);
  }
}

test("keeps an already granted notification permission without prompting", async () => {
  let requests = 0;
  const result = await initializeNotificationPermission({
    isPermissionGranted: async () => true,
    requestPermission: async () => { requests += 1; return "granted"; },
    store: new MemoryStore(),
  });

  assert.equal(result, "granted");
  assert.equal(requests, 0);
});

test("requests notification permission once on the first undecided startup", async () => {
  const store = new MemoryStore();
  const observations: string[] = [];
  const result = await initializeNotificationPermission({
    isPermissionGranted: async () => false,
    requestPermission: async () => {
      observations.push(store.getItem(NOTIFICATION_PROMPTED_KEY) ?? "missing");
      return "granted";
    },
    store,
  });

  assert.equal(result, "granted");
  assert.deepEqual(observations, ["1"], "the durable marker must be stored before the OS prompt");
});

test("does not repeat a denied startup prompt", async () => {
  const store = new MemoryStore();
  store.setItem(NOTIFICATION_PROMPTED_KEY, "1");
  let requests = 0;
  const result = await initializeNotificationPermission({
    isPermissionGranted: async () => false,
    requestPermission: async () => { requests += 1; return "granted"; },
    store,
  });

  assert.equal(result, "denied");
  assert.equal(requests, 0);
});

test("does not create an authorization storm when the OS prompt throws", async () => {
  const store = new MemoryStore();
  const result = await initializeNotificationPermission({
    isPermissionGranted: async () => false,
    requestPermission: async () => { throw new Error("permission API unavailable"); },
    store,
  });

  assert.equal(result, "denied");
  assert.equal(store.getItem(NOTIFICATION_PROMPTED_KEY), "1");
});

test("allows an explicit settings action to retry after denial", async () => {
  const store = new MemoryStore();
  store.setItem(NOTIFICATION_PROMPTED_KEY, "1");
  let requests = 0;
  const result = await requestNotificationPermission({
    isPermissionGranted: async () => false,
    requestPermission: async () => { requests += 1; return "granted"; },
    store,
  });

  assert.equal(result, "granted");
  assert.equal(requests, 1);
});

test("normalizes denied and default OS responses to denied", async () => {
  for (const response of ["denied", "default"] as const) {
    const result = await requestNotificationPermission({
      isPermissionGranted: async () => false,
      requestPermission: async () => response,
      store: new MemoryStore(),
    });
    assert.equal(result, "denied");
  }
});
