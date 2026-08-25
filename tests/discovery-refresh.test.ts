import assert from "node:assert/strict";
import test from "node:test";

import { submitDiscoveryRefresh } from "../src/discovery-refresh.ts";

test("manual discovery refresh releases its UI state without waiting for a snapshot", async () => {
  const states: boolean[] = [];
  let triggers = 0;
  let snapshotRefreshes = 0;

  await submitDiscoveryRefresh({
    triggerNetworkDiscovery: async () => { triggers += 1; },
    refreshSnapshot: async () => {
      snapshotRefreshes += 1;
      await new Promise<void>(() => undefined);
    },
    setRefreshing: (refreshing) => states.push(refreshing),
  });

  assert.equal(triggers, 1);
  assert.equal(snapshotRefreshes, 1);
  assert.deepEqual(states, [true, false]);
});

test("manual discovery refresh releases its UI state when command submission fails", async () => {
  const states: boolean[] = [];

  await assert.rejects(
    submitDiscoveryRefresh({
      triggerNetworkDiscovery: async () => { throw new Error("network queue unavailable"); },
      refreshSnapshot: async () => undefined,
      setRefreshing: (refreshing) => states.push(refreshing),
    }),
    /network queue unavailable/,
  );

  assert.deepEqual(states, [true, false]);
});
