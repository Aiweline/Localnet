import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";

test("reconciles an online friend even when the presence event was missed", async () => {
  const presenceModule = await import("../src/presence.ts");
  assert.equal(
    typeof presenceModule.startSnapshotReconciliation,
    "function",
    "presence reconciliation must be available",
  );
  assert.equal(
    typeof presenceModule.mergePresenceSnapshot,
    "function",
    "the production presence snapshot merge must be testable",
  );

  const messages = [{ messageId: "message-1", body: "still here" }];
  const transfers = [{ transferId: "transfer-1", status: "completed" }];
  let visibleSnapshot = {
    peers: [{ peerId: "mac-peer", online: false }],
    friends: [{ peerId: "mac-peer", nickname: "Mac", online: false }],
    messages,
    transfers,
  };
  let backendPresence = {
    peers: [{ peerId: "mac-peer", online: false }],
    friends: [{ peerId: "mac-peer", nickname: "Mac", online: false }],
  };
  let refreshCount = 0;
  const stop = presenceModule.startSnapshotReconciliation(async () => {
    refreshCount += 1;
    visibleSnapshot = presenceModule.mergePresenceSnapshot(visibleSnapshot, backendPresence);
  }, 10);

  backendPresence = {
    peers: [{ peerId: "mac-peer", online: true }],
    friends: [{ peerId: "mac-peer", nickname: "Mac", online: true }],
  };
  try {
    await delay(45);
  } finally {
    stop();
  }

  assert.ok(refreshCount > 0, "the authoritative snapshot should be refreshed");
  assert.equal(visibleSnapshot.friends[0].online, true);
  assert.equal(visibleSnapshot.friends[0].peerId, "mac-peer");
  assert.strictEqual(visibleSnapshot.messages, messages);
  assert.strictEqual(visibleSnapshot.transfers, transfers);
});

test("nearby relationships allow future protocol peers and never re-offer known friends", async () => {
  const { nearbyPeerEntries } = await import("../src/presence.ts");
  const peers = [
    { peerId: "future-peer", nickname: "Future", online: true, protocolVersion: 2 },
    { peerId: "friend-peer", nickname: "Friend", online: true, protocolVersion: 1 },
    { peerId: "accepted-peer", nickname: "Accepted", online: true, protocolVersion: 1 },
    { peerId: "pending-peer", nickname: "Pending", online: true, protocolVersion: 1 },
    { peerId: "self-peer", nickname: "Self", online: true, protocolVersion: 1 },
  ];
  const entries = nearbyPeerEntries(
    peers,
    [{ peerId: "friend-peer" }],
    [
      { peerId: "accepted-peer", direction: "outgoing", status: "accepted" },
      { peerId: "pending-peer", direction: "incoming", status: "pending" },
    ],
    "self-peer",
  );

  assert.deepEqual(
    entries.map(({ peer, relationship }) => [peer.peerId, relationship]),
    [
      ["future-peer", "available"],
      ["pending-peer", "pending"],
    ],
  );
});

test("a resolved friendship immediately removes the peer from the add-friend list", async () => {
  const { mergeResolvedFriendSnapshot, nearbyPeerEntries } = await import("../src/presence.ts");
  const messages = [{ messageId: "message-1" }];
  const current = {
    peers: [{ peerId: "mac-peer", nickname: "Mac", online: true }],
    friends: [],
    friendRequests: [{
      requestId: "request-1",
      peerId: "mac-peer",
      status: "pending",
    }],
    messages,
  };
  const resolvedRequest = {
    requestId: "request-1",
    peerId: "mac-peer",
    status: "accepted",
  };
  const friend = {
    peerId: "mac-peer",
    nickname: "Mac",
    online: true,
  };

  const next = mergeResolvedFriendSnapshot(current, resolvedRequest, friend);

  assert.deepEqual(next.friends, [friend]);
  assert.deepEqual(next.friendRequests, [resolvedRequest]);
  assert.strictEqual(next.messages, messages);
  assert.deepEqual(
    nearbyPeerEntries(next.peers, next.friends, next.friendRequests, "win-peer"),
    [],
  );
});

test("friend identity stays bound to PeerId when either device changes its nickname", async () => {
  const { nearbyPeerEntries } = await import("../src/presence.ts");
  const peers = [
    { peerId: "stable-device-id", nickname: "Mac Renamed Again", online: true },
    { peerId: "different-device-id", nickname: "Mac", online: true },
    { peerId: "third-device-id", nickname: "Office PC", online: true },
  ];
  const friends = [
    { peerId: "stable-device-id", nickname: "Old Mac Name", online: false },
  ];

  const entries = nearbyPeerEntries(peers, friends, [], "local-device-id");

  assert.deepEqual(
    entries.map(({ peer }) => peer.peerId),
    ["different-device-id", "third-device-id"],
    "only the stable PeerId may suppress an already-added device",
  );
});

test("a peer discovery event updates the matching friend's nickname and online state by PeerId", async () => {
  const { mergePeerDiscoverySnapshot } = await import("../src/presence.ts");
  const current: {
    peers: Array<{ peerId: string; nickname: string; online: boolean }>;
    friends: Array<{ peerId: string; nickname: string; online: boolean }>;
    messages: Array<{ messageId: string }>;
  } = {
    peers: [{ peerId: "mac-peer", nickname: "Old Mac", online: false }],
    friends: [{ peerId: "mac-peer", nickname: "Old Mac", online: false }],
    messages: [{ messageId: "keep-me" }],
  };

  const next = mergePeerDiscoverySnapshot(current, {
    peerId: "mac-peer",
    nickname: "New Mac",
    online: true,
  });

  assert.deepEqual(next.peers, [{ peerId: "mac-peer", nickname: "New Mac", online: true }]);
  assert.deepEqual(next.friends, [{ peerId: "mac-peer", nickname: "New Mac", online: true }]);
  assert.strictEqual(next.messages, current.messages);
});

test("discovering one friend never hides other addable devices", async () => {
  const { nearbyPeerEntries } = await import("../src/presence.ts");
  const peers = [
    { peerId: "friend-device", nickname: "Mac", online: true },
    { peerId: "nearby-one", nickname: "Design PC", online: true },
    { peerId: "nearby-two", nickname: "Meeting Room", online: true },
  ];

  const entries = nearbyPeerEntries(
    peers,
    [{ peerId: "friend-device", nickname: "Mac Before Rename", online: true }],
    [],
    "local-device-id",
  );

  assert.deepEqual(entries.map(({ peer }) => peer.peerId), ["nearby-one", "nearby-two"]);
});

test("a stalled snapshot cannot occupy every future presence refresh", async () => {
  const { startSnapshotReconciliation } = await import("../src/presence.ts");
  let calls = 0;
  const stop = startSnapshotReconciliation(() => {
    calls += 1;
    if (calls === 1) return new Promise<void>(() => undefined);
  }, 5, 10);

  try {
    await delay(45);
  } finally {
    stop();
  }

  assert.ok(calls >= 2, `expected the refresh lock to recover after timeout, received ${calls} call(s)`);
});
