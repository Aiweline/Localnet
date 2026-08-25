import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";

test("reconciles an online friend even when the presence event was missed", async () => {
  const presenceModule = await import("../src/presence.ts").catch(() => ({}));
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
