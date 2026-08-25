const DEFAULT_RECONCILIATION_INTERVAL_MS = 3_000;

export type NearbyRelationship = "available" | "pending";

export function nearbyPeerEntries<
  TPeer extends { peerId: string; online: boolean },
  TFriend extends { peerId: string },
  TRequest extends { peerId: string; status: string },
>(
  peers: TPeer[],
  friends: TFriend[],
  requests: TRequest[],
  localPeerId: string,
): Array<{ peer: TPeer; relationship: NearbyRelationship }> {
  const friendIds = new Set(friends.map((friend) => friend.peerId));
  const acceptedPeerIds = new Set(
    requests
      .filter((request) => request.status === "accepted")
      .map((request) => request.peerId),
  );
  const pendingPeerIds = new Set(
    requests
      .filter((request) => request.status === "pending")
      .map((request) => request.peerId),
  );

  return peers.flatMap((peer) => {
    if (
      !peer.online
      || peer.peerId === localPeerId
      || friendIds.has(peer.peerId)
      || acceptedPeerIds.has(peer.peerId)
    ) {
      return [];
    }
    return [{
      peer,
      relationship: pendingPeerIds.has(peer.peerId) ? "pending" : "available",
    }];
  });
}

export function mergePresenceSnapshot<
  TSnapshot extends { peers: unknown[]; friends: unknown[] },
>(
  current: TSnapshot,
  presence: Pick<TSnapshot, "peers" | "friends">,
): TSnapshot {
  return {
    ...current,
    peers: presence.peers,
    friends: presence.friends,
  };
}

export function startSnapshotReconciliation(
  refresh: () => void | Promise<void>,
  intervalMs = DEFAULT_RECONCILIATION_INTERVAL_MS,
): () => void {
  let running = false;

  const reconcile = async () => {
    if (running) return;
    running = true;
    try {
      await refresh();
    } finally {
      running = false;
    }
  };

  const timer = globalThis.setInterval(() => {
    void reconcile().catch((error) => {
      console.warn("Weline Localnet presence reconciliation failed", error);
    });
  }, intervalMs);

  return () => globalThis.clearInterval(timer);
}
