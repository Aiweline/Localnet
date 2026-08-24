const DEFAULT_RECONCILIATION_INTERVAL_MS = 3_000;

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
