export interface DiscoveryRefreshDependencies {
  triggerNetworkDiscovery: () => void | Promise<void>;
  refreshSnapshot: () => void | Promise<void>;
  setRefreshing: (refreshing: boolean) => void;
}

export async function submitDiscoveryRefresh({
  triggerNetworkDiscovery,
  refreshSnapshot,
  setRefreshing,
}: DiscoveryRefreshDependencies): Promise<void> {
  setRefreshing(true);
  try {
    await triggerNetworkDiscovery();
    try {
      void Promise.resolve(refreshSnapshot()).catch((error) => {
        console.warn("Weline Localnet manual presence refresh failed", error);
      });
    } catch (error) {
      console.warn("Weline Localnet manual presence refresh failed", error);
    }
  } finally {
    setRefreshing(false);
  }
}
