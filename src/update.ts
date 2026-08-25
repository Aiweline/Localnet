export type ClientPlatform = "windows" | "macos" | "unknown";

export interface GithubReleaseAsset {
  name: string;
  browser_download_url: string;
  state: string;
  digest?: string | null;
  size: number;
}

export interface GithubRelease {
  tag_name: string;
  draft: boolean;
  prerelease: boolean;
  html_url: string;
  body?: string | null;
  assets: GithubReleaseAsset[];
}

export interface UpdateInfo {
  version: string;
  tag: string;
  notes: string;
  releaseUrl: string;
  asset: {
    name: string;
    url: string;
    digest: string;
    size: number;
  };
}

export interface UpdateDownloadRequest {
  version: string;
  assetName: string;
  downloadUrl: string;
  sha256: string;
  size: number;
}

interface GithubFetchResponse {
  ok: boolean;
  json(): Promise<unknown>;
}

export type GithubFetch = (
  input: string,
  init?: { headers?: Record<string, string>; signal?: AbortSignal },
) => Promise<GithubFetchResponse>;

const LATEST_RELEASE_URL = "https://api.github.com/repos/Aiweline/Localnet/releases/latest";
const UPDATE_CHECK_TIMEOUT_MS = 10_000;
const MAX_UPDATE_BYTES = 512 * 1024 * 1024;
const STABLE_VERSION = /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const SHA256_DIGEST = /^sha256:[0-9a-f]{64}$/;

export function parseStableVersion(value: string): [number, number, number] | null {
  const match = STABLE_VERSION.exec(value);
  if (!match) return null;
  const version = match.slice(1).map(Number);
  if (version.some((part) => !Number.isSafeInteger(part))) return null;
  return version as [number, number, number];
}

export function compareStableVersions(left: string, right: string): -1 | 0 | 1 {
  const leftVersion = parseStableVersion(left);
  const rightVersion = parseStableVersion(right);
  if (!leftVersion || !rightVersion) throw new Error("invalid stable semantic version");
  for (let index = 0; index < leftVersion.length; index += 1) {
    if (leftVersion[index] > rightVersion[index]) return 1;
    if (leftVersion[index] < rightVersion[index]) return -1;
  }
  return 0;
}

export function selectAvailableUpdate(
  release: GithubRelease,
  currentVersion: string,
  platform: ClientPlatform,
): UpdateInfo | null {
  if (!isGithubRelease(release) || release.draft || release.prerelease) return null;
  const parsedVersion = parseStableVersion(release.tag_name);
  if (!parsedVersion) return null;
  const version = parsedVersion.join(".");
  if (release.tag_name !== `v${version}` || !parseStableVersion(currentVersion)) return null;
  if (compareStableVersions(version, currentVersion) <= 0) return null;

  const assetName = platformAssetName(platform, version);
  if (!assetName) return null;
  const matchingAssets = release.assets.filter((asset) => asset.name === assetName);
  if (matchingAssets.length !== 1) return null;
  const asset = matchingAssets[0];
  if (
    asset.state !== "uploaded"
    || typeof asset.digest !== "string"
    || !SHA256_DIGEST.test(asset.digest)
    || !Number.isSafeInteger(asset.size)
    || asset.size <= 0
    || asset.size > MAX_UPDATE_BYTES
    || !isTrustedDownloadUrl(asset.browser_download_url, version, assetName)
  ) return null;

  const releaseUrl = `https://github.com/Aiweline/Localnet/releases/tag/v${version}`;
  if (release.html_url !== releaseUrl) return null;
  return {
    version,
    tag: `v${version}`,
    notes: typeof release.body === "string" ? release.body : "",
    releaseUrl,
    asset: {
      name: asset.name,
      url: asset.browser_download_url,
      digest: asset.digest,
      size: asset.size,
    },
  };
}

export async function checkForUpdate(
  currentVersion: string,
  platform: ClientPlatform,
  fetchImpl: GithubFetch = (input, init) => fetch(input, init),
): Promise<UpdateInfo | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), UPDATE_CHECK_TIMEOUT_MS);
  try {
    const response = await fetchImpl(LATEST_RELEASE_URL, {
      headers: {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error("GitHub release check failed");
    const payload = await response.json();
    if (!isGithubRelease(payload)) throw new Error("GitHub release response is invalid");
    return selectAvailableUpdate(payload, currentVersion, platform);
  } finally {
    clearTimeout(timeout);
  }
}

export function createUpdateDownloadRequest(update: UpdateInfo): UpdateDownloadRequest {
  if (!SHA256_DIGEST.test(update.asset.digest)) {
    throw new Error("invalid verified update digest");
  }
  return {
    version: update.version,
    assetName: update.asset.name,
    downloadUrl: update.asset.url,
    sha256: update.asset.digest.slice("sha256:".length),
    size: update.asset.size,
  };
}

function platformAssetName(platform: ClientPlatform, version: string): string | null {
  if (platform === "windows") return `Weline_Localnet_${version}_x64-setup.exe`;
  if (platform === "macos") return `Weline_Localnet_${version}_universal.dmg`;
  return null;
}

function isTrustedDownloadUrl(value: string, version: string, assetName: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === "https:"
      && url.hostname === "github.com"
      && url.port === ""
      && url.username === ""
      && url.password === ""
      && url.search === ""
      && url.hash === ""
      && url.pathname === `/Aiweline/Localnet/releases/download/v${version}/${assetName}`;
  } catch {
    return false;
  }
}

function isGithubRelease(value: unknown): value is GithubRelease {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<GithubRelease>;
  return typeof candidate.tag_name === "string"
    && typeof candidate.draft === "boolean"
    && typeof candidate.prerelease === "boolean"
    && typeof candidate.html_url === "string"
    && (candidate.body === undefined || candidate.body === null || typeof candidate.body === "string")
    && Array.isArray(candidate.assets)
    && candidate.assets.every(isGithubReleaseAsset);
}

function isGithubReleaseAsset(value: unknown): value is GithubReleaseAsset {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<GithubReleaseAsset>;
  return typeof candidate.name === "string"
    && typeof candidate.browser_download_url === "string"
    && typeof candidate.state === "string"
    && (candidate.digest === undefined || candidate.digest === null || typeof candidate.digest === "string")
    && typeof candidate.size === "number";
}
