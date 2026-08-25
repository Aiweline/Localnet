import assert from "node:assert/strict";
import test from "node:test";

import {
  checkForUpdate,
  compareStableVersions,
  createUpdateDownloadRequest,
  parseStableVersion,
  selectAvailableUpdate,
  type GithubRelease,
} from "../src/update.ts";

const VERSION = "0.2.3";
const WINDOWS_ASSET = `Weline_Localnet_${VERSION}_x64-setup.exe`;
const MACOS_ASSET = `Weline_Localnet_${VERSION}_universal.dmg`;
const SHA256 = `sha256:${"a".repeat(64)}`;

function release(overrides: Partial<GithubRelease> = {}): GithubRelease {
  return {
    tag_name: `v${VERSION}`,
    draft: false,
    prerelease: false,
    html_url: `https://github.com/Aiweline/Localnet/releases/tag/v${VERSION}`,
    body: "Verified release notes",
    assets: [
      {
        name: WINDOWS_ASSET,
        browser_download_url: `https://github.com/Aiweline/Localnet/releases/download/v${VERSION}/${WINDOWS_ASSET}`,
        state: "uploaded",
        digest: SHA256,
        size: 8_000_000,
      },
      {
        name: MACOS_ASSET,
        browser_download_url: `https://github.com/Aiweline/Localnet/releases/download/v${VERSION}/${MACOS_ASSET}`,
        state: "uploaded",
        digest: SHA256,
        size: 9_000_000,
      },
    ],
    ...overrides,
  };
}

test("parses only canonical stable semantic versions", () => {
  assert.deepEqual(parseStableVersion("v0.2.3"), [0, 2, 3]);
  assert.deepEqual(parseStableVersion("10.20.30"), [10, 20, 30]);

  for (const invalid of [
    "0.2",
    "0.2.3-beta.1",
    "0.2.3+build",
    "00.2.3",
    "v01.2.3",
    " 0.2.3 ",
    `1.2.${Number.MAX_SAFE_INTEGER + 1}`,
  ]) {
    assert.equal(parseStableVersion(invalid), null, invalid);
  }
});

test("compares stable versions numerically instead of lexically", () => {
  assert.equal(compareStableVersions("0.2.10", "0.2.3"), 1);
  assert.equal(compareStableVersions("1.0.0", "0.99.99"), 1);
  assert.equal(compareStableVersions("0.2.3", "v0.2.3"), 0);
  assert.equal(compareStableVersions("0.2.2", "0.2.3"), -1);
});

test("selects the exact uploaded platform asset for a newer stable release", () => {
  const windows = selectAvailableUpdate(release(), "0.2.2", "windows");
  const macos = selectAvailableUpdate(release(), "0.2.2", "macos");

  assert.equal(windows?.version, VERSION);
  assert.deepEqual(windows?.asset, {
    name: WINDOWS_ASSET,
    url: `https://github.com/Aiweline/Localnet/releases/download/v${VERSION}/${WINDOWS_ASSET}`,
    digest: SHA256,
    size: 8_000_000,
  });
  assert.equal(windows?.notes, "Verified release notes");
  assert.equal(macos?.asset.name, MACOS_ASSET);
});

test("does not offer current, older, draft, prerelease, or unknown-platform releases", () => {
  assert.equal(selectAvailableUpdate(release(), VERSION, "windows"), null);
  assert.equal(selectAvailableUpdate(release({ tag_name: "v0.2.2" }), VERSION, "windows"), null);
  assert.equal(selectAvailableUpdate(release({ draft: true }), "0.2.2", "windows"), null);
  assert.equal(selectAvailableUpdate(release({ prerelease: true }), "0.2.2", "windows"), null);
  assert.equal(selectAvailableUpdate(release(), "0.2.2", "unknown"), null);
});

test("rejects ambiguous or untrusted release assets", () => {
  const baseAsset = release().assets[0];
  const cases = [
    { ...baseAsset, name: "Weline_Localnet_0.2.3_x64-portable.zip" },
    { ...baseAsset, browser_download_url: `http://github.com/Aiweline/Localnet/releases/download/v${VERSION}/${WINDOWS_ASSET}` },
    { ...baseAsset, browser_download_url: `https://evil.example/Aiweline/Localnet/releases/download/v${VERSION}/${WINDOWS_ASSET}` },
    { ...baseAsset, browser_download_url: `https://github.com/Aiweline/Other/releases/download/v${VERSION}/${WINDOWS_ASSET}` },
    { ...baseAsset, browser_download_url: `https://github.com/Aiweline/Localnet/releases/download/v9.9.9/${WINDOWS_ASSET}` },
    { ...baseAsset, state: "new" },
    { ...baseAsset, digest: null },
    { ...baseAsset, digest: `sha256:${"A".repeat(64)}` },
    { ...baseAsset, size: 0 },
  ];

  for (const asset of cases) {
    assert.equal(selectAvailableUpdate(release({ assets: [asset] }), "0.2.2", "windows"), null, JSON.stringify(asset));
  }

  assert.equal(selectAvailableUpdate(release({ assets: [baseAsset, { ...baseAsset }] }), "0.2.2", "windows"), null);
});

test("checks only the fixed GitHub latest-release endpoint", async () => {
  const calls: Array<{ input: string; init: { headers?: Record<string, string> } | undefined }> = [];
  const fetchImpl = async (input: string, init?: { headers?: Record<string, string> }) => {
    calls.push({ input, init });
    return {
      ok: true,
      json: async () => release(),
    };
  };

  const update = await checkForUpdate("0.2.2", "windows", fetchImpl);

  assert.equal(update?.version, VERSION);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].input, "https://api.github.com/repos/Aiweline/Localnet/releases/latest");
  assert.match(calls[0].init?.headers?.Accept ?? "", /application\/vnd\.github\+json/);
});

test("fails closed when GitHub returns an unsuccessful response", async () => {
  await assert.rejects(
    checkForUpdate("0.2.2", "windows", async () => ({ ok: false, json: async () => release() })),
    /GitHub release check failed/,
  );
});

test("creates the exact verified-download command payload", () => {
  const update = selectAvailableUpdate(release(), "0.2.2", "windows");
  assert.ok(update);

  assert.deepEqual(createUpdateDownloadRequest(update), {
    version: VERSION,
    assetName: WINDOWS_ASSET,
    downloadUrl: `https://github.com/Aiweline/Localnet/releases/download/v${VERSION}/${WINDOWS_ASSET}`,
    sha256: "a".repeat(64),
    size: 8_000_000,
  });
});
