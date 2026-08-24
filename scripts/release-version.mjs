import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const packagePath = join(projectRoot, "package.json");
const tauriConfigPath = join(projectRoot, "src-tauri", "tauri.conf.json");
const cargoManifestPath = join(projectRoot, "src-tauri", "Cargo.toml");
const cargoLockPath = join(projectRoot, "src-tauri", "Cargo.lock");
const stableVersionPattern = /^\d+\.\d+\.\d+$/;

function readText(path) {
  return readFileSync(path, "utf8");
}

function cargoManifestVersion(content) {
  const match = content.match(/^version\s*=\s*"([^"]+)"/m);
  if (!match) throw new Error("src-tauri/Cargo.toml 缺少 package version");
  return match[1];
}

function cargoLockVersion(content) {
  const match = content.match(
    /\[\[package\]\]\r?\nname = "localnet"\r?\nversion = "([^"]+)"/,
  );
  if (!match) throw new Error("src-tauri/Cargo.lock 缺少 localnet package version");
  return match[1];
}

function readVersions() {
  const packageJson = JSON.parse(readText(packagePath));
  const tauriConfig = JSON.parse(readText(tauriConfigPath));
  const cargoManifest = readText(cargoManifestPath);
  const cargoLock = readText(cargoLockPath);

  return {
    packageJson: packageJson.version,
    tauriConfig: tauriConfig.version,
    cargoManifest: cargoManifestVersion(cargoManifest),
    cargoLock: cargoLockVersion(cargoLock),
  };
}

function assertStableVersion(version) {
  if (!stableVersionPattern.test(version)) {
    throw new Error(`版本号必须是稳定 SemVer，例如 0.1.6；收到：${version}`);
  }
}

function checkVersions() {
  const versions = readVersions();
  const uniqueVersions = new Set(Object.values(versions));

  for (const version of uniqueVersions) assertStableVersion(version);
  if (uniqueVersions.size !== 1) {
    throw new Error(`版本号不一致：${JSON.stringify(versions)}`);
  }

  const [version] = uniqueVersions;
  process.stdout.write(`Weline Localnet release version: ${version}\n`);
  return version;
}

function updateJsonVersion(path, version) {
  const content = readText(path);
  JSON.parse(content);
  const nextContent = replaceRequired(
    content,
    /^(\s*"version"\s*:\s*")[^"]+(".*)$/m,
    `$1${version}$2`,
    path,
  );
  writeFileSync(path, nextContent, "utf8");
}

function replaceRequired(content, pattern, replacement, label) {
  if (!pattern.test(content)) throw new Error(`${label} 中未找到可更新的版本号`);
  return content.replace(pattern, replacement);
}

function setVersion(version) {
  assertStableVersion(version);

  const cargoManifest = readText(cargoManifestPath);
  const cargoLock = readText(cargoLockPath);
  const nextCargoManifest = replaceRequired(
    cargoManifest,
    /^version\s*=\s*"[^"]+"/m,
    `version = "${version}"`,
    "src-tauri/Cargo.toml",
  );
  const nextCargoLock = replaceRequired(
    cargoLock,
    /(\[\[package\]\]\r?\nname = "localnet"\r?\nversion = ")[^"]+(")/,
    `$1${version}$2`,
    "src-tauri/Cargo.lock",
  );

  updateJsonVersion(packagePath, version);
  updateJsonVersion(tauriConfigPath, version);
  writeFileSync(cargoManifestPath, nextCargoManifest, "utf8");
  writeFileSync(cargoLockPath, nextCargoLock, "utf8");
  checkVersions();
}

const args = process.argv.slice(2);

try {
  if (args.length === 1 && args[0] === "--check") {
    checkVersions();
  } else if (args.length === 1) {
    setVersion(args[0]);
  } else {
    throw new Error(
      "用法：pnpm release:check 或 pnpm release:version <major.minor.patch>",
    );
  }
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
