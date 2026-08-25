import assert from "node:assert/strict";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const repository = fileURLToPath(new URL("..", import.meta.url));
const workflowPath = ".github/workflows/release.yml";
const decisionScript = "scripts/release-decision.sh";
const policyScript = "scripts/release-policy.mjs";
const releaseVersion = "0.2.0";
const releaseTag = `v${releaseVersion}`;
const auditedRuleset = {
  id: 21367373,
  name: "Protect immutable release tags",
  target: "tag",
  enforcement: "active",
  updated_at: "2026-08-25T11:07:17.637+08:00",
  bypass_actors: [],
  current_user_can_bypass: "never",
  conditions: {
    ref_name: {
      include: ["refs/tags/v*"],
      exclude: [],
    },
  },
  rules: [{ type: "update" }, { type: "deletion" }],
};

function releaseAssetNames(version = releaseVersion) {
  return [
    `Weline_Localnet_${version}_SHA256SUMS.txt`,
    `Weline_Localnet_${version}_universal.dmg`,
    `Weline_Localnet_${version}_universal.dmg.sha256`,
    `Weline_Localnet_${version}_x64-portable.exe`,
    `Weline_Localnet_${version}_x64-setup.exe`,
  ];
}

function releaseJson({
  draft,
  immutable,
  body = "test release notes\n",
  assets = releaseAssetNames(),
}) {
  return {
    id: 101,
    tag_name: releaseTag,
    name: `Weline Localnet v${releaseVersion}`,
    body,
    draft,
    prerelease: false,
    immutable,
    assets: assets.map((name, index) => ({
      id: index + 1,
      name,
      state: "uploaded",
    })),
  };
}

function metadataVisibleAuditedRuleset() {
  const ruleset = structuredClone(auditedRuleset);
  delete ruleset.bypass_actors;
  delete ruleset.current_user_can_bypass;
  return ruleset;
}

function bashExecutable() {
  const candidates = [
    process.env.BASH_PATH,
    process.platform === "win32" ? "C:\\Program Files\\Git\\bin\\bash.exe" : undefined,
    "bash",
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (candidate.includes("\\") && !existsSync(candidate)) continue;
    const result = spawnSync(candidate, ["--version"], { encoding: "utf8" });
    if (result.status === 0) return candidate;
  }
  throw new Error("A Bash executable is required to validate release selection");
}

const bash = bashExecutable();

function runDecision(command) {
  return spawnSync(bash, ["-c", `source ${decisionScript}; ${command}`], {
    cwd: repository,
    encoding: "utf8",
  });
}

function run(command, args, cwd, env = {}) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    env: { ...process.env, ...env },
  });
  assert.equal(
    result.status,
    0,
    `${command} ${args.join(" ")} failed:\n${result.stdout}${result.stderr}`,
  );
  return result.stdout.trim();
}

function remoteTagFixture(tagKind) {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "localnet-release-tag-"));
  const origin = join(fixtureRoot, "origin.git");
  const seed = join(fixtureRoot, "seed");
  const checkout = join(fixtureRoot, "checkout");

  run("git", ["init", "--bare", origin], fixtureRoot);
  run("git", ["init", "-b", "main", seed], fixtureRoot);
  run("git", ["config", "user.name", "Release Test"], seed);
  run("git", ["config", "user.email", "release-test@example.invalid"], seed);
  mkdirSync(join(seed, "scripts"));
  mkdirSync(join(seed, "docs", "releases"), { recursive: true });
  copyFileSync(join(repository, decisionScript), join(seed, decisionScript));
  copyFileSync(join(repository, policyScript), join(seed, policyScript));
  copyFileSync(
    join(repository, "docs", "releases", "v0.2.0.md"),
    join(seed, "docs", "releases", "v0.2.0.md"),
  );
  writeFileSync(join(seed, "payload.txt"), "release commit\n");
  run(
    "git",
    [
      "add",
      "scripts/release-decision.sh",
      "scripts/release-policy.mjs",
      "docs/releases/v0.2.0.md",
      "payload.txt",
    ],
    seed,
  );
  run("git", ["commit", "-m", "release commit"], seed);
  run("git", ["remote", "add", "origin", origin], seed);
  run("git", ["push", "-u", "origin", "main"], seed);
  run("git", ["symbolic-ref", "HEAD", "refs/heads/main"], origin);
  if (tagKind === "annotated") {
    run("git", ["tag", "-a", "v0.2.0", "-m", "release v0.2.0"], seed);
  } else if (tagKind === "lightweight") {
    run("git", ["tag", "v0.2.0"], seed);
  }
  if (tagKind !== "missing") {
    run("git", ["push", "origin", "refs/tags/v0.2.0"], seed);
  }
  run("git", ["clone", origin, checkout], fixtureRoot);

  return {
    checkout,
    expectedSha: run("git", ["rev-parse", "HEAD"], checkout),
    origin,
    seed,
    dispose() {
      rmSync(fixtureRoot, { recursive: true, force: true });
    },
  };
}

function verifyRemoteTag(fixture, tag = "v0.2.0") {
  return spawnSync(
    bash,
    [
      "-c",
      "source scripts/release-decision.sh; verify_remote_release_tag origin \"$1\" \"$EXPECTED_SHA\"",
      "verify-remote-release-tag",
      tag,
    ],
    {
      cwd: fixture.checkout,
      encoding: "utf8",
      env: { ...process.env, EXPECTED_SHA: fixture.expectedSha },
    },
  );
}

function createRemoteTag(fixture) {
  return spawnSync(
    bash,
    [
      "-c",
      "source scripts/release-decision.sh; create_remote_release_tag origin v0.2.0 \"$EXPECTED_SHA\"",
    ],
    {
      cwd: fixture.checkout,
      encoding: "utf8",
      env: { ...process.env, EXPECTED_SHA: fixture.expectedSha },
    },
  );
}

function protectReleaseTags(fixture) {
  const hookPath = join(fixture.origin, "hooks", "update");
  writeFileSync(
    hookPath,
    `#!/bin/sh
set -eu
ref_name="$1"
old_object="$2"
zero=0000000000000000000000000000000000000000
case "$ref_name" in
  refs/tags/v*)
    if [ "$old_object" != "$zero" ]; then
      echo "release tags are immutable" >&2
      exit 1
    fi
    ;;
esac
exit 0
`,
  );
  chmodSync(hookPath, 0o755);
}

function advanceSeed(fixture, message = "different release commit") {
  writeFileSync(join(fixture.seed, "payload.txt"), `${message}\n`);
  run("git", ["add", "payload.txt"], fixture.seed);
  run("git", ["commit", "-m", message], fixture.seed);
}

function releaseMutationStep() {
  const workflow = readFileSync(new URL(`../${workflowPath}`, import.meta.url), "utf8");
  const lines = workflow.split(/\r?\n/);
  const nameIndex = lines.findIndex(
    (line) => line.trim() === "- name: Create or repair GitHub Release",
  );
  assert.ok(nameIndex >= 0, "release mutation step must exist");
  const runIndex = lines.findIndex(
    (line, index) => index > nameIndex && line.trim() === "run: |",
  );
  assert.ok(runIndex > nameIndex, "release mutation step must have a run block");
  const runIndent = lines[runIndex].search(/\S/);
  const contentIndent = runIndent + 2;
  const content = [];
  for (let index = runIndex + 1; index < lines.length; index += 1) {
    const indent = lines[index].search(/\S/);
    if (indent >= 0 && indent <= runIndent) break;
    content.push(lines[index].slice(Math.min(contentIndent, lines[index].length)));
  }
  return content
    .join("\n")
    .replaceAll("${{ needs.prepare.outputs.version }}", "0.2.0");
}

function runReleaseMutationStep(
  fixture,
  {
    initialRelease = "absent",
    ruleset = metadataVisibleAuditedRuleset(),
    rulesetList = undefined,
    uploadedAssetsComplete = true,
    publishImmutable = true,
    raceMissingProtection = false,
    secondPolicyMissing = false,
    draftListDelayReads = 0,
  } = {},
) {
  const fixtureFiles = {
    rulesetList: join(fixture.checkout, "fake-ruleset-list.json"),
    rulesetDetail: join(fixture.checkout, "fake-ruleset-detail.json"),
    releaseState: join(fixture.checkout, "fake-release-state.json"),
    draftEmpty: join(fixture.checkout, "fake-release-draft-empty.json"),
    draftUploaded: join(fixture.checkout, "fake-release-draft-uploaded.json"),
    published: join(fixture.checkout, "fake-release-published.json"),
    mutationLog: join(fixture.checkout, "release-mutations.log"),
    protectionCount: join(fixture.checkout, "fake-protection-count"),
    releaseVisibilityCount: join(fixture.checkout, "fake-release-visibility-count"),
    eventLog: join(fixture.checkout, "fake-release-events.log"),
  };
  const summaries = rulesetList ?? (ruleset ? [[], [{
    id: ruleset.id,
    name: ruleset.name,
    target: ruleset.target,
    enforcement: ruleset.enforcement,
    updated_at: ruleset.updated_at,
  }]] : [[], []]);
  const rulesetDetail = ruleset ? {
    source_type: "Repository",
    source: "example/localnet",
    ...ruleset,
  } : {};
  for (const stale of [
    fixtureFiles.releaseState,
    fixtureFiles.mutationLog,
    fixtureFiles.protectionCount,
    fixtureFiles.releaseVisibilityCount,
    fixtureFiles.eventLog,
  ]) {
    rmSync(stale, { force: true });
  }
  writeFileSync(fixtureFiles.rulesetList, JSON.stringify(summaries));
  writeFileSync(fixtureFiles.rulesetDetail, JSON.stringify(rulesetDetail));
  writeFileSync(
    fixtureFiles.draftEmpty,
    JSON.stringify(releaseJson({ draft: true, immutable: false, assets: [] })),
  );
  writeFileSync(
    fixtureFiles.draftUploaded,
    JSON.stringify(releaseJson({
      draft: true,
      immutable: false,
      assets: uploadedAssetsComplete ? releaseAssetNames() : releaseAssetNames().slice(0, 4),
    })),
  );
  writeFileSync(
    fixtureFiles.published,
    JSON.stringify(releaseJson({
      draft: false,
      immutable: publishImmutable,
      assets: uploadedAssetsComplete ? releaseAssetNames() : releaseAssetNames().slice(0, 4),
    })),
  );
  if (initialRelease !== "absent") {
    const initial = initialRelease === "draft"
      ? releaseJson({ draft: true, immutable: false, assets: [] })
      : initialRelease;
    writeFileSync(fixtureFiles.releaseState, JSON.stringify(initial));
  }
  writeFileSync(join(fixture.checkout, "release-notes.md"), "test release notes\n");
  const assetDirectory = join(fixture.checkout, "release-assets");
  mkdirSync(assetDirectory, { recursive: true });
  for (const asset of releaseAssetNames()) {
    writeFileSync(join(assetDirectory, asset), `${asset}\n`);
  }

  const fakeGh = `
git() {
  if [[ "$RACE_MISSING_PROTECTION" == "true" && "$1" == "ls-remote" ]]; then
    printf 'ls-remote-old-ref\\n' >> "$EVENT_LOG"
  fi
  command git "$@"
}

gh() {
  if [[ "$1" == "api" ]]; then
    local endpoint="\${!#}"
    case "$endpoint" in
      */rulesets\\?*)
        if [[ " $* " != *" --paginate "* || " $* " != *" --slurp "* ]]; then
          echo "ruleset list was not read with complete pagination" >&2
          return 1
        fi
        local count=0
        if [[ -f "$PROTECTION_COUNT" ]]; then
          count="$(cat "$PROTECTION_COUNT")"
        fi
        count=$((count + 1))
        printf '%s' "$count" > "$PROTECTION_COUNT"
        printf 'policy-read-%s\\n' "$count" >> "$EVENT_LOG"
        if [[ ("$RACE_MISSING_PROTECTION" == "true" || "$SECOND_POLICY_MISSING" == "true") && "$count" -ge 2 ]]; then
          if [[ "$RACE_MISSING_PROTECTION" == "true" ]]; then
            git -C "$FAKE_SEED" push --force origin HEAD:refs/tags/v0.2.0 >/dev/null 2>&1 || true
            printf 'remote-ref-moved\\n' >> "$EVENT_LOG"
          fi
          printf 'policy-missing\\n' >> "$EVENT_LOG"
          printf '[[]]'
        else
          cat "$RULESET_LIST"
        fi
        return
        ;;
      */rulesets/*)
        cat "$RULESET_DETAIL"
        return
        ;;
      */releases\\?*)
        if [[ " $* " != *" --paginate "* || " $* " != *" --slurp "* ]]; then
          echo "release list was not read with complete pagination" >&2
          return 1
        fi
        if [[ -f "$RELEASE_STATE" ]]; then
          local visibility_count=0
          if [[ -f "$RELEASE_VISIBILITY_COUNT" ]]; then
            visibility_count="$(cat "$RELEASE_VISIBILITY_COUNT")"
          fi
          if (( visibility_count < DRAFT_LIST_DELAY_READS )); then
            visibility_count=$((visibility_count + 1))
            printf '%s' "$visibility_count" > "$RELEASE_VISIBILITY_COUNT"
            printf '[[],[]]'
          else
            printf '[[],[%s]]' "$(cat "$RELEASE_STATE")"
          fi
        else
          printf '[[],[]]'
        fi
        return
        ;;
      */releases/tags/*|*/releases/[0-9]*)
        if [[ -f "$RELEASE_STATE" ]]; then
          cat "$RELEASE_STATE"
          return
        fi
        return 1
        ;;
      *)
        echo "unexpected gh api endpoint: $endpoint" >&2
        return 1
        ;;
    esac
  fi
  if [[ "$1 $2" == "release view" ]]; then
    [[ -f "$RELEASE_STATE" ]]
    return
  fi
  if [[ "$RACE_MISSING_PROTECTION" == "true" && ! -f "$PROTECTION_COUNT" ]]; then
    git -C "$FAKE_SEED" push --force origin HEAD:refs/tags/v0.2.0 >/dev/null 2>&1 || true
  fi
  printf '%s\\n' "$*" >> "$MUTATION_LOG"
  if [[ "$1 $2" == "release create" ]]; then
    cp "$DRAFT_EMPTY" "$RELEASE_STATE"
  elif [[ "$1 $2" == "release upload" ]]; then
    cp "$DRAFT_UPLOADED" "$RELEASE_STATE"
  elif [[ "$1 $2" == "release edit" && "$*" == *"--draft=false"* ]]; then
    cp "$PUBLISHED_RELEASE" "$RELEASE_STATE"
  fi
}
`;
  return spawnSync(bash, ["-c", `${fakeGh}\n${releaseMutationStep()}`], {
    cwd: fixture.checkout,
    encoding: "utf8",
    env: {
      ...process.env,
      DRAFT_EMPTY: fixtureFiles.draftEmpty,
      DRAFT_UPLOADED: fixtureFiles.draftUploaded,
      EVENT_LOG: fixtureFiles.eventLog,
      FAKE_SEED: fixture.seed,
      GITHUB_REPOSITORY: "example/localnet",
      GITHUB_SHA: fixture.expectedSha,
      MUTATION_LOG: fixtureFiles.mutationLog,
      PROTECTION_COUNT: fixtureFiles.protectionCount,
      PUBLISHED_RELEASE: fixtureFiles.published,
      RACE_MISSING_PROTECTION: String(raceMissingProtection),
      DRAFT_LIST_DELAY_READS: String(draftListDelayReads),
      RELEASE_STATE: fixtureFiles.releaseState,
      RELEASE_VISIBILITY_COUNT: fixtureFiles.releaseVisibilityCount,
      RULESET_DETAIL: fixtureFiles.rulesetDetail,
      RULESET_LIST: fixtureFiles.rulesetList,
      SECOND_POLICY_MISSING: String(secondPolicyMissing),
    },
  });
}

function runReleaseDecisionMain(fixture, release, { apiFailure = false } = {}) {
  const apiRelease = join(fixture.checkout, "fake-prepare-release.json");
  const legacyRelease = join(fixture.checkout, "fake-legacy-release.json");
  if (release) {
    writeFileSync(apiRelease, JSON.stringify(release));
    writeFileSync(legacyRelease, JSON.stringify({
      isDraft: release.draft,
      isPrerelease: release.prerelease,
      assets: release.assets,
    }));
  }
  const fakeCommands = `
gh() {
  if [[ "$1" == "api" ]]; then
    if [[ "$API_FAILURE" == "true" ]]; then
      return 1
    fi
    if [[ -f "$API_RELEASE" ]]; then
      printf '[[%s]]' "$(cat "$API_RELEASE")"
    else
      printf '[[]]'
    fi
    return
  fi
  if [[ "$1 $2" == "release view" ]]; then
    if [[ "$API_FAILURE" == "true" || ! -f "$LEGACY_RELEASE" ]]; then
      return 1
    fi
    cat "$LEGACY_RELEASE"
    return
  fi
  return 1
}
jq() {
  local filter="\${!#}"
  case "$filter" in
    '.assets[].name') node -e 'const j=JSON.parse(require("fs").readFileSync(0,"utf8")); for (const a of j.assets) console.log(a.name)' ;;
    '.isDraft') node -e 'const j=JSON.parse(require("fs").readFileSync(0,"utf8")); console.log(j.isDraft)' ;;
    '.isPrerelease') node -e 'const j=JSON.parse(require("fs").readFileSync(0,"utf8")); console.log(j.isPrerelease)' ;;
    *) return 1 ;;
  esac
}
source scripts/release-decision.sh
release_decision_main 0.2.0
`;
  return spawnSync(bash, ["-c", fakeCommands], {
    cwd: fixture.checkout,
    encoding: "utf8",
    env: {
      ...process.env,
      API_FAILURE: String(apiFailure),
      API_RELEASE: apiRelease,
      GITHUB_EVENT_NAME: "workflow_dispatch",
      GITHUB_REF: "refs/heads/main",
      GITHUB_REPOSITORY: "example/localnet",
      GITHUB_SHA: fixture.expectedSha,
      LEGACY_RELEASE: legacyRelease,
    },
  });
}

test("feature-branch workflow dispatch is rejected", () => {
  const result = runDecision("validate_release_ref workflow_dispatch refs/heads/feature");
  assert.notEqual(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout + result.stderr, /refs\/heads\/main/);
});

test("later main commit cannot repair an incomplete tag from an older SHA", () => {
  const result = runDecision(
    "validate_release_ref workflow_dispatch refs/heads/main && decide_release v0.2.0 true old-sha false false current-sha",
  );
  assert.notEqual(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout + result.stderr, /old-sha/);
});

test("an incomplete tag at the current SHA is repaired", () => {
  const result = runDecision(
    "validate_release_ref workflow_dispatch refs/heads/main && decide_release v0.2.0 true current-sha false false current-sha",
  );
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /should_release=true/);
});

test("a version without a tag starts a new release", () => {
  const result = runDecision(
    "decide_release v0.2.0 false '' false true current-sha",
  );
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /should_release=true/);
});

test("an immutable complete release with no app changes is a no-op", () => {
  const result = runDecision(
    "decide_release v0.2.0 true published-sha true false current-sha",
  );
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(result.stdout, /should_release=false/);
});

test("prepare rejects a published mutable release instead of skipping the authoritative job", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const body = readFileSync(join(fixture.checkout, "docs", "releases", "v0.2.0.md"), "utf8");
    const result = runReleaseDecisionMain(
      fixture,
      releaseJson({ draft: false, immutable: false, body }),
    );
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.doesNotMatch(result.stdout, /should_release=false/);
  } finally {
    fixture.dispose();
  }
});

test("prepare treats a GitHub release API failure as a gate failure", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const result = runReleaseDecisionMain(fixture, null, { apiFailure: true });
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.doesNotMatch(result.stdout, /should_release=true/);
  } finally {
    fixture.dispose();
  }
});

test("prepare skips only an exact published immutable release", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const body = readFileSync(join(fixture.checkout, "docs", "releases", "v0.2.0.md"), "utf8");
    const result = runReleaseDecisionMain(
      fixture,
      releaseJson({ draft: false, immutable: true, body }),
    );
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /should_release=false/);
  } finally {
    fixture.dispose();
  }
});

test("prepare rejects published immutable release notes drift", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const result = runReleaseDecisionMain(
      fixture,
      releaseJson({ draft: false, immutable: true, body: "wrong notes\n" }),
    );
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.doesNotMatch(result.stdout, /should_release=false/);
  } finally {
    fixture.dispose();
  }
});

test("an annotated release tag moved on the bare origin after checkout is rejected", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    writeFileSync(join(fixture.seed, "payload.txt"), "different commit\n");
    run("git", ["add", "payload.txt"], fixture.seed);
    run("git", ["commit", "-m", "different commit"], fixture.seed);
    run("git", ["tag", "-f", "-a", "v0.2.0", "-m", "moved release"], fixture.seed);
    run("git", ["push", "--force", "origin", "refs/tags/v0.2.0"], fixture.seed);

    assert.equal(
      run("git", ["rev-parse", "v0.2.0^{commit}"], fixture.checkout),
      fixture.expectedSha,
      "the checkout must retain the stale tag snapshot",
    );
    const result = verifyRemoteTag(fixture);
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.match(result.stdout + result.stderr, /remote tag v0\.2\.0.*does not point to/i);
  } finally {
    fixture.dispose();
  }
});

test("a lightweight release tag deleted from the bare origin after checkout is rejected", () => {
  const fixture = remoteTagFixture("lightweight");
  try {
    run("git", ["push", "origin", ":refs/tags/v0.2.0"], fixture.seed);

    assert.equal(
      run("git", ["rev-parse", "v0.2.0^{commit}"], fixture.checkout),
      fixture.expectedSha,
      "the checkout must retain the stale tag snapshot",
    );
    const result = verifyRemoteTag(fixture);
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.match(result.stdout + result.stderr, /remote tag v0\.2\.0.*missing/i);
  } finally {
    fixture.dispose();
  }
});

test("the remote tag guard rejects shell metacharacters as invalid tag input", () => {
  const fixture = remoteTagFixture("lightweight");
  try {
    const marker = join(fixture.checkout, "release-tag-injection");
    const result = verifyRemoteTag(
      fixture,
      "v0.2.0; touch release-tag-injection; #",
    );
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.match(result.stdout + result.stderr, /invalid stable release tag/i);
    assert.equal(existsSync(marker), false);
  } finally {
    fixture.dispose();
  }
});

test("a guard validation failure stops before external commands in an AND-list context", () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "localnet-release-validation-"));
  const commandLog = join(fixtureRoot, "external-commands.log");
  try {
    const result = spawnSync(
      bash,
      [
        "-c",
        `node() {
  printf 'node %s\\n' "$*" >> "$COMMAND_LOG"
  if [[ "$*" == *"audited-ruleset-id"* ]]; then
    printf '21367373\\n'
  fi
}
gh() {
  printf 'gh %s\\n' "$*" >> "$COMMAND_LOG"
  printf '[]\\n'
}
git() {
  printf 'git %s\\n' "$*" >> "$COMMAND_LOG"
  if [[ "$1" == "ls-remote" ]]; then
    printf '%s\\trefs/tags/v0.2.0\\n' "$EXPECTED_SHA"
  fi
}
source scripts/release-decision.sh
guard_release_mutation 'invalid repository' origin v0.2.0 "$EXPECTED_SHA" && printf 'MUTATION_REACHED\\n'`,
      ],
      {
        cwd: repository,
        encoding: "utf8",
        env: {
          ...process.env,
          COMMAND_LOG: commandLog,
          EXPECTED_SHA: "0123456789abcdef0123456789abcdef01234567",
        },
      },
    );

    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.equal(existsSync(commandLog), false, "validation must fail before node, gh, or git runs");
    assert.doesNotMatch(result.stdout, /MUTATION_REACHED/);
  } finally {
    rmSync(fixtureRoot, { recursive: true, force: true });
  }
});

for (const tagKind of ["annotated", "lightweight"]) {
  test(`an unchanged ${tagKind} remote release tag peels to the expected commit`, () => {
    const fixture = remoteTagFixture(tagKind);
    try {
      const result = verifyRemoteTag(fixture);
      assert.equal(result.status, 0, result.stdout + result.stderr);
    } finally {
      fixture.dispose();
    }
  });
}

test("a missing new release tag is created atomically at the expected commit", () => {
  const fixture = remoteTagFixture("missing");
  try {
    const result = createRemoteTag(fixture);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.equal(
      run(
        "git",
        ["--git-dir", fixture.origin, "rev-parse", "refs/tags/v0.2.0^{commit}"],
        fixture.checkout,
      ),
      fixture.expectedSha,
    );
  } finally {
    fixture.dispose();
  }
});

test("the publish step rejects remote tag drift before any GitHub release mutation", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    writeFileSync(join(fixture.seed, "payload.txt"), "different publish commit\n");
    run("git", ["add", "payload.txt"], fixture.seed);
    run("git", ["commit", "-m", "different publish commit"], fixture.seed);
    run("git", ["tag", "-f", "-a", "v0.2.0", "-m", "moved publish tag"], fixture.seed);
    run("git", ["push", "--force", "origin", "refs/tags/v0.2.0"], fixture.seed);

    const result = runReleaseMutationStep(fixture, {
      initialRelease: releaseJson({ draft: false, immutable: true }),
    });
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
  } finally {
    fixture.dispose();
  }
});

test("the publish step atomically creates a missing new tag before creating its release", () => {
  const fixture = remoteTagFixture("missing");
  try {
    const result = runReleaseMutationStep(fixture);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.equal(
      run(
        "git",
        ["--git-dir", fixture.origin, "rev-parse", "refs/tags/v0.2.0^{commit}"],
        fixture.checkout,
      ),
      fixture.expectedSha,
    );
    assert.match(
      readFileSync(join(fixture.checkout, "release-mutations.log"), "utf8"),
      /^release create v0\.2\.0 /m,
    );
  } finally {
    fixture.dispose();
  }
});

test("a newly created draft waits for release-list propagation before upload", () => {
  const fixture = remoteTagFixture("missing");
  try {
    const result = runReleaseMutationStep(fixture, { draftListDelayReads: 1 });
    assert.equal(result.status, 0, result.stdout + result.stderr);
    const mutations = readFileSync(
      join(fixture.checkout, "release-mutations.log"),
      "utf8",
    );
    assert.match(mutations, /^release create v0\.2\.0 /m);
    assert.match(mutations, /^release upload v0\.2\.0 /m);
    assert.match(mutations, /^release edit v0\.2\.0 .*--draft=false/m);
  } finally {
    fixture.dispose();
  }
});

test("a tag moved after the first remote read cannot reach a release mutation when protection disappears", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    advanceSeed(fixture, "race the release tag");
    const result = runReleaseMutationStep(fixture, { raceMissingProtection: true });
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
    const events = readFileSync(
      join(fixture.checkout, "fake-release-events.log"),
      "utf8",
    ).trim().split(/\r?\n/);
    const remoteRead = events.indexOf("ls-remote-old-ref");
    const refMove = events.indexOf("remote-ref-moved");
    const policyFailure = events.indexOf("policy-missing");
    assert.ok(remoteRead >= 0, events.join(" -> "));
    assert.ok(refMove > remoteRead, events.join(" -> "));
    assert.ok(policyFailure > refMove, events.join(" -> "));
    assert.equal(
      run(
        "git",
        ["--git-dir", fixture.origin, "rev-parse", "refs/tags/v0.2.0^{commit}"],
        fixture.checkout,
      ),
      run("git", ["rev-parse", "HEAD"], fixture.seed),
      "the fixture must move the unprotected server ref inside the verification/mutation window",
    );
  } finally {
    fixture.dispose();
  }
});

test("a new release tag is not pushed when the audited policy drifts immediately before creation", () => {
  const fixture = remoteTagFixture("missing");
  try {
    const result = runReleaseMutationStep(fixture, { secondPolicyMissing: true });
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    const remoteTag = spawnSync(
      "git",
      ["--git-dir", fixture.origin, "rev-parse", "--verify", "refs/tags/v0.2.0"],
      { cwd: fixture.checkout, encoding: "utf8" },
    );
    assert.notEqual(remoteTag.status, 0, remoteTag.stdout + remoteTag.stderr);
    assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
  } finally {
    fixture.dispose();
  }
});

test("the audited server rule blocks tag updates and deletion while draft upload then publish succeeds", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    advanceSeed(fixture, "attempt protected release tag changes");
    protectReleaseTags(fixture);
    const moved = spawnSync(
      "git",
      ["push", "--force", "origin", "HEAD:refs/tags/v0.2.0"],
      { cwd: fixture.seed, encoding: "utf8" },
    );
    assert.notEqual(moved.status, 0, moved.stdout + moved.stderr);
    const deleted = spawnSync(
      "git",
      ["push", "origin", ":refs/tags/v0.2.0"],
      { cwd: fixture.seed, encoding: "utf8" },
    );
    assert.notEqual(deleted.status, 0, deleted.stdout + deleted.stderr);

    const result = runReleaseMutationStep(fixture);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    const mutations = readFileSync(
      join(fixture.checkout, "release-mutations.log"),
      "utf8",
    ).trim().split(/\r?\n/);
    const createIndex = mutations.findIndex((line) => line.startsWith("release create "));
    const uploadIndex = mutations.findIndex((line) => line.startsWith("release upload "));
    const publishIndex = mutations.findIndex((line) => line.startsWith("release edit "));
    assert.ok(createIndex >= 0, mutations.join("\n"));
    assert.ok(uploadIndex > createIndex, mutations.join("\n"));
    assert.ok(publishIndex > uploadIndex, mutations.join("\n"));
    assert.match(mutations[createIndex], /--draft(?:\s|$)/);
    for (const asset of releaseAssetNames()) {
      assert.doesNotMatch(mutations[createIndex], new RegExp(asset.replaceAll(".", "\\.")));
      assert.match(mutations[uploadIndex], new RegExp(asset.replaceAll(".", "\\.")));
    }
    assert.match(mutations[publishIndex], /--draft=false(?:\s|$)/);
    assert.equal(
      run(
        "git",
        ["--git-dir", fixture.origin, "rev-parse", "refs/tags/v0.2.0^{commit}"],
        fixture.checkout,
      ),
      fixture.expectedSha,
    );
  } finally {
    fixture.dispose();
  }
});

test("ruleset snapshot drift fails closed before release mutation", () => {
  const fixture = remoteTagFixture("annotated");
  const cases = [
    ["missing ruleset", null],
    ["wrong id", { ...metadataVisibleAuditedRuleset(), id: auditedRuleset.id + 1 }],
    ["wrong name", { ...metadataVisibleAuditedRuleset(), name: "Almost the right policy" }],
    ["disabled", { ...metadataVisibleAuditedRuleset(), enforcement: "disabled" }],
    ["timestamp drift", { ...metadataVisibleAuditedRuleset(), updated_at: "2026-08-25T11:07:18.000+08:00" }],
    ["wrong target", { ...metadataVisibleAuditedRuleset(), target: "branch" }],
    ["wrong include", {
      ...metadataVisibleAuditedRuleset(),
      conditions: { ref_name: { include: ["refs/tags/release-*"], exclude: [] } },
    }],
    ["nonempty exclude", {
      ...metadataVisibleAuditedRuleset(),
      conditions: { ref_name: { include: ["refs/tags/v*"], exclude: ["refs/tags/v0.2.0"] } },
    }],
    ["missing update", { ...metadataVisibleAuditedRuleset(), rules: [{ type: "deletion" }] }],
    ["missing deletion", { ...metadataVisibleAuditedRuleset(), rules: [{ type: "update" }] }],
    ["bypass actor", {
      ...metadataVisibleAuditedRuleset(),
      bypass_actors: [{ actor_id: 1, actor_type: "RepositoryRole", bypass_mode: "always" }],
    }],
    ["workflow bypass", {
      ...metadataVisibleAuditedRuleset(),
      current_user_can_bypass: "always",
    }],
  ];
  try {
    for (const [name, ruleset] of cases) {
      const result = runReleaseMutationStep(fixture, { ruleset });
      assert.notEqual(result.status, 0, `${name}: ${result.stdout}${result.stderr}`);
      assert.equal(
        existsSync(join(fixture.checkout, "release-mutations.log")),
        false,
        `${name} reached a release mutation`,
      );
    }
  } finally {
    fixture.dispose();
  }
});

test("an incomplete uploaded draft is not published", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const result = runReleaseMutationStep(fixture, {
      initialRelease: "draft",
      uploadedAssetsComplete: false,
    });
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    const mutations = readFileSync(
      join(fixture.checkout, "release-mutations.log"),
      "utf8",
    );
    assert.match(mutations, /^release upload /m);
    assert.doesNotMatch(mutations, /^release edit /m);
  } finally {
    fixture.dispose();
  }
});

test("a valid partial draft resumes through exact upload and immutable publication", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const result = runReleaseMutationStep(fixture, {
      initialRelease: releaseJson({
        draft: true,
        immutable: false,
        assets: releaseAssetNames().slice(0, 2),
      }),
    });
    assert.equal(result.status, 0, result.stdout + result.stderr);
    const mutations = readFileSync(
      join(fixture.checkout, "release-mutations.log"),
      "utf8",
    );
    assert.match(mutations, /^release upload /m);
    assert.match(mutations, /^release edit .*--draft=false/m);
  } finally {
    fixture.dispose();
  }
});

test("post-publish mutable readback fails and a rerun performs no repair mutation", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const first = runReleaseMutationStep(fixture, { publishImmutable: false });
    assert.notEqual(first.status, 0, first.stdout + first.stderr);
    const firstMutations = readFileSync(
      join(fixture.checkout, "release-mutations.log"),
      "utf8",
    ).trim().split(/\r?\n/);
    assert.deepEqual(
      firstMutations.map((line) => line.split(" ").slice(0, 2).join(" ")),
      ["release create", "release upload", "release edit"],
    );
    const mutablePublished = JSON.parse(
      readFileSync(join(fixture.checkout, "fake-release-state.json"), "utf8"),
    );
    assert.equal(mutablePublished.immutable, false);

    const retry = runReleaseMutationStep(fixture, { initialRelease: mutablePublished });
    assert.notEqual(retry.status, 0, retry.stdout + retry.stderr);
    assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
  } finally {
    fixture.dispose();
  }
});

test("an exact published immutable release on later API pages is a mutation-free no-op", () => {
  const fixture = remoteTagFixture("annotated");
  try {
    const result = runReleaseMutationStep(fixture, {
      initialRelease: releaseJson({ draft: false, immutable: true }),
    });
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
  } finally {
    fixture.dispose();
  }
});

for (const [name, release] of [
  ["mutable published release", releaseJson({ draft: false, immutable: false })],
  ["published release with wrong notes", releaseJson({
    draft: false,
    immutable: true,
    body: "different notes\n",
  })],
]) {
  test(`${name} is rejected without repair mutation`, () => {
    const fixture = remoteTagFixture("annotated");
    try {
      const result = runReleaseMutationStep(fixture, { initialRelease: release });
      assert.notEqual(result.status, 0, result.stdout + result.stderr);
      assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
    } finally {
      fixture.dispose();
    }
  });
}

test("release workflow is valid YAML and wires provenance checks before decisions", () => {
  const workflow = readFileSync(new URL(`../${workflowPath}`, import.meta.url), "utf8");
  const refGuard = workflow.indexOf("Validate release source ref");
  const decision = workflow.indexOf("Decide whether this commit needs a release");
  assert.ok(refGuard >= 0, "workflow must have an early source-ref guard");
  assert.ok(decision > refGuard, "source-ref guard must run before the release decision");
  assert.match(workflow, /bash scripts\/release-decision\.sh/);

  const validators = [
    ["ruby", ["-e", "require 'json'; require 'yaml'; puts JSON.generate(YAML.safe_load_file('.github/workflows/release.yml', aliases: true))"]],
    ...(process.env.GITHUB_ACTIONS === "true" ? [] : [
      ["python", ["-c", "import json, pathlib, yaml; print(json.dumps(yaml.safe_load(pathlib.Path('.github/workflows/release.yml').read_text(encoding='utf-8'))))"]],
      ["python3", ["-c", "import json, pathlib, yaml; print(json.dumps(yaml.safe_load(pathlib.Path('.github/workflows/release.yml').read_text(encoding='utf-8'))))"]],
    ]),
  ];
  const attempts = validators.map(([command, args]) =>
    spawnSync(command, args, { cwd: repository, encoding: "utf8" }),
  );
  const successfulValidation = attempts.find((attempt) => attempt.status === 0);
  assert.ok(
    successfulValidation,
    attempts.map((attempt) => attempt.stderr).filter(Boolean).join("\n"),
  );
  const parsedWorkflow = JSON.parse(successfulValidation.stdout);
  const prepareSteps = parsedWorkflow.jobs.prepare.steps;
  const rubySetupIndex = prepareSteps.findIndex((step) => step.name === "Set up Ruby for release workflow validation");
  const releaseRegressionIndex = prepareSteps.findIndex((step) => step.name === "Run release workflow regression test");
  assert.ok(rubySetupIndex >= 0, "prepare must install the YAML validator's pinned Ruby runtime");
  assert.ok(
    releaseRegressionIndex > rubySetupIndex,
    "Ruby setup must complete before the release workflow regression test",
  );
  assert.equal(prepareSteps[rubySetupIndex].uses, "ruby/setup-ruby@v1");
  assert.equal(prepareSteps[rubySetupIndex].with["ruby-version"], "3.3");
});

test("release decision script and every inline Bash block pass Bash syntax validation", () => {
  const scriptResult = spawnSync(bash, ["-n", decisionScript], {
    cwd: repository,
    encoding: "utf8",
  });
  assert.equal(scriptResult.status, 0, scriptResult.stdout + scriptResult.stderr);

  const workflow = readFileSync(new URL(`../${workflowPath}`, import.meta.url), "utf8");
  const lines = workflow.split(/\r?\n/);
  const blocks = [];
  for (let index = 0; index < lines.length; index += 1) {
    if (lines[index].trim() !== "shell: bash") continue;
    const shellIndent = lines[index].search(/\S/);
    let runIndex = index + 1;
    while (runIndex < lines.length) {
      const indent = lines[runIndex].search(/\S/);
      if (indent >= 0 && indent <= shellIndent && lines[runIndex].trim().startsWith("- name:")) break;
      if (lines[runIndex].trim() === "run: |") break;
      runIndex += 1;
    }
    assert.equal(lines[runIndex]?.trim(), "run: |", `missing run block after Bash shell at line ${index + 1}`);
    const runIndent = lines[runIndex].search(/\S/);
    const contentIndent = runIndent + 2;
    const content = [];
    for (let lineIndex = runIndex + 1; lineIndex < lines.length; lineIndex += 1) {
      const indent = lines[lineIndex].search(/\S/);
      if (indent >= 0 && indent <= runIndent) break;
      content.push(lines[lineIndex].slice(Math.min(contentIndent, lines[lineIndex].length)));
    }
    blocks.push(content.join("\n").replace(/\$\{\{[\s\S]*?\}\}/g, "GH_EXPR"));
  }
  assert.ok(blocks.length >= 5, "expected all release Bash blocks to be discovered");
  for (const [index, block] of blocks.entries()) {
    const result = spawnSync(bash, ["-n"], { input: block, encoding: "utf8" });
    assert.equal(result.status, 0, `Bash block ${index + 1}: ${result.stdout}${result.stderr}`);
  }
});
