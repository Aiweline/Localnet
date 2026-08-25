import assert from "node:assert/strict";
import {
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
  copyFileSync(join(repository, decisionScript), join(seed, decisionScript));
  writeFileSync(join(seed, "payload.txt"), "release commit\n");
  run("git", ["add", "scripts/release-decision.sh", "payload.txt"], seed);
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

function runReleaseMutationStep(fixture, releaseExists) {
  const fakeGh = `
gh() {
  if [[ "$1 $2" == "release view" ]]; then
    [[ "$FAKE_RELEASE_EXISTS" == "true" ]]
    return
  fi
  printf '%s\\n' "$*" >> "$MUTATION_LOG"
}
`;
  return spawnSync(bash, ["-c", `${fakeGh}\n${releaseMutationStep()}`], {
    cwd: fixture.checkout,
    encoding: "utf8",
    env: {
      ...process.env,
      FAKE_RELEASE_EXISTS: String(releaseExists),
      GITHUB_REPOSITORY: "example/localnet",
      GITHUB_SHA: fixture.expectedSha,
      MUTATION_LOG: "release-mutations.log",
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

    const result = runReleaseMutationStep(fixture, true);
    assert.notEqual(result.status, 0, result.stdout + result.stderr);
    assert.equal(existsSync(join(fixture.checkout, "release-mutations.log")), false);
  } finally {
    fixture.dispose();
  }
});

test("the publish step atomically creates a missing new tag before creating its release", () => {
  const fixture = remoteTagFixture("missing");
  try {
    const result = runReleaseMutationStep(fixture, false);
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

test("release workflow is valid YAML and wires provenance checks before decisions", () => {
  const workflow = readFileSync(new URL(`../${workflowPath}`, import.meta.url), "utf8");
  const refGuard = workflow.indexOf("Validate release source ref");
  const decision = workflow.indexOf("Decide whether this commit needs a release");
  assert.ok(refGuard >= 0, "workflow must have an early source-ref guard");
  assert.ok(decision > refGuard, "source-ref guard must run before the release decision");
  assert.match(workflow, /bash scripts\/release-decision\.sh/);

  const validators = [
    ["python", ["-c", "import pathlib, yaml; yaml.safe_load(pathlib.Path('.github/workflows/release.yml').read_text(encoding='utf-8'))"]],
    ["python3", ["-c", "import pathlib, yaml; yaml.safe_load(pathlib.Path('.github/workflows/release.yml').read_text(encoding='utf-8'))"]],
    ["ruby", ["-e", "require 'yaml'; YAML.safe_load_file('.github/workflows/release.yml', aliases: true)"]],
  ];
  const attempts = validators.map(([command, args]) =>
    spawnSync(command, args, { cwd: repository, encoding: "utf8" }),
  );
  assert.ok(
    attempts.some((attempt) => attempt.status === 0),
    attempts.map((attempt) => attempt.stderr).filter(Boolean).join("\n"),
  );
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
