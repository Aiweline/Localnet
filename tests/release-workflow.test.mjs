import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
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
