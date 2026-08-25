import { readFileSync } from "node:fs";

const auditedRuleset = Object.freeze({
  id: 21367373,
  name: "Protect immutable release tags",
  target: "tag",
  enforcement: "active",
  updatedAtUtc: "2026-08-25T03:07:17.637Z",
  include: ["refs/tags/v*"],
  exclude: [],
  ruleTypes: ["deletion", "update"],
});

function fail(message) {
  console.error(`::error::${message}`);
  process.exit(1);
}

function readJsonFromStdin() {
  const input = readFileSync(0, "utf8");
  try {
    return JSON.parse(input);
  } catch {
    fail("GitHub returned malformed JSON for the release policy gate.");
  }
}

function own(value, key) {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function sameArray(left, right) {
  return Array.isArray(left)
    && left.length === right.length
    && left.every((value, index) => value === right[index]);
}

function canonicalTimestamp(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T/.test(value)) return null;
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.valueOf()) ? null : timestamp.toISOString();
}

function validateRulesetList(pages) {
  if (!Array.isArray(pages)
    || !pages.every((page) => Array.isArray(page))
    || !pages.flat().every((entry) => entry && typeof entry === "object"
      && Number.isInteger(entry.id))) {
    fail("GitHub returned a malformed repository ruleset list.");
  }
  const matches = pages.flat().filter((entry) => entry.id === auditedRuleset.id);
  if (matches.length !== 1) {
    fail("The audited immutable release-tag ruleset is missing or duplicated.");
  }
}

function validateRulesetDetail(detail, repository) {
  const refName = detail?.conditions?.ref_name;
  const rules = detail?.rules;
  const ruleTypes = Array.isArray(rules)
    && rules.every((rule) => rule && typeof rule === "object" && typeof rule.type === "string")
    ? rules.map((rule) => rule.type).sort()
    : null;
  const fieldsMatch = detail && typeof detail === "object"
    && detail.id === auditedRuleset.id
    && detail.name === auditedRuleset.name
    && detail.target === auditedRuleset.target
    && detail.enforcement === auditedRuleset.enforcement
    && detail.source_type === "Repository"
    && typeof detail.source === "string"
    && detail.source.toLowerCase() === repository.toLowerCase()
    && sameArray(refName?.include, auditedRuleset.include)
    && sameArray(refName?.exclude, auditedRuleset.exclude)
    && sameArray(ruleTypes, auditedRuleset.ruleTypes)
    && canonicalTimestamp(detail.updated_at) === auditedRuleset.updatedAtUtc;
  if (!fieldsMatch) {
    fail("The immutable release-tag ruleset no longer matches the administrator-reviewed snapshot.");
  }

  if (own(detail, "bypass_actors")) {
    if (!Array.isArray(detail.bypass_actors) || detail.bypass_actors.length !== 0) {
      fail("The immutable release-tag ruleset exposes a bypass actor.");
    }
  } else {
    console.error(
      `::notice::The metadata-scoped workflow token cannot read bypass_actors. `
      + `No-bypass authority is anchored to audited ruleset ${auditedRuleset.id} `
      + `at ${auditedRuleset.updatedAtUtc}; any ruleset edit changes updated_at and fails this gate.`,
    );
  }
  if (own(detail, "current_user_can_bypass")) {
    if (detail.current_user_can_bypass !== "never") {
      fail("The workflow identity can bypass the immutable release-tag ruleset.");
    }
  } else {
    console.error(
      "::notice::current_user_can_bypass is hidden from the workflow token and is covered by the same administrator-reviewed snapshot.",
    );
  }
}

function findRelease(pages, tag) {
  if (!Array.isArray(pages)
    || !pages.every((page) => Array.isArray(page))
    || !pages.flat().every((entry) => entry && typeof entry === "object"
      && typeof entry.tag_name === "string")) {
    fail("GitHub returned a malformed release list.");
  }
  const matches = pages.flat().filter((entry) => entry.tag_name === tag);
  if (matches.length > 1) fail(`More than one GitHub release exists for ${tag}.`);
  if (matches.length === 1) process.stdout.write(JSON.stringify(matches[0]));
}

function expectedAssets(version) {
  return [
    `Weline_Localnet_${version}_SHA256SUMS.txt`,
    `Weline_Localnet_${version}_universal.dmg`,
    `Weline_Localnet_${version}_universal.dmg.sha256`,
    `Weline_Localnet_${version}_x64-portable.exe`,
    `Weline_Localnet_${version}_x64-setup.exe`,
  ].sort();
}

function validateReleaseMetadata(release, version, notesFile) {
  const expectedBody = readFileSync(notesFile, "utf8");
  return release && typeof release === "object"
    && Number.isInteger(release.id)
    && release.tag_name === `v${version}`
    && release.name === `Weline Localnet v${version}`
    && release.body === expectedBody
    && release.prerelease === false
    && Array.isArray(release.assets)
    && release.assets.every((asset) => asset && typeof asset === "object"
      && typeof asset.name === "string" && asset.state === "uploaded");
}

function validateRelease(release, mode, version, notesFile) {
  if (!validateReleaseMetadata(release, version, notesFile)) {
    fail("The GitHub release metadata or notes differ from the exact release contract.");
  }
  const names = release.assets.map((asset) => asset.name);
  if (new Set(names).size !== names.length) fail("The GitHub release has duplicate assets.");
  const expected = expectedAssets(version);
  if (mode === "draft-resumable") {
    if (release.draft !== true || release.immutable !== false
      || !names.every((name) => expected.includes(name))) {
      fail("The GitHub release is not a resumable owned draft.");
    }
    return;
  }
  if (!sameArray([...names].sort(), expected)) {
    fail("The GitHub release does not contain exactly the five expected assets.");
  }
  if (mode === "draft-complete") {
    if (release.draft !== true || release.immutable !== false) {
      fail("The GitHub release is not a complete mutable draft.");
    }
    return;
  }
  if (mode === "published") {
    if (release.draft !== false || release.immutable !== true) {
      fail("The published GitHub release is not immutable.");
    }
    return;
  }
  fail(`Unknown release validation mode: ${mode}.`);
}

const [command, ...args] = process.argv.slice(2);
switch (command) {
  case "audited-ruleset-id":
    process.stdout.write(String(auditedRuleset.id));
    break;
  case "validate-ruleset-list":
    validateRulesetList(readJsonFromStdin());
    break;
  case "validate-ruleset-detail":
    if (!args[0]) fail("GitHub repository is required for ruleset validation.");
    validateRulesetDetail(readJsonFromStdin(), args[0]);
    break;
  case "find-release":
    if (!args[0]) fail("Release tag is required for release discovery.");
    findRelease(readJsonFromStdin(), args[0]);
    break;
  case "release-id": {
    const release = readJsonFromStdin();
    if (!Number.isInteger(release?.id)) fail("Invalid GitHub release id.");
    process.stdout.write(String(release.id));
    break;
  }
  case "release-state": {
    const release = readJsonFromStdin();
    if (release?.draft === true) process.stdout.write("draft");
    else if (release?.draft === false) process.stdout.write("published");
    else fail("GitHub release draft state is missing or malformed.");
    break;
  }
  case "validate-release":
    if (args.length !== 3) fail("Release mode, version, and notes file are required.");
    validateRelease(readJsonFromStdin(), args[0], args[1], args[2]);
    break;
  default:
    fail(`Unknown release policy command: ${command ?? ""}.`);
}
