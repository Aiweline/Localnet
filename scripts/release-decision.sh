validate_release_ref() {
  local event_name="${1:?event name is required}"
  local source_ref="${2:?source ref is required}"

  if [[ "$event_name" == "workflow_dispatch" && "$source_ref" != "refs/heads/main" ]]; then
    echo "::error::workflow_dispatch releases are allowed only from refs/heads/main (received $source_ref)."
    return 1
  fi
}

readonly RELEASE_POLICY_API_VERSION="2026-03-10"

validate_github_repository_name() {
  local repository="${1:?GitHub repository is required}"
  if [[ ! "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "::error::Invalid GitHub repository name for release policy verification."
    return 1
  fi
}

github_api_json() {
  gh api \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: $RELEASE_POLICY_API_VERSION" \
    -H "Time-Zone: UTC" \
    "$@"
}

verify_audited_release_tag_policy() {
  local repository="${1:?GitHub repository is required}"
  validate_github_repository_name "$repository" || return 1
  local ruleset_id
  ruleset_id="$(node scripts/release-policy.mjs audited-ruleset-id)"

  local pages
  if ! pages="$(
    github_api_json --paginate --slurp \
      "repos/$repository/rulesets?per_page=100&includes_parents=false&targets=tag"
  )"; then
    echo "::error::Could not read the complete repository tag-ruleset list."
    return 1
  fi
  if ! node scripts/release-policy.mjs validate-ruleset-list <<< "$pages"; then
    return 1
  fi

  local detail
  if ! detail="$(
    github_api_json \
      "repos/$repository/rulesets/$ruleset_id?includes_parents=false"
  )"; then
    echo "::error::Could not read the audited immutable release-tag ruleset detail."
    return 1
  fi
  node scripts/release-policy.mjs validate-ruleset-detail "$repository" <<< "$detail"
}

find_release_by_tag() {
  local repository="${1:?GitHub repository is required}"
  local tag="${2:?release tag is required}"
  validate_github_repository_name "$repository" || return 1
  local pages
  if ! pages="$(
    github_api_json --paginate --slurp "repos/$repository/releases?per_page=100"
  )"; then
    echo "::error::Could not read the complete GitHub release list." >&2
    return 1
  fi
  node scripts/release-policy.mjs find-release "$tag" <<< "$pages"
}

wait_for_release_by_tag() {
  local repository="${1:?GitHub repository is required}"
  local tag="${2:?release tag is required}"
  local release_json="" attempt
  for ((attempt = 1; attempt <= 10; attempt++)); do
    if ! release_json="$(find_release_by_tag "$repository" "$tag")"; then
      return 1
    fi
    if [[ -n "$release_json" ]]; then
      printf '%s' "$release_json"
      return 0
    fi
    if ((attempt < 10)); then
      sleep 2
    fi
  done
  echo "::error::The newly created release draft did not become readable after bounded retries." >&2
  return 1
}

read_release_by_id() {
  local repository="${1:?GitHub repository is required}"
  local release_id="${2:?release id is required}"
  validate_github_repository_name "$repository" || return 1
  if [[ ! "$release_id" =~ ^[0-9]+$ ]]; then
    echo "::error::Invalid GitHub release id." >&2
    return 1
  fi
  github_api_json "repos/$repository/releases/$release_id"
}

validate_release_common() {
  local release_json="${1:?release JSON is required}"
  local version="${2:?release version is required}"
  local notes_file="${3:?release notes file is required}"
  node scripts/release-policy.mjs validate-release draft-complete "$version" "$notes_file" \
    <<< "$release_json"
}

validate_published_immutable_release() {
  local release_json="${1:?release JSON is required}"
  local version="${2:?release version is required}"
  local notes_file="${3:?release notes file is required}"
  node scripts/release-policy.mjs validate-release published "$version" "$notes_file" \
    <<< "$release_json"
}

validate_resumable_draft_release() {
  local release_json="${1:?release JSON is required}"
  local version="${2:?release version is required}"
  local notes_file="${3:?release notes file is required}"
  node scripts/release-policy.mjs validate-release draft-resumable "$version" "$notes_file" \
    <<< "$release_json"
}

guard_release_mutation() {
  local repository="${1:?GitHub repository is required}"
  local remote_name="${2:?remote name is required}"
  local tag="${3:?release tag is required}"
  local expected_sha="${4:?expected SHA is required}"
  verify_audited_release_tag_policy "$repository" &&
    verify_remote_release_tag "$remote_name" "$tag" "$expected_sha"
}

write_release_notes() {
  local version="${1:?release version is required}"
  local commit_ref="${2:?release commit is required}"
  local output="${3:?release notes output is required}"
  if ! git cat-file -e "$commit_ref^{commit}" 2>/dev/null; then
    echo "::error::Release notes commit is not available: $commit_ref."
    return 1
  fi
  if git ls-files --error-unmatch "docs/releases/v${version}.md" >/dev/null 2>&1; then
    cp "docs/releases/v${version}.md" "$output"
    return
  fi

  local previous_tag changes
  previous_tag="$(git tag --sort=-v:refname | grep -Fxv "v${version}" | head -n 1 || true)"
  if [[ -n "$previous_tag" ]]; then
    changes="$(git log --format='- %s' "${previous_tag}..${commit_ref}")"
  else
    changes="$(git log --format='- %s' -n 20 "$commit_ref")"
  fi
  cat > "$output" <<EOF
## Weline Localnet ${version}

Windows 与 macOS 局域网私密聊天及文件直传工具。本版本由通过发布门禁的 main 提交自动构建。

### 变更

${changes}

### 下载

- Windows x64 安装版与便携版
- macOS Universal DMG（Apple Silicon + Intel）
- SHA-256 完整性校验文件

---

Weline Localnet is a private peer-to-peer LAN messenger and file-transfer app for Windows and macOS. This release was built automatically from a gated main-branch commit.

Company: 成都阿玛云科技有限公司
Contact: contact@amayum.com
EOF
}

validate_remote_release_tag_arguments() {
  local remote_name="${1:?remote name is required}"
  local tag="${2:?tag is required}"
  local expected_sha="${3:?expected SHA is required}"

  if [[ ! "$remote_name" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ ]]; then
    echo "::error::Invalid Git remote name for release provenance verification."
    return 1
  fi
  if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    ! git check-ref-format "refs/tags/$tag" >/dev/null 2>&1; then
    echo "::error::Invalid stable release tag: $tag."
    return 1
  fi
  if [[ ! "$expected_sha" =~ ^[0-9A-Fa-f]{40}$ && ! "$expected_sha" =~ ^[0-9A-Fa-f]{64}$ ]]; then
    echo "::error::Invalid expected commit SHA for $tag."
    return 1
  fi
}

verify_remote_release_tag() {
  local remote_name="${1:?remote name is required}"
  local tag="${2:?tag is required}"
  local expected_sha="${3:?expected SHA is required}"
  validate_remote_release_tag_arguments "$remote_name" "$tag" "$expected_sha" || return 1

  local remote_refs
  if ! remote_refs="$(
    git ls-remote --exit-code -- "$remote_name" \
      "refs/tags/$tag" "refs/tags/$tag^{}"
  )"; then
    echo "::error::Remote tag $tag is missing or could not be read from $remote_name."
    return 1
  fi

  local direct_object=""
  local peeled_object=""
  local object_id ref_name
  while IFS=$'\t' read -r object_id ref_name; do
    case "$ref_name" in
      "refs/tags/$tag") direct_object="$object_id" ;;
      "refs/tags/$tag^{}") peeled_object="$object_id" ;;
    esac
  done <<< "$remote_refs"

  local remote_commit="${peeled_object:-$direct_object}"
  if [[ -z "$remote_commit" ]]; then
    echo "::error::Remote tag $tag is missing from $remote_name."
    return 1
  fi
  if [[ "${remote_commit,,}" != "${expected_sha,,}" ]]; then
    echo "::error::Remote tag $tag does not point to expected commit $expected_sha; current peeled object is $remote_commit."
    return 1
  fi
}

create_remote_release_tag() {
  local remote_name="${1:?remote name is required}"
  local tag="${2:?tag is required}"
  local expected_sha="${3:?expected SHA is required}"
  validate_remote_release_tag_arguments "$remote_name" "$tag" "$expected_sha" || return 1

  if ! git cat-file -e "$expected_sha^{commit}" 2>/dev/null; then
    echo "::error::Expected release commit $expected_sha is not available locally."
    return 1
  fi
  if ! git push --porcelain "$remote_name" "$expected_sha:refs/tags/$tag"; then
    echo "::error::Could not create remote tag $tag atomically at $expected_sha; the ref may have appeared concurrently."
    return 1
  fi
  verify_remote_release_tag "$remote_name" "$tag" "$expected_sha"
}

emit_release_output() {
  local should_release="${1:?release decision is required}"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "should_release=$should_release" >> "$GITHUB_OUTPUT"
  fi
  echo "should_release=$should_release"
}

decide_release() {
  local tag="${1:?tag is required}"
  local tag_exists="${2:?tag existence is required}"
  local tag_commit="${3:-}"
  local release_complete="${4:?release completeness is required}"
  local app_changed="${5:?application change state is required}"
  local github_sha="${6:?GitHub SHA is required}"

  if [[ "$tag_exists" == "true" ]]; then
    if [[ "$release_complete" == "false" && "$tag_commit" != "$github_sha" ]]; then
      echo "::error::Release $tag is incomplete, but its tag points to $tag_commit instead of $github_sha. Refusing to rebuild or clobber assets from a different commit."
      return 1
    elif [[ "$release_complete" == "false" ]]; then
      echo "Release $tag is missing or incomplete at the current commit; it will be rebuilt."
      emit_release_output true
    elif [[ "$app_changed" == "true" ]]; then
      echo "::error::Application files changed, but $tag is already published. Run pnpm release:version with a higher version before merging."
      return 1
    else
      echo "Release $tag is already complete and no application files changed."
      emit_release_output false
    fi
  else
    echo "No tag exists for $tag; release build is required."
    emit_release_output true
  fi
}

release_decision_main() {
  set -euo pipefail
  local version="${1:?version is required}"
  local tag="v$version"
  local app_changed=false
  local tag_exists=false
  local tag_commit=""
  local release_complete=false

  validate_release_ref "$GITHUB_EVENT_NAME" "$GITHUB_REF" || return 1

  if [[ "$GITHUB_EVENT_NAME" == "push" ]]; then
    local before="${RELEASE_EVENT_BEFORE:-}"
    local changed_files
    if [[ -z "$before" || "$before" =~ ^0+$ ]] || ! git cat-file -e "$before^{commit}" 2>/dev/null; then
      changed_files="$(git diff-tree --no-commit-id --name-only -r "$GITHUB_SHA")"
    else
      changed_files="$(git diff --name-only "$before" "$GITHUB_SHA")"
    fi

    echo "Changed files:"
    printf '%s\n' "$changed_files"
    while IFS= read -r file; do
      if [[ "$file" =~ ^(\.cargo/|assets/|src/|src-tauri/|index\.html$|package\.json$|pnpm-lock\.yaml$|tsconfig(\..+)?\.json$|vite\.config\.ts$) ]]; then
        app_changed=true
        break
      fi
    done <<< "$changed_files"
  fi

  if git rev-parse --verify --quiet "$tag^{commit}" >/dev/null; then
    tag_exists=true
    tag_commit="$(git rev-list -n 1 "$tag")"
  fi

  local release_json
  if ! release_json="$(find_release_by_tag "$GITHUB_REPOSITORY" "$tag")"; then
    return 1
  fi
  if [[ "$tag_exists" == "false" && -n "$release_json" ]]; then
    echo "::error::A GitHub release already exists for $tag, but the checked-out tag is missing."
    return 1
  fi
  if [[ -n "$release_json" ]]; then
    local notes_file generated_notes="" release_state
    notes_file="docs/releases/v${version}.md"
    if ! git ls-files --error-unmatch "$notes_file" >/dev/null 2>&1; then
      generated_notes="$(mktemp)"
      notes_file="$generated_notes"
      write_release_notes "$version" "${tag_commit:-$GITHUB_SHA}" "$notes_file"
    fi
    release_state="$(node scripts/release-policy.mjs release-state <<< "$release_json")"
    if [[ "$release_state" == "published" ]]; then
      if ! validate_published_immutable_release "$release_json" "$version" "$notes_file"; then
        [[ -n "$generated_notes" ]] && rm -f "$generated_notes"
        echo "::error::Published release $tag is mutable or differs from the exact immutable release contract; it cannot be repaired."
        return 1
      fi
      release_complete=true
    elif ! validate_resumable_draft_release "$release_json" "$version" "$notes_file"; then
      [[ -n "$generated_notes" ]] && rm -f "$generated_notes"
      echo "::error::Existing release draft $tag does not match the owned draft contract."
      return 1
    fi
    [[ -n "$generated_notes" ]] && rm -f "$generated_notes"
  fi

  decide_release "$tag" "$tag_exists" "$tag_commit" "$release_complete" "$app_changed" "$GITHUB_SHA"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  release_decision_main "$@"
fi
