validate_release_ref() {
  local event_name="${1:?event name is required}"
  local source_ref="${2:?source ref is required}"

  if [[ "$event_name" == "workflow_dispatch" && "$source_ref" != "refs/heads/main" ]]; then
    echo "::error::workflow_dispatch releases are allowed only from refs/heads/main (received $source_ref)."
    return 1
  fi
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
  validate_remote_release_tag_arguments "$remote_name" "$tag" "$expected_sha"

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
  validate_remote_release_tag_arguments "$remote_name" "$tag" "$expected_sha"

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

  validate_release_ref "$GITHUB_EVENT_NAME" "$GITHUB_REF"

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
    local release_json
    release_json="$(gh release view "$tag" --repo "$GITHUB_REPOSITORY" --json isDraft,isPrerelease,assets 2>/dev/null || true)"
    if [[ -n "$release_json" ]]; then
      local expected_assets actual_assets is_draft is_prerelease
      expected_assets="$(printf '%s\n' \
        "Weline_Localnet_${version}_SHA256SUMS.txt" \
        "Weline_Localnet_${version}_universal.dmg" \
        "Weline_Localnet_${version}_universal.dmg.sha256" \
        "Weline_Localnet_${version}_x64-portable.exe" \
        "Weline_Localnet_${version}_x64-setup.exe" | sort)"
      actual_assets="$(jq -r '.assets[].name' <<< "$release_json" | sort)"
      is_draft="$(jq -r '.isDraft' <<< "$release_json")"
      is_prerelease="$(jq -r '.isPrerelease' <<< "$release_json")"
      if [[ "$is_draft" == "false" && "$is_prerelease" == "false" && "$actual_assets" == "$expected_assets" ]]; then
        release_complete=true
      fi
    fi
  fi

  decide_release "$tag" "$tag_exists" "$tag_commit" "$release_complete" "$app_changed" "$GITHUB_SHA"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  release_decision_main "$@"
fi
