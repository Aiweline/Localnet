# Release policy maintenance

GitHub releases depend on two repository settings that are maintained outside GitHub Actions by a repository administrator. The workflow verifies their effects but never creates, updates, disables, or bypasses them.

## Immutable release tags

The active repository ruleset is the administrator-reviewed trust anchor for stable tags:

- ID: `21367373`
- Name: `Protect immutable release tags`
- Target and enforcement: `tag`, `active`
- Ref condition: include only `refs/tags/v*`; exclude nothing
- Rules: `update` and `deletion`
- Bypass actors: none; `current_user_can_bypass` is `never`
- Audited update instant: `2026-08-25T03:07:17.637Z`

The workflow token has metadata and release-content permissions, not repository-administration permission. GitHub therefore omits `bypass_actors` and `current_user_can_bypass` from its ruleset response. The workflow does not interpret a missing field as an empty array. It accepts the hidden fields only while the fixed ruleset ID, name, UTC-normalized `updated_at`, source, target, enforcement, conditions, and rule types all match the administrator-reviewed snapshot. Every ruleset edit changes `updated_at` and closes the release gate.

If the ruleset must change, an administrator must first read back the full rule with an administrative identity and confirm an empty `bypass_actors` array and `current_user_can_bypass=never`. Update the audited snapshot in `scripts/release-policy.mjs`, its executable tests, and this document in the same reviewed commit. Do not add an administrator token to the release workflow.

## Immutable GitHub releases

Repository release immutability must remain enabled by an administrator. The workflow follows GitHub's immutable-release publication sequence:

1. Create a draft without assets.
2. Upload the five explicit, locally verified release assets.
3. Read back the draft and require exactly those five assets in `uploaded` state plus the exact title and notes.
4. Publish the draft.
5. Read back the release and require `immutable=true`.

A published release is never repaired. It is a no-op only when it is already immutable and has the exact five assets, title, and notes. Any mutable, incomplete, duplicated, or drifted published release fails closed and requires administrator investigation and, normally, a new version.
