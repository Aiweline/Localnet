# Weline Chat 10-Language Internationalization Design

## Outcome

Weline Chat 0.1.4 will provide a complete, first-class experience in ten languages on Windows and macOS. A fresh installation follows the operating-system language automatically, users can change the language without restarting, and the selected preference persists. Internationalization must cover application UI, user-facing runtime errors, Windows installer messages, and macOS Local Network permission text without changing peer identity, discovery, friendship, chat history, or transfer protocols.

## Supported locales

| Locale | Display name | Direction |
| --- | --- | --- |
| `zh-CN` | 简体中文 | LTR |
| `en-US` | English | LTR |
| `es-ES` | Español | LTR |
| `fr-FR` | Français | LTR |
| `de-DE` | Deutsch | LTR |
| `pt-BR` | Português (Brasil) | LTR |
| `ru-RU` | Русский | LTR |
| `ja-JP` | 日本語 | LTR |
| `ko-KR` | 한국어 | LTR |
| `ar-SA` | العربية | RTL |

English is the catalog fallback. Region variants that do not have an exact catalog use a language-prefix match, for example `es-MX` uses `es-ES`, `pt-PT` uses `pt-BR`, and `ar-EG` uses `ar-SA`.

## Approaches considered

### Chosen: one canonical catalog plus stable backend error codes

The React application owns human-readable runtime text. Rust returns stable machine-readable error codes and interpolation parameters to the webview. One canonical key set is translated into all ten locales, and platform-specific assets are synchronized from the same translation source where practical.

This keeps language policy in one place, lets the user change language instantly, prevents peers from sending each other localized protocol strings, and gives catalog completeness a mechanically verifiable contract.

### Rejected: translate only the React UI

This is quicker but leaves command failures, transfer rejection states, installer warnings, and macOS permission prompts in Chinese or English. It does not meet the requested complete multilingual experience.

### Rejected: independent catalogs in TypeScript and Rust

Separate catalogs would let each layer format its own messages, but duplicate keys and translations would drift. It would also make live language switching harder and could accidentally put localized strings into the network protocol.

## Locale resolution and persistence

The stored preference is either `auto` or one of the ten canonical locale tags. It is stored in the existing SQLite `settings` table, so no schema change is needed.

Startup resolution order is:

1. Read the persisted language preference.
2. For a canonical tag, use that locale.
3. For `auto`, match `navigator.languages` in order, first exactly and then by language prefix.
4. If no language matches, use `en-US`.

Fresh 0.1.4 databases store `auto`. During upgrade, an existing database with no language preference stores `zh-CN` once, preserving the behavior users already had before the update. The backend determines whether the database existed before it initializes the new setting; it does not infer this from nickname or chat data.

Selecting a language updates the UI immediately and persists it through a dedicated settings command. Selecting “System default” stores `auto`. While `auto` is active, the application listens for the browser `languagechange` event and also resolves the OS language again on every launch.

## Catalog contract

Locale files live under `src/i18n/locales/` and use the locale tag as the filename. The English catalog is the canonical key list. Keys are semantic and grouped by feature, for example:

- `common.*` for shared buttons, state, dates, and sizes;
- `onboarding.*` for initial profile setup;
- `sidebar.*`, `contacts.*`, and `friends.*` for discovery and friend flows;
- `chat.*` and `composer.*` for conversations and message composition;
- `transfer.*` for images, files, progress, completion, cancellation, and failure;
- `settings.*` for profile and language preferences;
- `permissions.*` for firewall and Local Network guidance;
- `errors.*` for stable backend and validation error codes.

Catalog interpolation uses named placeholders such as `{name}` and `{size}`. A validation command checks that all ten catalogs have:

- exactly the canonical key set;
- no empty translated values;
- the same placeholder names for each key;
- valid JSON and canonical locale metadata.

The validator is part of the normal `pnpm check` and package build path. It is a catalog integrity gate rather than a standalone test suite.

## Frontend architecture

`src/i18n/` provides a small internal internationalization layer with:

- locale metadata and OS-language matching;
- catalog loading and English fallback;
- a React provider and `useI18n()` hook;
- `t(key, params)` interpolation;
- locale-aware date, time, number, and file-size helpers built on `Intl`;
- document `lang` and `dir` synchronization.

No production dependency is required for these needs. The implementation will use the platform `Intl` APIs and a typed translation-key union derived from the canonical catalog.

Every user-visible literal in `src/main.tsx` moves into the catalog, including:

- onboarding, empty, loading, online, and offline states;
- nearby-user and friend-request actions;
- text, image, and file composer controls;
- transfer status, retryable failure guidance, and file picker labels;
- profile editing, permission guidance, validation, and confirmation text;
- accessible labels, titles, and tooltips.

User content is never translated. Nicknames, text messages, filenames, file paths, and peer-provided content remain exactly as entered.

## Language control and live switching

The existing profile/settings surface receives a Language field. The first choice is “System default”, followed by all ten languages shown using their native display names. The native labels remain recognizable even if the current interface language is unfamiliar.

Saving a new language:

1. applies the new catalog immediately;
2. sets `<html lang>` and `<html dir>`;
3. re-renders timestamps, formatted numbers, and state labels;
4. persists the preference locally;
5. shows a localized success or recoverable failure state.

The setting is device-local. It is not broadcast in discovery beacons, Hello messages, friendship records, or chat traffic.

## Arabic and bidirectional layout

`ar-SA` sets the document direction to RTL. Layout CSS will use logical properties such as `margin-inline`, `padding-inline`, `border-inline`, and `inset-inline` instead of physical left/right rules where direction matters.

Sidebar and content flow mirror for Arabic, as do direction-dependent navigation affordances. Media previews, user-generated text, filenames, progress direction, and universally meaningful icons are not blindly mirrored. Message bodies use Unicode bidirectional behavior so Arabic and Latin content can coexist.

The existing visual hierarchy, minimum window size, scroll behavior, and responsive breakpoints remain unchanged in all languages. Controls must accommodate longer German, French, and Russian labels without clipping.

## Backend error boundary

Rust continues to use rich internal error types and logs diagnostic details locally. Tauri commands expose a serializable error envelope:

```json
{
  "code": "transfer.file_unavailable",
  "params": { "name": "report.pdf" }
}
```

The frontend resolves `errors.<code>` in the active locale and interpolates safe parameters. Unknown codes display a localized generic error and retain the raw diagnostic only in local logs.

Peer-to-peer wire formats remain compatible with 0.1.2. Existing remote rejection strings are treated as diagnostics, not displayed directly; the receiving command maps the operation and failure category to a local stable code. No localized UI sentence is added to mDNS, UDP beacon, Hello, friend, message, or transfer payloads.

Input validation and expected application errors receive stable codes. Unexpected initialization failures that occur before the webview exists remain platform-level diagnostics, because no application locale has been resolved yet; the installer and macOS permission prompts are localized separately as described below.

## Windows installer localization

The NSIS bundle enables the Tauri/NSIS language identifiers corresponding to the ten supported locales. NSIS follows the operating-system installer language and falls back to English.

There is no custom firewall setup hook. The installer runs for the current user, does not request elevation, and does not stream localized system-command output into the NSIS log. Windows owns the separate first-run firewall consent when inbound LAN traffic requires it.

## macOS bundle localization

The macOS app bundle includes localized `InfoPlist.strings` resources for:

- `zh-Hans.lproj`;
- `en.lproj`;
- `es.lproj`;
- `fr.lproj`;
- `de.lproj`;
- `pt-BR.lproj`;
- `ru.lproj`;
- `ja.lproj`;
- `ko.lproj`;
- `ar.lproj`.

Each resource localizes `NSLocalNetworkUsageDescription`. The base `Info.plist` keeps an English fallback, and the localized purpose text consistently explains that Weline Chat needs the local network to discover nearby users and transfer messages and files directly. The Bonjour declaration and application identifier are unchanged.

This follows Apple's model in which language-specific `InfoPlist.strings` overrides human-readable values from the base property list.

## Compatibility and data safety

The following remain unchanged:

- `com.aiweline.localnet` application identifier;
- keychain/service identifiers and Ed25519 identity;
- SQLite database path and existing tables;
- friendship, conversation, message, and transfer records;
- mDNS plus UDP broadcast discovery behavior;
- Noise authentication, Yamux transport, and libp2p Peer ID;
- LAN-only text, image, and file transfer behavior.

The locale preference is a new settings row only. Rollback to 0.1.2 safely ignores it. Unrecognized future locale values resolve to `en-US` without deleting the stored value.

## Failure behavior

- A missing translation key falls back to English and records a development warning.
- A catalog load failure falls back to the bundled English catalog; the application remains usable.
- A settings write failure leaves the newly selected language active for the current session and shows a localized persistence warning.
- An unknown backend error code displays a localized generic message and logs the code.
- Long translated text wraps rather than truncating critical instructions or actions.
- Locale failures never stop discovery, messaging, or transfer services.

## Acceptance criteria

1. A fresh install follows each supported OS language and falls back to English for an unsupported language.
2. An existing 0.1.2 database with no locale preference opens in Simplified Chinese; users can then choose automatic or another language.
3. Language switching is immediate, persists after restart, and never changes peer identity, friends, history, or files.
4. All ten catalogs pass exact key, placeholder, nonempty-value, and JSON validation.
5. Onboarding, nearby users, friend requests, chat, image/file transfer, profile settings, empty/loading states, and representative failures are complete in every locale.
6. Arabic uses RTL layout without clipping, reversed media, or broken mixed-direction message content.
7. German, French, and Russian labels fit or wrap correctly at the minimum supported window size.
8. Every expected Rust/Tauri application failure visible in the webview is represented by a stable code that all catalogs can translate.
9. Windows NSIS contains all ten installer languages and installs for the current user without requesting UAC.
10. The macOS app bundle contains all ten localized `InfoPlist.strings` resources and a valid localized Local Network purpose string.
11. Windows and universal macOS packages build successfully as version 0.1.4.
12. Real two-device smoke checks confirm both default mDNS and mDNS-disabled UDP fallback discovery still work after the internationalization changes, with TUN enabled and disabled.

## Non-goals

- Automatic translation of conversations or files.
- Translating nicknames, filenames, or user-entered content.
- Negotiating a shared language between peers.
- Adding cloud translation, analytics, or a localization service.
- Changing network discovery, authentication, friendship, or transfer protocols.
- Supporting locales beyond the ten listed in this release.

## Platform references

- [Tauri Windows Installer configuration](https://v2.tauri.app/distribute/windows-installer/)
- [Apple Information Property List localization](https://developer.apple.com/library/archive/documentation/General/Reference/InfoPlistKeyReference/Articles/AboutInformationPropertyListFiles.html)
- [Apple Local Network usage description](https://developer.apple.com/documentation/bundleresources/information-property-list/nslocalnetworkusagedescription)
