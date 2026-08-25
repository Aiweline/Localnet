import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const localeDirectories = [
  "en.lproj",
  "zh-Hans.lproj",
  "es.lproj",
  "fr.lproj",
  "de.lproj",
  "pt-BR.lproj",
  "ru.lproj",
  "ja.lproj",
  "ko.lproj",
  "ar.lproj",
];

test("macOS bundle ships a localized Local Network purpose string for all ten languages", async () => {
  for (const directory of localeDirectories) {
    const contents = await readFile(
      new URL(`../src-tauri/macos-locales/${directory}/InfoPlist.strings`, import.meta.url),
      "utf8",
    );
    assert.match(
      contents,
      /^"NSLocalNetworkUsageDescription" = ".+";\r?\n$/u,
      `${directory} must contain one non-empty localized purpose string`,
    );
  }
});

test("Tauri preserves the lproj directory structure in the macOS Resources directory", async () => {
  const config = JSON.parse(await readFile(new URL("../src-tauri/tauri.conf.json", import.meta.url), "utf8"));
  assert.deepEqual(config.bundle.resources, { "macos-locales/": "" });
});

test("the base plist has an English fallback for unsupported system languages", async () => {
  const plist = await readFile(new URL("../src-tauri/Info.plist", import.meta.url), "utf8");
  assert.match(plist, /Weline Localnet needs access to your local network/u);
  assert.doesNotMatch(plist, /[\u3400-\u9fff]/u);
});

test("Windows installer always offers and hands off the exact ten supported languages", async () => {
  const config = JSON.parse(await readFile(new URL("../src-tauri/tauri.conf.json", import.meta.url), "utf8"));
  assert.equal(config.bundle.windows.nsis.displayLanguageSelector, true);
  assert.deepEqual(config.bundle.windows.nsis.languages, [
    "English",
    "SimpChinese",
    "Spanish",
    "French",
    "German",
    "PortugueseBR",
    "Russian",
    "Japanese",
    "Korean",
    "Arabic",
  ]);

  const hook = await readFile(
    new URL("../src-tauri/windows/installer-hooks.nsh", import.meta.url),
    "utf8",
  );
  assert.match(hook, /!define MUI_LANGDLL_ALWAYSSHOW/u);
  assert.match(hook, /StrCpy \$R9 "en-US"/u);
  for (const [nsisLanguage, locale] of [
    ["SIMPCHINESE", "zh-CN"],
    ["SPANISH", "es-ES"],
    ["FRENCH", "fr-FR"],
    ["GERMAN", "de-DE"],
    ["PORTUGUESEBR", "pt-BR"],
    ["RUSSIAN", "ru-RU"],
    ["JAPANESE", "ja-JP"],
    ["KOREAN", "ko-KR"],
    ["ARABIC", "ar-SA"],
  ]) {
    assert.match(
      hook,
      new RegExp(`StrCmp \\$LANGUAGE \\$\\{LANG_${nsisLanguage}\\} 0 \\+2\\r?\\n\\s+StrCpy \\$R9 "${locale}"`, "u"),
    );
  }
  assert.match(hook, /\$APPDATA\\com\.aiweline\.localnet\\installer-locale/u);
  assert.match(hook, /ole32::CoCreateGuid\(g \.s\)/u);
  assert.match(hook, /Pop \$R7/u);
  assert.match(hook, /FileWrite \$R8 "\$R9\$\\n\$R7"/u);
});
