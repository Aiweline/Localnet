import assert from "node:assert/strict";
import test from "node:test";

test("resolves exact and language-prefix matches across all supported locales", async () => {
  const { resolveLocale } = await import("../src/i18n/core.ts");

  assert.equal(resolveLocale(["ja-JP"]), "ja-JP");
  assert.equal(resolveLocale(["es-MX", "en-US"]), "es-ES");
  assert.equal(resolveLocale(["pt-PT"]), "pt-BR");
  assert.equal(resolveLocale(["ar-EG"]), "ar-SA");
  assert.equal(resolveLocale(["zh-TW", "de-AT"]), "de-DE");
  assert.equal(resolveLocale(["nl-NL"]), "en-US");
});

test("renders a real localized settings title in every supported language", async () => {
  const { translate } = await import("../src/i18n/core.ts");
  const expected = {
    "zh-CN": "本机设置",
    "en-US": "Settings",
    "es-ES": "Configuración",
    "fr-FR": "Paramètres",
    "de-DE": "Einstellungen",
    "pt-BR": "Configurações",
    "ru-RU": "Настройки",
    "ja-JP": "設定",
    "ko-KR": "설정",
    "ar-SA": "الإعدادات",
  } as const;

  for (const [locale, title] of Object.entries(expected)) {
    assert.equal(translate(locale, "settings.title"), title, locale);
  }
});

test("catalogs have the exact canonical keys and placeholder contract", async () => {
  const { catalogDiagnostics } = await import("../src/i18n/core.ts");
  assert.deepEqual(catalogDiagnostics(), []);
});

test("interpolates named values without translating user content", async () => {
  const { translate } = await import("../src/i18n/core.ts");
  assert.equal(
    translate("de-DE", "friends.requestFrom", { name: "小林" }),
    "小林 möchte dich als Kontakt hinzufügen.",
  );
  assert.equal(
    translate("ar-SA", "transfer.receivedComplete", { name: "report-报告.pdf" }),
    "اكتمل استلام report-报告.pdf.",
  );
});

test("marks only Arabic as right-to-left and resolves system-default preferences", async () => {
  const { localeDirection, resolveLanguagePreference } = await import("../src/i18n/core.ts");
  assert.equal(localeDirection("ar-SA"), "rtl");
  assert.equal(localeDirection("de-DE"), "ltr");
  assert.equal(resolveLanguagePreference("auto", ["ko-KR"]), "ko-KR");
  assert.equal(resolveLanguagePreference("fr-FR", ["ko-KR"]), "fr-FR");
  assert.equal(resolveLanguagePreference("forged", ["ko-KR"]), "en-US");
});

test("a stale bootstrap snapshot cannot overwrite the language selected in this session", async () => {
  const { reconcileLanguageSnapshot } = await import("../src/i18n/core.ts");

  assert.equal(reconcileLanguageSnapshot("de-DE", null), "de-DE");
  assert.equal(reconcileLanguageSnapshot("de-DE", "ja-JP"), "ja-JP");
  assert.equal(reconcileLanguageSnapshot("forged", null), "auto");
});
