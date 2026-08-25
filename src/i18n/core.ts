import arSA from "./locales/ar-SA.json" with { type: "json" };
import deDE from "./locales/de-DE.json" with { type: "json" };
import enUS from "./locales/en-US.json" with { type: "json" };
import esES from "./locales/es-ES.json" with { type: "json" };
import frFR from "./locales/fr-FR.json" with { type: "json" };
import jaJP from "./locales/ja-JP.json" with { type: "json" };
import koKR from "./locales/ko-KR.json" with { type: "json" };
import ptBR from "./locales/pt-BR.json" with { type: "json" };
import ruRU from "./locales/ru-RU.json" with { type: "json" };
import zhCN from "./locales/zh-CN.json" with { type: "json" };

export const SUPPORTED_LOCALES = [
  "zh-CN", "en-US", "es-ES", "fr-FR", "de-DE", "pt-BR", "ru-RU", "ja-JP", "ko-KR", "ar-SA",
] as const;

export type AppLocale = (typeof SUPPORTED_LOCALES)[number];
export type LanguagePreference = AppLocale | "auto";
export type TranslationKey = keyof typeof enUS;
export type TranslationParams = Record<string, string | number>;

export interface LocaleOption {
  value: AppLocale;
  label: string;
  direction: "ltr" | "rtl";
}

export const LOCALE_OPTIONS: readonly LocaleOption[] = [
  { value: "zh-CN", label: "简体中文", direction: "ltr" },
  { value: "en-US", label: "English", direction: "ltr" },
  { value: "es-ES", label: "Español", direction: "ltr" },
  { value: "fr-FR", label: "Français", direction: "ltr" },
  { value: "de-DE", label: "Deutsch", direction: "ltr" },
  { value: "pt-BR", label: "Português (Brasil)", direction: "ltr" },
  { value: "ru-RU", label: "Русский", direction: "ltr" },
  { value: "ja-JP", label: "日本語", direction: "ltr" },
  { value: "ko-KR", label: "한국어", direction: "ltr" },
  { value: "ar-SA", label: "العربية", direction: "rtl" },
];

const catalogs: Record<AppLocale, Record<TranslationKey, string>> = {
  "zh-CN": zhCN,
  "en-US": enUS,
  "es-ES": esES,
  "fr-FR": frFR,
  "de-DE": deDE,
  "pt-BR": ptBR,
  "ru-RU": ruRU,
  "ja-JP": jaJP,
  "ko-KR": koKR,
  "ar-SA": arSA,
};

const localeSet = new Set<string>(SUPPORTED_LOCALES);
const prefixLocales: Record<string, AppLocale> = {
  en: "en-US",
  es: "es-ES",
  fr: "fr-FR",
  de: "de-DE",
  pt: "pt-BR",
  ru: "ru-RU",
  ja: "ja-JP",
  ko: "ko-KR",
  ar: "ar-SA",
};

export function resolveLocale(languages: readonly string[]): AppLocale {
  for (const language of languages) {
    const canonical = canonicalLocale(language);
    if (canonical) return canonical;
  }
  return "en-US";
}

export function resolveLanguagePreference(
  preference: string,
  languages: readonly string[],
): AppLocale {
  if (preference === "auto") return resolveLocale(languages);
  return localeSet.has(preference) ? preference as AppLocale : "en-US";
}

export function isLanguagePreference(value: string): value is LanguagePreference {
  return value === "auto" || localeSet.has(value);
}

export function reconcileLanguageSnapshot(
  snapshotPreference: string,
  sessionPreference: LanguagePreference | null,
): LanguagePreference {
  if (sessionPreference !== null) return sessionPreference;
  return isLanguagePreference(snapshotPreference) ? snapshotPreference : "auto";
}

export function localeDirection(locale: AppLocale): "ltr" | "rtl" {
  return locale === "ar-SA" ? "rtl" : "ltr";
}

export function translate(
  locale: string,
  key: TranslationKey,
  params: TranslationParams = {},
): string {
  const resolved = localeSet.has(locale) ? locale as AppLocale : "en-US";
  const template = catalogs[resolved][key] || enUS[key];
  return template.replace(/\{([A-Za-z][A-Za-z0-9]*)\}/g, (match, name: string) => (
    Object.prototype.hasOwnProperty.call(params, name) ? String(params[name]) : match
  ));
}

export function catalogDiagnostics(): string[] {
  const canonicalKeys = Object.keys(enUS).sort();
  const diagnostics: string[] = [];
  for (const locale of SUPPORTED_LOCALES) {
    const catalog = catalogs[locale];
    const keys = Object.keys(catalog).sort();
    if (keys.join("\n") !== canonicalKeys.join("\n")) {
      diagnostics.push(`${locale}: key set differs from en-US`);
      continue;
    }
    for (const key of canonicalKeys as TranslationKey[]) {
      if (!catalog[key].trim()) diagnostics.push(`${locale}:${key}: empty translation`);
      const expected = placeholders(enUS[key]);
      const actual = placeholders(catalog[key]);
      if (expected.join("\n") !== actual.join("\n")) {
        diagnostics.push(`${locale}:${key}: placeholders differ`);
      }
    }
  }
  return diagnostics;
}

function canonicalLocale(language: string): AppLocale | null {
  const normalized = language.replace("_", "-");
  const exact = SUPPORTED_LOCALES.find((locale) => locale.toLowerCase() === normalized.toLowerCase());
  if (exact) return exact;
  const [prefix, region = ""] = normalized.toLowerCase().split("-");
  if (prefix === "zh") return ["cn", "sg", "hans"].includes(region) ? "zh-CN" : null;
  return prefixLocales[prefix] ?? null;
}

function placeholders(value: string): string[] {
  return [...value.matchAll(/\{([A-Za-z][A-Za-z0-9]*)\}/g)].map((match) => match[1]).sort();
}
