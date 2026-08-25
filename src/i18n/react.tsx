import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  LOCALE_OPTIONS,
  localeDirection,
  resolveLanguagePreference,
  translate,
  type AppLocale,
  type LanguagePreference,
  type TranslationKey,
  type TranslationParams,
} from "./core";

interface I18nContextValue {
  locale: AppLocale;
  preference: LanguagePreference;
  localeOptions: typeof LOCALE_OPTIONS;
  setLanguagePreference: (preference: LanguagePreference) => void;
  t: (key: TranslationKey, params?: TranslationParams) => string;
  formatTime: (value: string) => string;
  formatBytes: (bytes: number) => string;
  relativeTime: (value: string) => string;
}

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [preference, setPreference] = useState<LanguagePreference>("auto");
  const [systemLanguages, setSystemLanguages] = useState<readonly string[]>(() => navigator.languages);
  const locale = resolveLanguagePreference(preference, systemLanguages);

  useEffect(() => {
    const update = () => setSystemLanguages([...navigator.languages]);
    window.addEventListener("languagechange", update);
    return () => window.removeEventListener("languagechange", update);
  }, []);

  useEffect(() => {
    document.documentElement.lang = locale;
    document.documentElement.dir = localeDirection(locale);
  }, [locale]);

  const t = useCallback(
    (key: TranslationKey, params?: TranslationParams) => translate(locale, key, params),
    [locale],
  );
  const formatTime = useCallback((value: string) => {
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? ""
      : new Intl.DateTimeFormat(locale, { hour: "2-digit", minute: "2-digit" }).format(date);
  }, [locale]);
  const formatBytes = useCallback((bytes: number) => {
    const safe = Number.isFinite(bytes) && bytes > 0 ? bytes : 0;
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    const index = safe === 0 ? 0 : Math.min(units.length - 1, Math.floor(Math.log(safe) / Math.log(1024)));
    const value = safe / 1024 ** index;
    const digits = index === 0 ? 0 : index < 3 ? 1 : 2;
    return `${new Intl.NumberFormat(locale, { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(value)} ${units[index]}`;
  }, [locale]);
  const relativeTime = useCallback((value: string) => {
    const milliseconds = Date.now() - new Date(value).getTime();
    if (!Number.isFinite(milliseconds) || milliseconds < 60_000) return t("time.justNow");
    const minutes = Math.floor(milliseconds / 60_000);
    if (minutes < 60) return t("time.minutesAgo", { count: minutes });
    const hours = Math.floor(minutes / 60);
    return hours < 24
      ? t("time.hoursAgo", { count: hours })
      : t("time.daysAgo", { count: Math.floor(hours / 24) });
  }, [t]);

  const value = useMemo<I18nContextValue>(() => ({
    locale,
    preference,
    localeOptions: LOCALE_OPTIONS,
    setLanguagePreference: setPreference,
    t,
    formatTime,
    formatBytes,
    relativeTime,
  }), [formatBytes, formatTime, locale, preference, relativeTime, t]);

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  const context = useContext(I18nContext);
  if (!context) throw new Error("useI18n must be used inside I18nProvider");
  return context;
}
