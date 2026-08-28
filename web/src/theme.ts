export type ThemePreference = 'system' | 'light' | 'dark'
export type ThemeSurface = 'auth' | 'dashboard'

export const themeStorageKey = 'slipway.web.theme'
const preferences: ThemePreference[] = ['system', 'light', 'dark']

export function readThemePreference(): ThemePreference {
  try {
    const stored = window.localStorage.getItem(themeStorageKey)
    return preferences.includes(stored as ThemePreference) ? stored as ThemePreference : 'system'
  } catch {
    return 'system'
  }
}

export function persistThemePreference(preference: ThemePreference) {
  try {
    window.localStorage.setItem(themeStorageKey, preference)
  } catch {
    // Storage can be unavailable in hardened or private browser contexts.
  }
}

export function systemPrefersDark() {
  return typeof window.matchMedia === 'function' && window.matchMedia('(prefers-color-scheme: dark)').matches
}

export function applyTheme(preference: ThemePreference, systemDark = systemPrefersDark()) {
  const resolved = preference === 'system' ? systemDark ? 'dark' : 'light' : preference
  const root = document.documentElement
  root.dataset.theme = resolved

  syncThemeColor()
}

export function syncThemeColor(surface?: ThemeSurface) {
  const root = document.documentElement
  const themeColor = document.querySelector<HTMLMetaElement>('meta[name="theme-color"]')
  if (!themeColor) return

  const activeSurface = surface ?? (document.querySelector('.auth-shell') ? 'auth' : 'dashboard')
  const property = activeSurface === 'auth' ? '--auth-bg' : '--surface-canvas'
  const color = window.getComputedStyle(root).getPropertyValue(property).trim()
  if (color) themeColor.content = color
}
