(() => {
  const storageKey = 'slipway.web.theme'
  const allowedPreferences = new Set(['system', 'light', 'dark'])
  let preference = 'system'
  let signedIn = false

  try {
    const stored = window.localStorage.getItem(storageKey)
    if (allowedPreferences.has(stored)) preference = stored
  } catch {
    // Storage can be unavailable in hardened or private browser contexts.
  }

  try {
    signedIn = Boolean(window.sessionStorage.getItem('slipway.web.token'))
  } catch {
    // The token gate is the safe default when session storage is unavailable.
  }

  const systemDark = typeof window.matchMedia === 'function'
    && window.matchMedia('(prefers-color-scheme: dark)').matches
  const resolved = preference === 'system' ? systemDark ? 'dark' : 'light' : preference
  const root = document.documentElement
  root.dataset.theme = resolved

  const themeColor = document.querySelector('meta[name="theme-color"]')
  const dashboardColor = resolved === 'dark' ? '#0f1311' : '#eeece4'
  const authColor = resolved === 'dark' ? '#080c0a' : '#171c19'
  if (themeColor) themeColor.content = signedIn ? dashboardColor : authColor
})()
