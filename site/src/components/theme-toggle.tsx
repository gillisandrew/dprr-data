// site/src/components/theme-toggle.tsx
import { useEffect, useState } from "react"
import { Moon, Sun } from "lucide-react"

function getStoredTheme(): "light" | "dark" | null {
  try {
    const stored = localStorage.getItem("theme")
    return stored === "light" || stored === "dark" ? stored : null
  } catch {
    return null
  }
}

export function ThemeToggle() {
  const [mounted, setMounted] = useState(false)
  const [isDark, setIsDark] = useState(false)

  // Read the class the no-flash init script already applied, then keep in
  // sync with live OS changes as long as the user hasn't made an explicit
  // choice.
  useEffect(() => {
    setMounted(true)
    setIsDark(document.documentElement.classList.contains("dark"))

    const media = matchMedia("(prefers-color-scheme: dark)")
    function handleChange(e: MediaQueryListEvent) {
      if (getStoredTheme() !== null) return
      document.documentElement.classList.toggle("dark", e.matches)
      setIsDark(e.matches)
    }
    media.addEventListener("change", handleChange)
    return () => media.removeEventListener("change", handleChange)
  }, [])

  function toggle() {
    const next = !isDark
    document.documentElement.classList.toggle("dark", next)
    try {
      localStorage.setItem("theme", next ? "dark" : "light")
    } catch {
      // localStorage unavailable (e.g. private browsing) — toggle still
      // works for the current page view.
    }
    setIsDark(next)
  }

  // Avoid a hydration mismatch: the server has no notion of the client's
  // stored preference or OS setting, so render a fixed-size placeholder
  // until mounted.
  if (!mounted) {
    return <span className="inline-block h-4 w-4" aria-hidden="true" />
  }

  return (
    <button
      type="button"
      onClick={toggle}
      aria-label="Toggle dark mode"
      className="text-muted-foreground hover:text-foreground"
    >
      {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
    </button>
  )
}
