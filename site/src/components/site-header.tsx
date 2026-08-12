// site/src/components/site-header.tsx
import { Link } from "@tanstack/react-router"
import { ThemeToggle } from "@/components/theme-toggle"
// Inlined (not <img>) so page CSS can retheme it with the site's .dark
// class — an embedded image only sees the OS color scheme, which the
// theme toggle can override. Single source shared with the favicon link
// in __root.tsx (Vite forbids ?raw imports from public/, so it lives in
// src/assets and the favicon uses the hashed asset URL).
import iconSvg from "@/assets/icon.svg?raw"

const links = [
  { to: "/offices", label: "Offices" },
  { to: "/gentes", label: "Gentes" },
  { to: "/tribes", label: "Tribes" },
  { to: "/provinces", label: "Locations" },
] as const

export function SiteHeader() {
  return (
    <header className="rule-hair">
      <nav className="mx-auto flex max-w-6xl items-baseline gap-6 px-4 py-3">
        <Link to="/" aria-label="DPRR — home" className="self-center">
          <span
            aria-hidden="true"
            className="-my-1 block h-7 [&>svg]:h-full [&>svg]:w-auto"
            dangerouslySetInnerHTML={{ __html: iconSvg }}
          />
        </Link>
        {links.map((l) => (
          <Link
            key={l.to}
            to={l.to}
            className="text-sm text-muted-foreground hover:text-accent-ink"
            activeProps={{ className: "text-sm text-accent-ink" }}
          >
            {l.label}
          </Link>
        ))}
        <div className="ml-auto self-center">
          <ThemeToggle />
        </div>
      </nav>
    </header>
  )
}
