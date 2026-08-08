// site/src/components/site-header.tsx
import { Link } from "@tanstack/react-router"

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
        <Link to="/" className="font-heading font-bold">
          DPRR
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
      </nav>
    </header>
  )
}
