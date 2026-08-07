// site/src/components/site-footer.tsx
import { Link } from "@tanstack/react-router"

export function SiteFooter() {
  return (
    <footer className="mt-12 border-t">
      <div className="mx-auto flex max-w-6xl items-baseline justify-between px-4 py-4 text-xs text-muted-foreground">
        <span>
          Data: Digital Prosopography of the Roman Republic (King&apos;s College
          London)
        </span>
        <Link to="/directory" className="hover:text-foreground hover:underline">
          Directory
        </Link>
      </div>
    </footer>
  )
}
