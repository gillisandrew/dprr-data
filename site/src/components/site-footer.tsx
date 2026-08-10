// site/src/components/site-footer.tsx
import { Link } from "@tanstack/react-router"

export function SiteFooter() {
  return (
    <footer className="mt-12 border-t border-rule-hair">
      <div className="mx-auto flex max-w-6xl flex-wrap items-baseline justify-between gap-x-6 gap-y-1 px-4 py-4 text-xs text-muted-foreground">
        <span>
          Unofficial interface to data from the Digital Prosopography of the
          Roman Republic (KCL) ·{" "}
          <a
            href="https://creativecommons.org/licenses/by-nc/4.0/"
            target="_blank"
            rel="noreferrer"
            className="hover:text-foreground hover:underline"
          >
            CC BY-NC 4.0
          </a>{" "}
          ·{" "}
          <Link to="/about" className="hover:text-foreground hover:underline">
            About
          </Link>
        </span>
        <Link to="/directory" className="hover:text-foreground hover:underline">
          Directory
        </Link>
      </div>
    </footer>
  )
}
