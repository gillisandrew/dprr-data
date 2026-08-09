// site/src/components/fasti-row.tsx
import { Link } from "@tanstack/react-router"
import { EraRange } from "@/components/date-display"
import { displayName } from "@/lib/order"
import { slugify } from "@/lib/slug"
import type { PersonSummary } from "@/data/types"

export function FastiRow({ person }: { person: PersonSummary }) {
  const name = displayName(person.name)
  const gensSlug = person.nomen ? slugify(person.nomen) : ""
  return (
    <div className="ledger-row relative px-1 transition-colors">
      <p className="font-heading text-[0.95rem] leading-snug">
        <Link
          to="/persons/$id"
          params={{ id: person.id }}
          className="after:absolute after:inset-0"
        >
          {name}
        </Link>
        {person.highestOffice && (
          <span className="ml-2 font-sans text-sm text-accent-ink">
            — {person.highestOffice}
          </span>
        )}
      </p>
      <p className="text-xs leading-snug text-muted-foreground">
        {person.contextLine && <span>{person.contextLine} · </span>}
        {person.filiation && <span>{person.filiation} · </span>}
        <EraRange from={person.eraFrom} to={person.eraTo} />
        {person.nomen && (
          <>
            {" · "}
            {gensSlug ? (
              <Link
                to="/gentes/$slug"
                params={{ slug: gensSlug }}
                className="relative z-10 hover:text-accent-ink hover:underline"
              >
                gens {person.nomen}
              </Link>
            ) : (
              <span>gens {person.nomen}</span>
            )}
          </>
        )}
      </p>
    </div>
  )
}
