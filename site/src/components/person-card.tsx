// site/src/components/person-card.tsx
import { Link } from "@tanstack/react-router"
import { EraRange } from "@/components/date-display"
import { displayName } from "@/lib/order"
import type { PersonSummary } from "@/data/types"

/** Two-line ledger row for tribe/gens member lists — office, era, status. */
export function PersonCard({ person }: { person: PersonSummary }) {
  const name = displayName(person.name)

  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="ledger-row group block px-1 transition-colors"
    >
      <p className="font-heading text-[0.95rem] leading-snug">
        <span className="group-hover:text-accent-ink">{name}</span>
        {person.highestOffice && (
          <span className="ml-2 font-sans text-sm text-accent-ink">
            — {person.highestOffice}
          </span>
        )}
        {person.statuses.includes("Patrician") && (
          <span className="small-caps ml-2 text-muted-foreground">
            patrician
          </span>
        )}
        {person.statuses.includes("Nobilis") && (
          <span className="small-caps ml-1 text-muted-foreground">nobilis</span>
        )}
      </p>
      <p className="text-xs leading-snug text-muted-foreground">
        {person.filiation && <span>{person.filiation} · </span>}
        <EraRange from={person.eraFrom} to={person.eraTo} />
      </p>
    </Link>
  )
}

/** Compact inline link for relationship contexts — just name as a link. */
export function PersonLink({ id, name }: { id: string; name: string }) {
  const label = displayName(name) || id
  return (
    <Link
      to="/persons/$id"
      params={{ id }}
      className="font-medium text-accent-ink hover:underline"
    >
      {label}
    </Link>
  )
}
