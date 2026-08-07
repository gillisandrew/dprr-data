// site/src/components/fasti-row.tsx
import { Link } from "@tanstack/react-router"
import { EraRange } from "@/components/date-display"
import { displayName } from "@/lib/order"
import type { PersonSummary } from "@/data/types"

export function FastiRow({ person }: { person: PersonSummary }) {
  const name = displayName(person.name)
  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="block border-b px-1 py-2 transition-colors hover:bg-accent"
    >
      <p className="font-heading text-sm font-medium">
        {name}
        {person.highestOffice && (
          <span className="ml-2 font-normal">— {person.highestOffice}</span>
        )}
      </p>
      <p className="text-xs text-muted-foreground">
        {person.filiation && <span>{person.filiation} · </span>}
        <EraRange from={person.eraFrom} to={person.eraTo} />
        {person.nomen && <span> · gens {person.nomen}</span>}
      </p>
    </Link>
  )
}
