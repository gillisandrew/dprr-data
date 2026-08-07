// site/src/components/fasti-row.tsx
import { Link } from "@tanstack/react-router"
import { EraRange } from "@/components/date-display"
import type { PersonSummary } from "@/data/types"

export function FastiRow({ person }: { person: PersonSummary }) {
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="block border-b px-1 py-2 transition-colors hover:bg-accent"
    >
      <p className="font-heading text-sm font-medium">
        {displayName}
        {person.reNumber && (
          <span className="ml-1 font-normal text-muted-foreground">
            ({person.reNumber})
          </span>
        )}
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
