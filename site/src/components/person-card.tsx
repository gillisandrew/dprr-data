// site/src/components/person-card.tsx
import { Link } from "@tanstack/react-router"
import { Badge } from "@/components/ui/badge"
import { EraRange } from "@/components/date-display"
import type { PersonSummary } from "@/data/types"

/** Full card for search results — shows office, era, badges. */
export function PersonCard({ person }: { person: PersonSummary }) {
  // Strip the DPRR ID prefix from the display name
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")

  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="hover:bg-accent block rounded-md border p-3 transition-colors"
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="font-heading truncate font-medium">{displayName}</p>
          <p className="text-muted-foreground text-sm">
            {person.highestOffice && (
              <span>{person.highestOffice}</span>
            )}
            {person.highestOffice && (person.eraFrom || person.eraTo) && (
              <span> · </span>
            )}
            <EraRange from={person.eraFrom} to={person.eraTo} />
          </p>
        </div>
        <div className="flex shrink-0 gap-1">
          {person.isPatrician && (
            <Badge variant="secondary">Patrician</Badge>
          )}
          {person.isNobilis && (
            <Badge variant="secondary">Nobilis</Badge>
          )}
        </div>
      </div>
    </Link>
  )
}

/** Compact inline link for relationship contexts — just name as a link. */
export function PersonLink({
  id,
  name,
}: {
  id: string
  name: string
}) {
  const displayName = name.replace(/^[A-Z]{4}\d+ /, "") || id
  return (
    <Link
      to="/persons/$id"
      params={{ id }}
      className="text-primary font-medium hover:underline"
    >
      {displayName}
    </Link>
  )
}
