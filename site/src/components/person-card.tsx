// site/src/components/person-card.tsx
import { Link } from "@tanstack/react-router"
import { Badge } from "@/components/ui/badge"
import { EraRange } from "@/components/date-display"
import { displayName } from "@/lib/order"
import type { PersonSummary } from "@/data/types"

/** Full card for search results — shows office, era, badges. */
export function PersonCard({ person }: { person: PersonSummary }) {
  const name = displayName(person.name)

  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="block rounded-md border p-3 transition-colors hover:bg-accent"
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="truncate font-heading font-medium">{name}</p>
          <p className="text-sm text-muted-foreground">
            {person.highestOffice && <span>{person.highestOffice}</span>}
            {person.highestOffice && (person.eraFrom || person.eraTo) && (
              <span> · </span>
            )}
            <EraRange from={person.eraFrom} to={person.eraTo} />
          </p>
        </div>
        <div className="flex shrink-0 gap-1">
          {person.isPatrician && <Badge variant="secondary">Patrician</Badge>}
          {person.isNobilis && <Badge variant="secondary">Nobilis</Badge>}
        </div>
      </div>
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
      className="font-medium text-primary hover:underline"
    >
      {label}
    </Link>
  )
}
