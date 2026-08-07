// site/src/components/person-rail.tsx
import { Link } from "@tanstack/react-router"
import { slugify } from "@/lib/slug"
import { Badge } from "@/components/ui/badge"
import { DateDisplay } from "@/components/date-display"
import { SourceCitation } from "@/components/source-citation"
import { PersonLink } from "@/components/person-card"
import type { Person, Relationship, DateInfo, Concordance } from "@/data/types"

/** Groups relationships by type (alphabetical), people by display name within. */
function groupRelationships(rels: Relationship[]): [string, Relationship[]][] {
  const byType = new Map<string, Relationship[]>()
  for (const r of rels) {
    const list = byType.get(r.relationshipType) ?? []
    list.push(r)
    byType.set(r.relationshipType, list)
  }
  const strip = (s: string) => s.replace(/^[A-Z]{4}\d+ /, "")
  return [...byType]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([type, list]) => [
      type,
      [...list].sort((a, b) =>
        strip(a.relatedPersonName).localeCompare(strip(b.relatedPersonName))
      ),
    ])
}

function RailCard({
  title,
  children,
}: {
  title: string
  children: React.ReactNode
}) {
  return (
    <div className="rounded-md border p-4">
      <h2 className="font-heading text-sm font-semibold">{title}</h2>
      <div className="mt-2">{children}</div>
    </div>
  )
}

export function IdentityCard({ person }: { person: Person }) {
  return (
    <RailCard title="Identity">
      <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs text-muted-foreground">
        {person.praenomen && (
          <>
            <dt className="font-medium">Praenomen</dt>
            <dd>{person.praenomen}</dd>
          </>
        )}
        {person.nomen && (
          <>
            <dt className="font-medium">Nomen</dt>
            <dd>{person.nomen}</dd>
          </>
        )}
        {person.cognomen && (
          <>
            <dt className="font-medium">Cognomen</dt>
            <dd>{person.cognomen}</dd>
          </>
        )}
        {person.filiation && (
          <>
            <dt className="font-medium">Filiation</dt>
            <dd>{person.filiation}</dd>
          </>
        )}
        {person.reNumber && (
          <>
            <dt className="font-medium">RE</dt>
            <dd>{person.reNumber}</dd>
          </>
        )}
        {person.tribes.length > 0 && (
          <>
            <dt className="font-medium">Tribe</dt>
            <dd>
              {person.tribes.map((t, i) => (
                <span key={t}>
                  {i > 0 && ", "}
                  <Link
                    to="/tribes/$slug"
                    params={{ slug: slugify(t) }}
                    className="hover:underline"
                  >
                    {t}
                  </Link>
                </span>
              ))}
            </dd>
          </>
        )}
        <dt className="font-medium">DPRR ID</dt>
        <dd className="font-mono">{person.id}</dd>
      </dl>
      {(person.sex || person.isPatrician || person.isNobilis) && (
        <div className="mt-3 flex flex-wrap gap-1">
          {person.sex && <Badge variant="outline">{person.sex}</Badge>}
          {person.isPatrician && <Badge variant="secondary">Patrician</Badge>}
          {person.isNobilis && <Badge variant="secondary">Nobilis</Badge>}
        </div>
      )}
      {person.nobilisNotes && (
        <p className="mt-3 text-xs italic">{person.nobilisNotes}</p>
      )}
    </RailCard>
  )
}

export function FamilyCard({ person }: { person: Person }) {
  if (person.relationships.length === 0) return null
  const groups = groupRelationships(person.relationships)
  return (
    <RailCard title="Family">
      <div className="space-y-3">
        {groups.map(([type, rels]) => (
          <div key={type}>
            <p className="text-xs font-medium text-muted-foreground capitalize">
              {type}
            </p>
            <ul className="mt-1 space-y-1">
              {rels.map((rel) => (
                <li key={rel.id} className="text-sm">
                  {rel.relatedPersonId ? (
                    <PersonLink
                      id={rel.relatedPersonId}
                      name={rel.relatedPersonName}
                    />
                  ) : (
                    <span>
                      {rel.relatedPersonName.replace(/^[A-Z]{4}\d+ /, "")}
                    </span>
                  )}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </RailCard>
  )
}

function DateEntry({ dateInfo }: { dateInfo: DateInfo }) {
  return (
    <div className="flex flex-wrap items-baseline gap-1 text-xs">
      <span className="font-medium text-muted-foreground capitalize">
        {dateInfo.type}:
      </span>
      <DateDisplay year={dateInfo.value} uncertain={dateInfo.isUncertain} />
      {dateInfo.notes && (
        <span className="text-muted-foreground">— {dateInfo.notes}</span>
      )}
      <SourceCitation
        name={dateInfo.secondarySource}
        className="text-muted-foreground"
      />
    </div>
  )
}

export function DatesCard({ person }: { person: Person }) {
  if (person.dateInformation.length === 0) return null
  const sorted = [...person.dateInformation].sort((a, b) => a.value - b.value)
  return (
    <RailCard title="Dates">
      <div className="space-y-2">
        {sorted.map((d, i) => (
          <DateEntry key={i} dateInfo={d} />
        ))}
      </div>
    </RailCard>
  )
}

export function LinksCard({ person }: { person: Person }) {
  if (person.concordances.length === 0) return null
  const grouped = new Map<string, Concordance[]>()
  for (const c of person.concordances) {
    const existing = grouped.get(c.system) ?? []
    existing.push(c)
    grouped.set(c.system, existing)
  }
  const sortedGroups = [...grouped].sort((a, b) => a[0].localeCompare(b[0]))
  return (
    <RailCard title="External Links">
      <dl className="space-y-2 text-xs">
        {sortedGroups.map(([system, links]) => (
          <div key={system}>
            <dt className="font-medium text-muted-foreground capitalize">
              {system}
            </dt>
            <dd className="mt-0.5 flex flex-col gap-1">
              {links.map((link, i) => (
                <a
                  key={i}
                  href={link.uri}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="break-all text-primary hover:underline"
                >
                  {link.uri}
                </a>
              ))}
            </dd>
          </div>
        ))}
      </dl>
    </RailCard>
  )
}

/** Sticky rail composed of the four compact identity/family/dates/links cards. */
export function PersonRail({ person }: { person: Person }) {
  return (
    <div className="space-y-4">
      <IdentityCard person={person} />
      <FamilyCard person={person} />
      <DatesCard person={person} />
      <LinksCard person={person} />
    </div>
  )
}
