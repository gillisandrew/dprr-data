// site/src/routes/persons.$id.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getPersonById } from "@/server/data"
import { Badge } from "@/components/ui/badge"
import { Section } from "@/components/section"
import { DateDisplay, EraRange } from "@/components/date-display"
import { SourceCitation } from "@/components/source-citation"
import { PersonLink } from "@/components/person-card"
import type {
  Person,
  PostAssertion,
  Relationship,
  DateInfo,
  Note,
  Concordance,
} from "@/data/types"

export const Route = createFileRoute("/persons/$id")({
  loader: ({ params }) => getPersonById({ data: params.id }),
  head: ({ loaderData: person }) => {
    if (!person) return {}
    const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
    const desc = [person.highestOffice, person.isPatrician ? "Patrician" : null]
      .filter(Boolean)
      .join(" · ")
    return {
      meta: [
        { title: `${displayName} (${person.id}) — DPRR` },
        { name: "description", content: desc },
        { property: "og:title", content: `${displayName} — DPRR` },
        { property: "og:description", content: desc },
        { property: "og:type", content: "profile" },
      ],
    }
  },
  component: PersonPage,
})

function PersonPage() {
  const person = Route.useLoaderData()
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <PersonHeader person={person} displayName={displayName} />

      {person.postAssertions.length > 0 && (
        <Section title="Offices" count={person.postAssertions.length}>
          <div className="space-y-4">
            {person.postAssertions.map((pa) => (
              <OfficeEntry key={pa.id} assertion={pa} />
            ))}
          </div>
        </Section>
      )}

      {person.relationships.length > 0 && (
        <Section title="Relationships" count={person.relationships.length}>
          <div className="space-y-3">
            {person.relationships.map((rel) => (
              <RelationshipEntry key={rel.id} relationship={rel} />
            ))}
          </div>
        </Section>
      )}

      {person.dateInformation.length > 0 && (
        <Section title="Dates" count={person.dateInformation.length}>
          <div className="space-y-2">
            {person.dateInformation.map((d, i) => (
              <DateEntry key={i} dateInfo={d} />
            ))}
          </div>
        </Section>
      )}

      {person.personNotes.length > 0 && (
        <Section title="Notes" count={person.personNotes.length}>
          <div className="space-y-4">
            {person.personNotes.map((note, i) => (
              <NoteEntry key={i} note={note} />
            ))}
          </div>
        </Section>
      )}

      {person.concordances.length > 0 && (
        <Section title="External Links" count={person.concordances.length}>
          <ConcordanceList concordances={person.concordances} />
        </Section>
      )}
    </div>
  )
}

function PersonHeader({
  person,
  displayName,
}: {
  person: Person
  displayName: string
}) {
  return (
    <header className="mb-8">
      <h1 className="font-heading text-3xl font-bold">{displayName}</h1>
      {(person.eraFrom !== null || person.eraTo !== null) && (
        <p className="mt-1 text-lg text-muted-foreground">
          <EraRange from={person.eraFrom} to={person.eraTo} />
        </p>
      )}
      <div className="mt-3 flex flex-wrap gap-2">
        {person.sex && <Badge variant="outline">{person.sex}</Badge>}
        {person.isPatrician && <Badge variant="secondary">Patrician</Badge>}
        {person.isNobilis && <Badge variant="secondary">Nobilis</Badge>}
      </div>
      <dl className="mt-4 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm text-muted-foreground">
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
        {person.tribe && (
          <>
            <dt className="font-medium">Tribe</dt>
            <dd>{person.tribe}</dd>
          </>
        )}
        {person.highestOffice && (
          <>
            <dt className="font-medium">Highest Office</dt>
            <dd>{person.highestOffice}</dd>
          </>
        )}
        <dt className="font-medium">DPRR ID</dt>
        <dd className="font-mono text-xs">{person.id}</dd>
      </dl>
      {person.nobilisNotes && (
        <p className="mt-3 text-sm italic">{person.nobilisNotes}</p>
      )}
    </header>
  )
}

function OfficeEntry({ assertion }: { assertion: PostAssertion }) {
  return (
    <div className="border-l-2 pl-4">
      <p className="font-medium">
        {assertion.officeName}
        {assertion.officeAbbreviation && (
          <span className="ml-1 text-sm text-muted-foreground">
            ({assertion.officeAbbreviation})
          </span>
        )}
      </p>
      {(assertion.dateStart || assertion.dateEnd) && (
        <p className="text-sm text-muted-foreground">
          {assertion.dateStart !== null && assertion.dateEnd !== null ? (
            assertion.dateStart === assertion.dateEnd ? (
              <DateDisplay year={assertion.dateStart} />
            ) : (
              <EraRange from={assertion.dateStart} to={assertion.dateEnd} />
            )
          ) : (
            <DateDisplay
              year={(assertion.dateStart ?? assertion.dateEnd) as number}
            />
          )}
        </p>
      )}
      {assertion.originalText && (
        <p className="mt-1 text-sm">{assertion.originalText}</p>
      )}
      <SourceCitation
        name={assertion.secondarySource}
        className="mt-1 block text-xs text-muted-foreground"
      />
      {assertion.primarySourceRefs.length > 0 && (
        <div className="mt-1">
          {assertion.primarySourceRefs.map((ref, i) => (
            <p key={i} className="text-xs text-muted-foreground">
              {ref}
            </p>
          ))}
        </div>
      )}
      {assertion.notes.map((note, i) => (
        <div key={i} className="mt-2 rounded bg-muted/50 p-3 text-sm">
          <p className="mb-1 text-xs font-medium text-muted-foreground">
            {note.type}
            {note.secondarySource && ` — ${note.secondarySource}`}
          </p>
          <p className="leading-relaxed whitespace-pre-wrap">{note.text}</p>
        </div>
      ))}
    </div>
  )
}

function RelationshipEntry({ relationship }: { relationship: Relationship }) {
  const relatedDisplayName = relationship.relatedPersonName.replace(
    /^[A-Z]{4}\d+ /,
    ""
  )
  return (
    <div className="flex items-baseline gap-2">
      <span className="text-sm text-muted-foreground capitalize">
        {relationship.relationshipType}:
      </span>
      {relationship.relatedPersonId ? (
        <PersonLink
          id={relationship.relatedPersonId}
          name={relationship.relatedPersonName}
        />
      ) : (
        <span>{relatedDisplayName}</span>
      )}
    </div>
  )
}

function DateEntry({ dateInfo }: { dateInfo: DateInfo }) {
  return (
    <div className="flex items-baseline gap-2 text-sm">
      <span className="font-medium text-muted-foreground capitalize">
        {dateInfo.type}:
      </span>
      <DateDisplay year={dateInfo.value} uncertain={dateInfo.isUncertain} />
      {dateInfo.notes && (
        <span className="text-muted-foreground">— {dateInfo.notes}</span>
      )}
      <SourceCitation
        name={dateInfo.secondarySource}
        className="text-xs text-muted-foreground"
      />
    </div>
  )
}

function NoteEntry({ note }: { note: Note }) {
  return (
    <div className="rounded bg-muted/50 p-3">
      <p className="mb-1 text-xs font-medium text-muted-foreground">
        {note.type}
        {note.secondarySource && ` — ${note.secondarySource}`}
      </p>
      <p className="text-sm leading-relaxed whitespace-pre-wrap">{note.text}</p>
    </div>
  )
}

function ConcordanceList({ concordances }: { concordances: Concordance[] }) {
  // Group by system
  const grouped = new Map<string, Concordance[]>()
  for (const c of concordances) {
    const existing = grouped.get(c.system) ?? []
    existing.push(c)
    grouped.set(c.system, existing)
  }

  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
      {[...grouped].map(([system, links]) => (
        <div key={system} className="contents">
          <dt className="font-medium capitalize">{system}</dt>
          <dd className="flex flex-col gap-1">
            {links.map((link, i) => (
              <a
                key={i}
                href={link.uri}
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary break-all hover:underline"
              >
                {link.uri}
              </a>
            ))}
          </dd>
        </div>
      ))}
    </dl>
  )
}
