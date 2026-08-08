// site/src/routes/persons.$id.tsx
import { createFileRoute, Link } from "@tanstack/react-router"
import { getPersonById } from "@/server/data"
import { slugify } from "@/lib/slug"
import { SITE_URL } from "@/lib/site"
import { Badge } from "@/components/ui/badge"
import { Section } from "@/components/section"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { DateDisplay, EraRange } from "@/components/date-display"
import { SourceCitation } from "@/components/source-citation"
import { PersonLink } from "@/components/person-card"
import {
  IdentityCard,
  PersonRail,
  groupRelationships,
} from "@/components/person-rail"
import type { PostAssertion, Note, Relationship } from "@/data/types"

export const Route = createFileRoute("/persons/$id")({
  loader: ({ params }) => getPersonById({ data: params.id }),
  head: ({ loaderData: person }) => {
    if (!person) return {}
    const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
    const desc = [person.highestOffice, person.isPatrician ? "Patrician" : null]
      .filter(Boolean)
      .join(" · ")
    const jsonLd = {
      "@context": "https://schema.org",
      "@type": "Person",
      name: displayName,
      ...(person.otherNames ? { alternateName: person.otherNames } : {}),
      gender: person.sex,
      ...(desc ? { description: desc } : {}),
      identifier: person.id,
      url: `${SITE_URL}/persons/${person.id}`,
      ...(person.concordances.length > 0
        ? { sameAs: person.concordances.map((c) => c.uri) }
        : {}),
    }
    return {
      meta: [
        { title: `${displayName} (${person.id}) — DPRR` },
        { name: "description", content: desc },
        { property: "og:title", content: `${displayName} — DPRR` },
        { property: "og:description", content: desc },
        { property: "og:type", content: "profile" },
      ],
      scripts: [
        { type: "application/ld+json", children: JSON.stringify(jsonLd) },
      ],
    }
  },
  component: PersonPage,
})

function PersonPage() {
  const person = Route.useLoaderData()
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
  const sortedNotes = [...person.personNotes].sort((a, b) =>
    a.type.localeCompare(b.type)
  )

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
      <header className="mb-6">
        <h1 className="font-heading text-3xl font-bold">{displayName}</h1>
        <p className="mt-1 text-lg text-muted-foreground">
          <EraRange from={person.eraFrom} to={person.eraTo} />
          {person.highestOffice && <span> · {person.highestOffice}</span>}
          {person.isPatrician && (
            <Badge variant="secondary" className="ml-2 align-middle">
              Patrician
            </Badge>
          )}
          {person.isNobilis && (
            <Badge variant="secondary" className="ml-1 align-middle">
              Nobilis
            </Badge>
          )}
        </p>
      </header>

      <div className="lg:grid lg:grid-cols-[1fr_280px] lg:gap-8">
        <div className="min-w-0">
          <div className="mb-6 lg:hidden">
            <IdentityCard person={person} />
          </div>

          {person.postAssertions.length > 0 && (
            <Section title="Career" count={person.postAssertions.length}>
              <div className="space-y-4">
                {person.postAssertions.map((pa) => (
                  <OfficeEntry key={pa.id} assertion={pa} />
                ))}
              </div>
            </Section>
          )}

          {person.relationships.length > 0 && (
            <Section title="Relationships" count={person.relationships.length}>
              <div className="space-y-4">
                {groupRelationships(person.relationships).map(
                  ([type, rels]) => (
                    <div key={type}>
                      <p className="text-xs font-medium text-muted-foreground capitalize">
                        {type}
                      </p>
                      <div className="mt-1 space-y-2">
                        {rels.map((rel) => (
                          <RelationshipEntry key={rel.id} relationship={rel} />
                        ))}
                      </div>
                    </div>
                  )
                )}
              </div>
            </Section>
          )}

          {sortedNotes.length > 0 && (
            <Section title="Notes" count={sortedNotes.length}>
              <div className="space-y-4">
                {sortedNotes.map((note, i) => (
                  <NoteEntry key={i} note={note} />
                ))}
              </div>
            </Section>
          )}
        </div>

        <aside className="mt-8 lg:sticky lg:top-4 lg:mt-0 lg:self-start">
          <PersonRail person={person} />
        </aside>
      </div>
    </div>
  )
}

function OfficeEntry({ assertion }: { assertion: PostAssertion }) {
  return (
    <div className="border-l-2 pl-4">
      <p className="font-medium">
        <span className={assertion.isUncertain ? "italic" : undefined}>
          {assertion.officeName ? (
            <Link
              to="/offices/$slug"
              params={{ slug: slugify(assertion.officeName) }}
              className="hover:underline"
            >
              {assertion.officeName}
            </Link>
          ) : (
            assertion.officeName
          )}
          {assertion.isUncertain && "?"}
        </span>
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
          {(assertion.isDateStartUncertain || assertion.isDateEndUncertain) &&
            "?"}
        </p>
      )}
      {assertion.provinceOriginal && (
        <p className="text-sm text-muted-foreground">
          Location:{" "}
          {assertion.provinces.length > 0 ? (
            assertion.provinces.map((pr, i) => (
              <span key={pr}>
                {i > 0 && ", "}
                <Link
                  to="/provinces/$slug"
                  params={{ slug: slugify(pr) }}
                  className="hover:underline"
                >
                  {pr}
                </Link>
              </span>
            ))
          ) : (
            <span>{assertion.provinceOriginal}</span>
          )}
          {assertion.provinces.length > 0 &&
            assertion.provinces.join(", ") !== assertion.provinceOriginal && (
              <span className="italic"> ({assertion.provinceOriginal})</span>
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
      {assertion.notes.length > 0 && (
        <Collapsible>
          <CollapsibleTrigger className="mt-1 text-xs text-muted-foreground hover:underline">
            {assertion.notes.length} scholarly note
            {assertion.notes.length === 1 ? "" : "s"} ▸
          </CollapsibleTrigger>
          <CollapsibleContent>
            {assertion.notes.map((note, i) => (
              <div key={i} className="mt-2 rounded bg-muted/50 p-3 text-sm">
                <p className="mb-1 text-xs font-medium text-muted-foreground">
                  {note.type}
                  {note.secondarySource && ` — ${note.secondarySource}`}
                </p>
                <p className="leading-relaxed whitespace-pre-wrap">
                  {note.text}
                </p>
              </div>
            ))}
          </CollapsibleContent>
        </Collapsible>
      )}
    </div>
  )
}

function RelationshipEntry({ relationship }: { relationship: Relationship }) {
  return (
    <div className="text-sm">
      <p>
        <span className="text-muted-foreground capitalize">
          {relationship.relationshipType}
        </span>{" "}
        {relationship.relatedPersonId ? (
          <PersonLink
            id={relationship.relatedPersonId}
            name={relationship.relatedPersonName}
          />
        ) : (
          <span>
            {relationship.relatedPersonName.replace(/^[A-Z]{4}\d+ /, "")}
          </span>
        )}
        <SourceCitation
          name={relationship.secondarySource}
          className="ml-1 text-xs text-muted-foreground"
        />
      </p>
      {relationship.references.length > 0 && (
        <Collapsible>
          <CollapsibleTrigger className="mt-1 text-xs text-muted-foreground hover:underline">
            {relationship.references.length} reference
            {relationship.references.length === 1 ? "" : "s"} ▸
          </CollapsibleTrigger>
          <CollapsibleContent>
            {relationship.references.map((ref, i) =>
              ref.extraInfo || ref.secondarySource ? (
                <p key={i} className="mt-1 text-xs text-muted-foreground">
                  {ref.extraInfo && <>{ref.extraInfo} </>}
                  <SourceCitation name={ref.secondarySource} />
                </p>
              ) : null
            )}
          </CollapsibleContent>
        </Collapsible>
      )}
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
