// site/src/routes/persons.$id.tsx
import { createFileRoute, Link, notFound } from "@tanstack/react-router"
import { fetchPerson, StaticDataError } from "@/lib/static-data"
import { slugify } from "@/lib/slug"
import { displayName } from "@/lib/order"
import { SITE_URL } from "@/lib/site"
import { cn } from "@/lib/utils"
import { Section } from "@/components/section"
import { PersonRegistry } from "@/components/person-registry"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { DateDisplay, EraRange } from "@/components/date-display"
import { formatYearWithInterval } from "@/lib/dates"
import { SourceCitation } from "@/components/source-citation"
import { PersonLink } from "@/components/person-card"
import { ReportIssueLink } from "@/components/report-issue-link"
import { personTtlPath } from "@/lib/report-issue"
import type {
  PostAssertion,
  StatusAssertion,
  Note,
  Relationship,
  DateInfo,
  Concordance,
} from "@/data/types"

/** Groups relationships by type in DPRR's curated order (hasOrderNumber,
 * alphabetical fallback); members by hasRelationshipNumber, then name. */
function groupRelationships(rels: Relationship[]): [string, Relationship[]][] {
  const byType = new Map<string, Relationship[]>()
  for (const r of rels) {
    const list = byType.get(r.relationshipType) ?? []
    list.push(r)
    byType.set(r.relationshipType, list)
  }
  const orderOf = (list: Relationship[]) =>
    list[0].typeOrderNumber ?? Number.MAX_SAFE_INTEGER
  return [...byType]
    .sort((a, b) => orderOf(a[1]) - orderOf(b[1]) || a[0].localeCompare(b[0]))
    .map(([type, list]) => [
      type,
      [...list].sort(
        (a, b) =>
          (a.relationshipNumber ?? Number.MAX_SAFE_INTEGER) -
            (b.relationshipNumber ?? Number.MAX_SAFE_INTEGER) ||
          displayName(a.relatedPersonName).localeCompare(
            displayName(b.relatedPersonName)
          )
      ),
    ])
}

export const Route = createFileRoute("/persons/$id")({
  loader: async ({ params }) => {
    try {
      return await fetchPerson(params.id)
    } catch (err) {
      if (err instanceof StaticDataError && err.status === 404) {
        throw notFound()
      }
      throw err
    }
  },
  head: ({ loaderData: person }) => {
    if (!person) return {}
    const name = displayName(person.name)
    const desc = [person.highestOffice, ...person.statuses]
      .filter(Boolean)
      .join(" · ")
    const jsonLd = {
      "@context": "https://schema.org",
      "@type": "Person",
      name,
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
        { title: `${name} (${person.id}) — DPRR` },
        { name: "description", content: desc },
        { property: "og:title", content: `${name} — DPRR` },
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
  const name = displayName(person.name)
  const sortedNotes = [...person.personNotes].sort((a, b) =>
    a.type.localeCompare(b.type)
  )
  const sortedDates = [...person.dateInformation].sort(
    (a, b) => a.value - b.value
  )
  const groupedConcordances = groupConcordances(person.concordances)

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">{name}</h1>
        <p className="mt-1 text-lg text-muted-foreground">
          {person.highestOffice && (
            <span className="text-accent-ink">{person.highestOffice}</span>
          )}
          {person.highestOffice && " · "}
          <EraRange from={person.eraFrom} to={person.eraTo} />
          {person.statuses.map((status, i) => (
            <span
              key={status}
              className={`small-caps ${i === 0 ? "ml-2" : "ml-1"} text-muted-foreground`}
            >
              {status}
            </span>
          ))}
        </p>
      </header>

      <PersonRegistry person={person} />

      {person.nobilisNotes && (
        <p className="mt-2 text-sm text-muted-foreground italic">
          {person.nobilisNotes}
        </p>
      )}
      {person.novusNotes && (
        <p className="mt-2 text-sm text-muted-foreground italic">
          {person.novusNotes}
        </p>
      )}

      {person.postAssertions.length > 0 && (
        <Section
          title="Career"
          count={person.postAssertions.length}
          hint="broughton-label"
        >
          <div>
            {person.postAssertions.map((pa) => (
              <OfficeEntry key={pa.id} assertion={pa} />
            ))}
          </div>
        </Section>
      )}

      {person.statusAssertions.length > 0 && (
        <Section
          title="Status"
          count={person.statusAssertions.length}
          hint="status"
        >
          <div>
            {person.statusAssertions.map((sa) => (
              <StatusEntry key={sa.id} assertion={sa} />
            ))}
          </div>
        </Section>
      )}

      {person.relationships.length > 0 && (
        <Section title="Relationships" count={person.relationships.length}>
          <div className="space-y-4">
            {groupRelationships(person.relationships).map(([type, rels]) => (
              <div key={type}>
                <p className="text-xs font-medium text-muted-foreground capitalize">
                  {type}
                </p>
                <div className="mt-1">
                  {rels.map((rel) => (
                    <RelationshipEntry key={rel.id} relationship={rel} />
                  ))}
                </div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {sortedDates.length > 0 && (
        <Section title="Dates" count={sortedDates.length} hint="life-events">
          <div>
            {sortedDates.map((d, i) => (
              <DateEntry key={i} dateInfo={d} />
            ))}
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

      {groupedConcordances.length > 0 && (
        <Section title="Links">
          <dl>
            {groupedConcordances.map(([system, links]) => (
              <div
                key={system}
                className="ledger-row flex flex-wrap items-baseline gap-x-3 gap-y-1"
              >
                <dt className="w-28 shrink-0 text-sm text-muted-foreground capitalize">
                  {system}
                </dt>
                <dd className="flex min-w-0 flex-1 flex-col gap-1">
                  {links.map((link, i) => (
                    <a
                      key={i}
                      href={link.uri}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="break-all text-accent-ink hover:underline"
                    >
                      {link.uri}
                    </a>
                  ))}
                </dd>
              </div>
            ))}
          </dl>
        </Section>
      )}

      <ReportIssueLink
        entityLabel={`${person.id} — ${name}`}
        ttlPath={personTtlPath(person.id)}
      />
    </div>
  )
}

/** Groups concordances by system (alphabetical). */
function groupConcordances(
  concordances: Concordance[]
): [string, Concordance[]][] {
  const grouped = new Map<string, Concordance[]>()
  for (const c of concordances) {
    const existing = grouped.get(c.system) ?? []
    existing.push(c)
    grouped.set(c.system, existing)
  }
  return [...grouped].sort((a, b) => a[0].localeCompare(b[0]))
}

function DateEntry({ dateInfo }: { dateInfo: DateInfo }) {
  return (
    <div className="ledger-row flex gap-3">
      <span className="year-col text-sm">
        {formatYearWithInterval(
          dateInfo.value,
          dateInfo.interval,
          dateInfo.isUncertain
        )}
      </span>
      <div className="min-w-0 flex-1 text-sm">
        <span className="text-muted-foreground capitalize">
          {dateInfo.type}
        </span>
        {dateInfo.notes && (
          <span className="text-muted-foreground"> — {dateInfo.notes}</span>
        )}
        <SourceCitation
          name={dateInfo.secondarySource}
          className="ml-1 text-xs text-muted-foreground"
        />
      </div>
    </div>
  )
}

function StatusEntry({ assertion }: { assertion: StatusAssertion }) {
  return (
    <div className="ledger-row flex gap-3">
      <span className="year-col text-sm">
        {(assertion.dateStart !== null || assertion.dateEnd !== null) && (
          <>
            {assertion.dateStart !== null &&
            assertion.dateEnd !== null &&
            assertion.dateStart !== assertion.dateEnd ? (
              <EraRange from={assertion.dateStart} to={assertion.dateEnd} />
            ) : (
              <DateDisplay
                year={(assertion.dateStart ?? assertion.dateEnd) as number}
              />
            )}
            {(assertion.isDateStartUncertain || assertion.isDateEndUncertain) &&
              "?"}
          </>
        )}
      </span>
      <div className="min-w-0 flex-1 text-sm">
        <span className={cn("small-caps", assertion.isUncertain && "italic")}>
          {assertion.statusName}
          {assertion.isUncertain && "?"}
        </span>
        {assertion.notes.map((note, i) => (
          <p key={i} className="text-sm text-muted-foreground">
            {note.text}
          </p>
        ))}
        <SourceCitation
          name={assertion.secondarySource}
          className="mt-1 block text-xs text-muted-foreground"
        />
      </div>
    </div>
  )
}

function OfficeEntry({ assertion }: { assertion: PostAssertion }) {
  return (
    <div className="ledger-row flex gap-3">
      <span className="year-col text-sm">
        {(assertion.dateStart || assertion.dateEnd) && (
          <>
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
          </>
        )}
      </span>
      <div className="min-w-0 flex-1">
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
          {assertion.officeXref && (
            <span className="ml-1 text-sm text-muted-foreground italic">
              {assertion.officeXref}
            </span>
          )}
        </p>
        {assertion.provinceOriginal && (
          <p className="text-sm text-muted-foreground">
            Provincia:{" "}
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
        {(assertion.dateSourceText ||
          (assertion.dateSecondarySource &&
            assertion.dateSecondarySource !== assertion.secondarySource)) && (
          <p className="text-xs text-muted-foreground italic">
            {assertion.dateSourceText
              ? `date: ${assertion.dateSourceText}`
              : "date"}
            {assertion.dateSecondarySource &&
              assertion.dateSecondarySource !== assertion.secondarySource && (
                <> per {assertion.dateSecondarySource}</>
              )}
          </p>
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
    </div>
  )
}

function RelationshipEntry({ relationship }: { relationship: Relationship }) {
  return (
    <div className="ledger-row text-sm">
      <p>
        <span className="text-muted-foreground capitalize">
          {relationship.relationshipType}
        </span>{" "}
        <span className={relationship.isUncertain ? "italic" : undefined}>
          {relationship.relatedPersonId ? (
            <PersonLink
              id={relationship.relatedPersonId}
              name={relationship.relatedPersonName}
            />
          ) : (
            <span>{displayName(relationship.relatedPersonName)}</span>
          )}
          {relationship.isUncertain && "?"}
        </span>
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
