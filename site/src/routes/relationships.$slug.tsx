// site/src/routes/relationships.$slug.tsx
import { Link, createFileRoute, notFound } from "@tanstack/react-router"
import { fetchRelationshipDetail, StaticDataError } from "@/lib/static-data"
import { ReportIssueLink } from "@/components/report-issue-link"
import { REFERENCE_TTL } from "@/lib/report-issue"
import { displayName } from "@/lib/order"

export const Route = createFileRoute("/relationships/$slug")({
  loader: async ({ params }) => {
    try {
      return await fetchRelationshipDetail(params.slug)
    } catch (err) {
      if (err instanceof StaticDataError && err.status === 404) {
        throw notFound()
      }
      throw err
    }
  },
  head: ({ loaderData: rel }) => {
    if (!rel) return {}
    const title = `${rel.name} — Relationships — DPRR`
    const desc = `${rel.pairs.length} recorded "${rel.name}" links between persons`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: RelationshipPage,
})

function RelationshipPage() {
  const rel = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">{rel.name}</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {rel.pairs.length.toLocaleString()} recorded links
          {rel.inverseName && <> · inverse of {rel.inverseName}</>}
        </p>
      </header>
      {rel.pairs.length === 0 ? (
        <p className="py-8 text-center text-sm text-muted-foreground">
          No person record uses this relationship type.
        </p>
      ) : (
        <ul className="mt-2">
          {rel.pairs.map((pair) => (
            <li
              key={`${pair.personId}|${pair.relatedPersonId}`}
              className="ledger-row flex flex-wrap items-baseline gap-x-2 px-1 text-sm"
            >
              <Link
                to="/persons/$id"
                params={{ id: pair.personId }}
                className="font-heading hover:text-accent-ink"
              >
                {displayName(pair.personName)}
              </Link>
              <span className={pair.isUncertain ? "italic" : undefined}>
                <span className="text-muted-foreground">{rel.name}</span>
                {pair.isUncertain && (
                  <span className="text-muted-foreground">?</span>
                )}
              </span>
              <Link
                to="/persons/$id"
                params={{ id: pair.relatedPersonId }}
                className="font-heading hover:text-accent-ink"
              >
                {displayName(pair.relatedPersonName)}
              </Link>
            </li>
          ))}
        </ul>
      )}
      <ReportIssueLink
        entityLabel={`Relationship: ${rel.name}`}
        ttlPath={REFERENCE_TTL.relationship}
      />
    </div>
  )
}
