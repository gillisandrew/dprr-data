// site/src/routes/sources.$slug.tsx
import { createFileRoute, notFound } from "@tanstack/react-router"
import { fetchSourceDetail, StaticDataError } from "@/lib/static-data"
import { ResultsList } from "@/components/results-list"
import { ReportIssueLink } from "@/components/report-issue-link"
import { REFERENCE_TTL } from "@/lib/report-issue"

export const Route = createFileRoute("/sources/$slug")({
  loader: async ({ params }) => {
    try {
      return await fetchSourceDetail(params.slug)
    } catch (err) {
      if (err instanceof StaticDataError && err.status === 404) {
        throw notFound()
      }
      throw err
    }
  },
  head: ({ loaderData: source }) => {
    if (!source) return {}
    const title = `${source.abbreviation ?? source.name} — Sources — DPRR`
    const desc = `${source.persons.length} persons documented by ${source.name}`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: SourcePage,
})

function SourcePage() {
  const source = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">
          {source.name}
          {source.abbreviation && (
            <span className="ml-2 text-xl font-normal text-muted-foreground">
              ({source.abbreviation})
            </span>
          )}
        </h1>
        {source.biblio && (
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            {source.biblio}
          </p>
        )}
        <p className="mt-1 text-sm text-muted-foreground">
          {source.persons.length.toLocaleString()} persons cite this work
        </p>
      </header>
      {source.persons.length === 0 ? (
        <p className="py-8 text-center text-sm text-muted-foreground">
          No person record cites this work.
        </p>
      ) : (
        <div className="mt-2">
          <ResultsList results={source.persons} />
        </div>
      )}
      <ReportIssueLink
        entityLabel={`Source: ${source.name}`}
        ttlPath={REFERENCE_TTL.source}
      />
    </div>
  )
}
