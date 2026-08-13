// site/src/routes/praenomina.$slug.tsx
import { createFileRoute, notFound } from "@tanstack/react-router"
import { fetchPraenomenDetail, StaticDataError } from "@/lib/static-data"
import { ResultsList } from "@/components/results-list"
import { ReportIssueLink } from "@/components/report-issue-link"
import { REFERENCE_TTL } from "@/lib/report-issue"

export const Route = createFileRoute("/praenomina/$slug")({
  loader: async ({ params }) => {
    try {
      return await fetchPraenomenDetail(params.slug)
    } catch (err) {
      if (err instanceof StaticDataError && err.status === 404) {
        throw notFound()
      }
      throw err
    }
  },
  head: ({ loaderData: praenomen }) => {
    if (!praenomen) return {}
    const title = `${praenomen.name} — Praenomina — DPRR`
    const desc = `${praenomen.persons.length} persons bearing the praenomen ${praenomen.name}`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: PraenomenPage,
})

function PraenomenPage() {
  const praenomen = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">
          {praenomen.name}
          {praenomen.abbreviation && (
            <span className="ml-2 text-xl font-normal text-muted-foreground">
              ({praenomen.abbreviation})
            </span>
          )}
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {praenomen.persons.length.toLocaleString()} persons bear this
          praenomen
        </p>
      </header>
      {praenomen.persons.length === 0 ? (
        <p className="py-8 text-center text-sm text-muted-foreground">
          No person record bears this praenomen.
        </p>
      ) : (
        <div className="mt-2">
          <ResultsList results={praenomen.persons} />
        </div>
      )}
      <ReportIssueLink
        entityLabel={`Praenomen: ${praenomen.name}`}
        ttlPath={REFERENCE_TTL.praenomen}
      />
    </div>
  )
}
