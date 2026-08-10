// site/src/routes/tribes.$slug.tsx
import { createFileRoute, notFound } from "@tanstack/react-router"
import { fetchTribeDetail, StaticDataError } from "@/lib/static-data"
import { PersonCard } from "@/components/person-card"
import { ReportIssueLink } from "@/components/report-issue-link"
import { REFERENCE_TTL } from "@/lib/report-issue"

export const Route = createFileRoute("/tribes/$slug")({
  loader: async ({ params }) => {
    try {
      return await fetchTribeDetail(params.slug)
    } catch (err) {
      if (err instanceof StaticDataError && err.status === 404) {
        throw notFound()
      }
      throw err
    }
  },
  head: ({ loaderData: tribe }) => {
    if (!tribe) return {}
    const title = `${tribe.name} — Tribes — DPRR`
    const desc = `${tribe.members.length} known members of the tribus ${tribe.name}`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: TribePage,
})

function TribePage() {
  const tribe = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">{tribe.name}</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {tribe.members.length} known members
        </p>
      </header>
      <div className="mt-2">
        {tribe.members.map((m) => (
          <PersonCard key={m.id} person={m} />
        ))}
      </div>
      <ReportIssueLink
        entityLabel={`Tribe: ${tribe.name}`}
        ttlPath={REFERENCE_TTL.tribe}
      />
    </div>
  )
}
