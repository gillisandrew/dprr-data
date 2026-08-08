// site/src/routes/gentes.$slug.tsx
import { createFileRoute, notFound } from "@tanstack/react-router"
import { fetchGensDetail, StaticDataError } from "@/lib/static-data"
import { PersonCard } from "@/components/person-card"

export const Route = createFileRoute("/gentes/$slug")({
  loader: async ({ params }) => {
    try {
      return await fetchGensDetail(params.slug)
    } catch (err) {
      if (err instanceof StaticDataError && err.status === 404) {
        throw notFound()
      }
      throw err
    }
  },
  head: ({ loaderData: gens }) => {
    if (!gens) return {}
    const title = `${gens.name} — Gentes — DPRR`
    const desc = `${gens.members.length} known members of the gens ${gens.name}`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: GensPage,
})

function GensPage() {
  const gens = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">{gens.name}</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {gens.members.length} known members
        </p>
      </header>
      <div className="mt-2">
        {gens.members.map((m) => (
          <PersonCard key={m.id} person={m} />
        ))}
      </div>
    </div>
  )
}
