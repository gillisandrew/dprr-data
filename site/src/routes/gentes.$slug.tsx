// site/src/routes/gentes.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getGensDetail } from "@/server/data"
import { PersonCard } from "@/components/person-card"

export const Route = createFileRoute("/gentes/$slug")({
  loader: ({ params }) => getGensDetail({ data: params.slug }),
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
      <h1 className="font-heading text-3xl font-bold">{gens.name}</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {gens.members.length} known members
      </p>
      <div className="mt-6 space-y-2">
        {gens.members.map((m) => (
          <PersonCard key={m.id} person={m} />
        ))}
      </div>
    </div>
  )
}
