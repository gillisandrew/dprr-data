// site/src/routes/tribes.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getTribeDetail } from "@/server/data"
import { PersonCard } from "@/components/person-card"

export const Route = createFileRoute("/tribes/$slug")({
  loader: ({ params }) => getTribeDetail({ data: params.slug }),
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
      <h1 className="font-heading text-3xl font-bold">{tribe.name}</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {tribe.members.length} known members
      </p>
      <div className="mt-6 space-y-2">
        {tribe.members.map((m) => (
          <PersonCard key={m.id} person={m} />
        ))}
      </div>
    </div>
  )
}
