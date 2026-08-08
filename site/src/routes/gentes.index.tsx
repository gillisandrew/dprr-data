// site/src/routes/gentes.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getGensIndex } from "@/server/data"

export const Route = createFileRoute("/gentes/")({
  loader: () => getGensIndex(),
  head: () => ({
    meta: [
      { title: "Gentes — DPRR" },
      {
        name: "description",
        content: "Roman gentes (families) with all known members",
      },
    ],
  }),
  component: GentesPage,
})

function GentesPage() {
  const gentes = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">Gentes</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {gentes.length} gentes with known members
      </p>
      <ul className="mt-6 space-y-1">
        {gentes.map((g) => (
          <li key={g.slug}>
            <Link
              to="/gentes/$slug"
              params={{ slug: g.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
                {g.name}
              </span>
              <span className="text-sm text-muted-foreground">
                {g.memberCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
