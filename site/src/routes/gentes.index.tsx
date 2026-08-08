// site/src/routes/gentes.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { fetchGensIndex } from "@/lib/static-data"

export const Route = createFileRoute("/gentes/")({
  loader: () => fetchGensIndex(),
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
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Gentes</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {gentes.length} gentes with known members
        </p>
      </header>
      <ul className="mt-2">
        {gentes.map((g) => (
          <li key={g.slug} className="contents">
            <Link
              to="/gentes/$slug"
              params={{ slug: g.slug }}
              className="ledger-row group flex items-baseline justify-between gap-2 px-1"
            >
              <span className="font-heading group-hover:text-accent-ink">
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
