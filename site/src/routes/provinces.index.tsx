// site/src/routes/provinces.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { fetchProvinceIndex } from "@/lib/static-data"

export const Route = createFileRoute("/provinces/")({
  loader: () => fetchProvinceIndex(),
  head: () => ({
    meta: [
      { title: "Locations — DPRR" },
      {
        name: "description",
        content:
          "Locations — provinces, courts, and spheres of responsibility with recorded office holders",
      },
    ],
  }),
  component: ProvincesPage,
})

function ProvincesPage() {
  const provinces = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Locations</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {provinces.length} provinces with recorded office holders
        </p>
      </header>
      <ul className="mt-2">
        {provinces.map((p) => (
          <li key={p.slug} className="contents">
            <Link
              to="/provinces/$slug"
              params={{ slug: p.slug }}
              className="ledger-row group flex items-baseline justify-between gap-2 px-1"
            >
              <span className="font-heading group-hover:text-accent-ink">
                {p.name}
              </span>
              <span className="text-sm text-muted-foreground">
                {p.personCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
