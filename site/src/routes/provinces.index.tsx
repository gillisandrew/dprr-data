// site/src/routes/provinces.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getProvinceIndex } from "@/server/data"

export const Route = createFileRoute("/provinces/")({
  loader: () => getProvinceIndex(),
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
      <h1 className="font-heading text-3xl font-bold">Locations</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {provinces.length} provinces with recorded office holders
      </p>
      <ul className="mt-6 space-y-1">
        {provinces.map((p) => (
          <li key={p.slug}>
            <Link
              to="/provinces/$slug"
              params={{ slug: p.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
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
