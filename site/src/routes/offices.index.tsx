// site/src/routes/offices.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getOfficeIndex } from "@/server/data"

export const Route = createFileRoute("/offices/")({
  loader: () => getOfficeIndex(),
  head: () => ({
    meta: [
      { title: "Offices — DPRR" },
      {
        name: "description",
        content:
          "Offices and priesthoods of the Roman Republic, with all known holders",
      },
    ],
  }),
  component: OfficesPage,
})

function OfficesPage() {
  const offices = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">Offices</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {offices.length} offices and priesthoods
      </p>
      <ul className="mt-6 space-y-1">
        {offices.map((o) => (
          <li key={o.slug}>
            <Link
              to="/offices/$slug"
              params={{ slug: o.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
                {o.name}
                {o.abbreviation && (
                  <span className="ml-1 text-sm font-normal text-muted-foreground">
                    ({o.abbreviation})
                  </span>
                )}
              </span>
              <span className="text-sm text-muted-foreground">
                {o.holderCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
