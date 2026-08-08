// site/src/routes/offices.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getOfficeIndex } from "@/server/data"
import type { OfficeIndexEntry } from "@/data/aggregate-references"

const CATEGORY_ORDER = [
  "Magisterial Posts",
  "Promagisterial Posts",
  "Priesthoods",
  "Non-magisterial Posts",
  "Equestrian Functions",
  "Distinctions",
]

function groupByCategory(offices: OfficeIndexEntry[]) {
  const groups = new Map<string, OfficeIndexEntry[]>()
  for (const o of offices) {
    const list = groups.get(o.category) ?? []
    list.push(o)
    groups.set(o.category, list)
  }
  return [...groups].sort(
    (a, b) =>
      (CATEGORY_ORDER.indexOf(a[0]) + 1 || 99) -
        (CATEGORY_ORDER.indexOf(b[0]) + 1 || 99) || a[0].localeCompare(b[0])
  )
}

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
  const groups = groupByCategory(offices)
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Offices</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {offices.length} offices and priesthoods
        </p>
      </header>
      {groups.map(([category, entries]) => (
        <section key={category} className="mt-7">
          <h2 className="micro-label rule-hair pb-1">{category}</h2>
          <div className="mt-2">
            {entries.map((o) => (
              <Link
                key={o.slug}
                to="/offices/$slug"
                params={{ slug: o.slug }}
                className="ledger-row group flex items-baseline justify-between gap-2 px-1"
              >
                <span className="font-heading group-hover:text-primary">
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
            ))}
          </div>
        </section>
      ))}
    </div>
  )
}
