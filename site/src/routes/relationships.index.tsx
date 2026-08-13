// site/src/routes/relationships.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { fetchRelationshipIndex } from "@/lib/static-data"

export const Route = createFileRoute("/relationships/")({
  loader: () => fetchRelationshipIndex(),
  head: () => ({
    meta: [
      { title: "Relationships — DPRR" },
      {
        name: "description",
        content:
          "Kinship and association types recorded between persons, in DPRR's curated order",
      },
    ],
  }),
  component: RelationshipsPage,
})

function RelationshipsPage() {
  const relationships = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Relationships</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {relationships.length} kinship and association types, in DPRR's
          curated order
        </p>
      </header>
      <ul className="mt-2">
        {relationships.map((r) => (
          <li key={r.slug} className="contents">
            <Link
              to="/relationships/$slug"
              params={{ slug: r.slug }}
              className="ledger-row group flex items-baseline justify-between gap-3 px-1"
            >
              <span>
                <span className="font-heading group-hover:text-accent-ink">
                  {r.name}
                </span>
                {r.inverseName && (
                  <span className="ml-2 text-xs text-muted-foreground">
                    ↔ {r.inverseName}
                  </span>
                )}
              </span>
              <span className="shrink-0 text-sm text-muted-foreground">
                {r.pairCount.toLocaleString()}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
