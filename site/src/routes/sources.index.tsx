// site/src/routes/sources.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { fetchSourceIndex } from "@/lib/static-data"

export const Route = createFileRoute("/sources/")({
  loader: () => fetchSourceIndex(),
  head: () => ({
    meta: [
      { title: "Sources — DPRR" },
      {
        name: "description",
        content:
          "Secondary scholarship cited by DPRR, and the persons each work documents",
      },
    ],
  }),
  component: SourcesPage,
})

function SourcesPage() {
  const sources = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Sources</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {sources.length} works of secondary scholarship underpinning the
          record
        </p>
      </header>
      <ul className="mt-2">
        {sources.map((s) => (
          <li key={s.slug} className="contents">
            <Link
              to="/sources/$slug"
              params={{ slug: s.slug }}
              className="ledger-row group flex items-baseline justify-between gap-3 px-1"
            >
              <span className="min-w-0">
                <span className="font-heading group-hover:text-accent-ink">
                  {s.name}
                </span>
                {s.abbreviation && (
                  <span className="small-caps ml-2 text-xs text-muted-foreground">
                    {s.abbreviation}
                  </span>
                )}
              </span>
              <span className="shrink-0 text-sm text-muted-foreground">
                {s.personCount.toLocaleString()}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
