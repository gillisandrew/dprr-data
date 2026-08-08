// site/src/routes/tribes.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { fetchTribeIndex } from "@/lib/static-data"

export const Route = createFileRoute("/tribes/")({
  loader: () => fetchTribeIndex(),
  head: () => ({
    meta: [
      { title: "Tribes — DPRR" },
      {
        name: "description",
        content: "Voting tribes of the Roman Republic and their known members",
      },
    ],
  }),
  component: TribesPage,
})

function TribesPage() {
  const tribes = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Tribes</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {tribes.length} voting tribes with known members
        </p>
      </header>
      <ul className="mt-2">
        {tribes.map((t) => (
          <li key={t.slug} className="contents">
            <Link
              to="/tribes/$slug"
              params={{ slug: t.slug }}
              className="ledger-row group flex items-baseline justify-between gap-2 px-1"
            >
              <span className="font-heading group-hover:text-accent-ink">
                {t.name}
              </span>
              <span className="text-sm text-muted-foreground">
                {t.memberCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
