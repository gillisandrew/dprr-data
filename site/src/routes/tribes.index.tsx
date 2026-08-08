// site/src/routes/tribes.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getTribeIndex } from "@/server/data"

export const Route = createFileRoute("/tribes/")({
  loader: () => getTribeIndex(),
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
      <div className="mt-2">
        {tribes.map((t) => (
          <Link
            key={t.slug}
            to="/tribes/$slug"
            params={{ slug: t.slug }}
            className="ledger-row group flex items-baseline justify-between gap-2 px-1"
          >
            <span className="font-heading group-hover:text-primary">
              {t.name}
            </span>
            <span className="text-sm text-muted-foreground">
              {t.memberCount}
            </span>
          </Link>
        ))}
      </div>
    </div>
  )
}
