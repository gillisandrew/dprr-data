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
      <h1 className="font-heading text-3xl font-bold">Tribes</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {tribes.length} voting tribes with known members
      </p>
      <ul className="mt-6 space-y-1">
        {tribes.map((t) => (
          <li key={t.slug}>
            <Link
              to="/tribes/$slug"
              params={{ slug: t.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
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
