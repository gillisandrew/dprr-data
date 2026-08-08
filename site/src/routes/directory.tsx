// site/src/routes/directory.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getAllPersonIds } from "@/server/data"

export const Route = createFileRoute("/directory")({
  loader: () => getAllPersonIds(),
  head: () => ({
    meta: [
      { title: "Directory — DPRR" },
      { name: "description", content: "Complete index of all person records" },
    ],
  }),
  component: DirectoryPage,
})

function DirectoryPage() {
  const ids = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-6xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">Directory</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Complete index of {ids.length.toLocaleString()} person records —{" "}
          <Link to="/offices" className="underline">
            offices
          </Link>
          ,{" "}
          <Link to="/tribes" className="underline">
            tribes
          </Link>
          , and{" "}
          <Link to="/provinces" className="underline">
            locations
          </Link>{" "}
          have their own indexes.
        </p>
      </header>
      <ul className="mt-2 columns-3 gap-4 text-xs sm:columns-5 lg:columns-8">
        {ids.map((id) => (
          <li key={id} className="ledger-row">
            <Link
              to="/persons/$id"
              params={{ id }}
              className="block px-1 py-1 hover:text-primary"
            >
              {id}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
