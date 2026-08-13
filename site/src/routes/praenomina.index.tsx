// site/src/routes/praenomina.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { fetchPraenomenIndex } from "@/lib/static-data"
import { InfoHint } from "@/components/info-hint"

export const Route = createFileRoute("/praenomina/")({
  loader: () => fetchPraenomenIndex(),
  head: () => ({
    meta: [
      { title: "Praenomina — DPRR" },
      {
        name: "description",
        content: "Roman personal names, their abbreviations, and who bore them",
      },
    ],
  }),
  component: PraenominaPage,
})

function PraenominaPage() {
  const praenomina = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">
          Praenomina
          <InfoHint term="roman-names" />
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {praenomina.length} personal names, with the abbreviations used in
          filiations
        </p>
      </header>
      <ul className="mt-2">
        {praenomina.map((p) => (
          <li key={p.slug} className="contents">
            <Link
              to="/praenomina/$slug"
              params={{ slug: p.slug }}
              className="ledger-row group flex items-baseline justify-between gap-3 px-1"
            >
              <span>
                <span className="font-heading group-hover:text-accent-ink">
                  {p.name}
                </span>
                {p.abbreviation && (
                  <span className="small-caps ml-2 text-xs text-muted-foreground">
                    {p.abbreviation}
                  </span>
                )}
              </span>
              <span className="shrink-0 text-sm text-muted-foreground">
                {p.personCount.toLocaleString()}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
