// site/src/routes/offices.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getOfficeDetail } from "@/server/data"
import { DateDisplay, EraRange } from "@/components/date-display"
import { PersonLink } from "@/components/person-card"
import { SourceCitation } from "@/components/source-citation"

export const Route = createFileRoute("/offices/$slug")({
  loader: ({ params }) => getOfficeDetail({ data: params.slug }),
  head: ({ loaderData: office }) => {
    if (!office) return {}
    const title = `${office.name} — Offices — DPRR`
    const distinct = new Set(office.holders.map((h) => h.personId)).size
    const desc = `${office.holders.length} recorded tenures of the office of ${office.name}, held by ${distinct} persons of the Roman Republic`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: OfficePage,
})

function OfficePage() {
  const office = Route.useLoaderData()
  const distinct = new Set(office.holders.map((h) => h.personId)).size
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">
        {office.name}
        {office.abbreviation && (
          <span className="ml-2 text-xl font-normal text-muted-foreground">
            ({office.abbreviation})
          </span>
        )}
      </h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {office.holders.length} recorded tenures held by {distinct} persons,
        listed chronologically
      </p>
      <ol className="mt-6 space-y-2">
        {office.holders.map((h, i) => (
          <li
            key={`${h.personId}-${i}`}
            className="flex flex-wrap items-baseline gap-x-3 border-l-2 pl-4"
          >
            <span className="min-w-24 text-sm text-muted-foreground tabular-nums">
              {h.dateStart !== null && h.dateEnd !== null ? (
                h.dateStart === h.dateEnd ? (
                  <DateDisplay year={h.dateStart} />
                ) : (
                  <EraRange from={h.dateStart} to={h.dateEnd} />
                )
              ) : h.dateStart !== null || h.dateEnd !== null ? (
                <DateDisplay year={(h.dateStart ?? h.dateEnd) as number} />
              ) : (
                "undated"
              )}
            </span>
            <span className={h.isUncertain ? "italic" : undefined}>
              <PersonLink id={h.personId} name={h.personName} />
            </span>
            {h.isUncertain && <span className="text-muted-foreground">?</span>}
            <SourceCitation
              name={h.secondarySource}
              className="text-xs text-muted-foreground"
            />
          </li>
        ))}
      </ol>
    </div>
  )
}
