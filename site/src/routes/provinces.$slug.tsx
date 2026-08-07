// site/src/routes/provinces.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getProvinceDetail } from "@/server/data"
import { DateDisplay, EraRange } from "@/components/date-display"
import { PersonLink } from "@/components/person-card"

export const Route = createFileRoute("/provinces/$slug")({
  loader: ({ params }) => getProvinceDetail({ data: params.slug }),
  head: ({ loaderData: province }) => {
    if (!province) return {}
    const title = `${province.name} — Provinces — DPRR`
    const distinct = new Set(province.assertions.map((a) => a.personId)).size
    const desc = `${province.assertions.length} recorded assignments in ${province.name}, held by ${distinct} persons of the Roman Republic`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: ProvincePage,
})

function ProvincePage() {
  const province = Route.useLoaderData()
  const distinct = new Set(province.assertions.map((a) => a.personId)).size
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">{province.name}</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {province.assertions.length} recorded assignments held by {distinct}{" "}
        persons, listed chronologically
      </p>
      <ol className="mt-6 space-y-2">
        {province.assertions.map((a, i) => (
          <li
            key={`${a.personId}-${i}`}
            className="flex flex-wrap items-baseline gap-x-3 border-l-2 pl-4"
          >
            <span className="min-w-24 text-sm text-muted-foreground tabular-nums">
              {a.dateStart !== null && a.dateEnd !== null ? (
                a.dateStart === a.dateEnd ? (
                  <DateDisplay year={a.dateStart} />
                ) : (
                  <EraRange from={a.dateStart} to={a.dateEnd} />
                )
              ) : a.dateStart !== null || a.dateEnd !== null ? (
                <DateDisplay year={(a.dateStart ?? a.dateEnd) as number} />
              ) : (
                "undated"
              )}
            </span>
            <span className={a.isUncertain ? "italic" : undefined}>
              <PersonLink id={a.personId} name={a.personName} />
            </span>
            {a.isUncertain && <span className="text-muted-foreground">?</span>}
            {a.officeName && (
              <span className="text-sm text-muted-foreground">
                {a.officeName}
              </span>
            )}
          </li>
        ))}
      </ol>
    </div>
  )
}
