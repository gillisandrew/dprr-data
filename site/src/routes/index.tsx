// site/src/routes/index.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getSearchData } from "@/server/data"
import { useSearchState } from "@/lib/search"
import { SearchInput } from "@/components/search-input"
import { ActiveFilterChips } from "@/components/active-filter-chips"
import { FacetSidebar } from "@/components/facet-sidebar"
import { ResultsList } from "@/components/results-list"
import { SITE_URL } from "@/lib/site"

export const Route = createFileRoute("/")({
  validateSearch: (search: Record<string, unknown>) => search,
  loader: () => getSearchData(),
  head: () => ({
    scripts: [
      {
        type: "application/ld+json",
        children: JSON.stringify({
          "@context": "https://schema.org",
          "@type": "Dataset",
          name: "Digital Prosopography of the Roman Republic",
          description:
            "Prosopographical data for 4,876 persons of the Roman Republic (509–31 BC): offices held, relationships, dates, and sources.",
          url: SITE_URL,
        }),
      },
    ],
  }),
  component: SearchPage,
})

function SearchPage() {
  const { summaries, searchIndex, officeHierarchy, provinceHierarchy } =
    Route.useLoaderData()
  const { state, results, facets, updateState, clearAll } = useSearchState(
    summaries,
    searchIndex
  )

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <header className="mb-6">
        <h1 className="font-heading text-2xl font-bold">
          Digital Prosopography of the Roman Republic
        </h1>
        <p className="text-sm text-muted-foreground">
          Search and browse {summaries.length.toLocaleString()} persons (509–31
          BC)
        </p>
      </header>

      <SearchInput value={state.q} onChange={(q) => updateState({ q })} />

      <div className="mt-3">
        <ActiveFilterChips
          state={state}
          onRemove={updateState}
          onClearAll={clearAll}
        />
      </div>

      <div className="mt-4 flex gap-6">
        <FacetSidebar
          facets={facets}
          state={state}
          onUpdate={updateState}
          officeHierarchy={officeHierarchy}
          provinceHierarchy={provinceHierarchy}
        />
        <main className="min-w-0 flex-1">
          <ResultsList results={results} />
        </main>
      </div>

      {/* Hidden links for static prerender crawler */}
      <div className="hidden" aria-hidden="true">
        {summaries.map((p) => (
          <a key={p.id} href={`${import.meta.env.BASE_URL}persons/${p.id}`}>
            {p.id}
          </a>
        ))}
      </div>
    </div>
  )
}
