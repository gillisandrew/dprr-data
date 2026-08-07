// site/src/routes/index.tsx
import { useEffect, useState } from "react"
import { createFileRoute } from "@tanstack/react-router"
import { parseSearchParams, toSearchParams, useSearchState } from "@/lib/search"
import { useSearchData, type SearchDataBundle } from "@/lib/use-search-data"
import { SearchInput } from "@/components/search-input"
import { ActiveFilterChips } from "@/components/active-filter-chips"
import { FacetSidebar } from "@/components/facet-sidebar"
import { ResultsList } from "@/components/results-list"
import { SearchLanding } from "@/components/search-landing"
import { SITE_URL } from "@/lib/site"

type Focus = "office" | "time" | "gens"

export const Route = createFileRoute("/")({
  validateSearch: (search: Record<string, unknown>) => search,
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
  const rawParams = Route.useSearch() as Record<string, string>
  const hasParams =
    Object.keys(toSearchParams(parseSearchParams(rawParams))).length > 0
  const [interacted, setInteracted] = useState(false)
  const [initialFocus, setInitialFocus] = useState<Focus | undefined>()
  const [pendingQuery, setPendingQuery] = useState<string | null>(null)
  const showLanding = !hasParams && !interacted
  const { bundle, error } = useSearchData(!showLanding)

  const content = showLanding ? (
    <SearchLanding
      onSearch={(q) => {
        setPendingQuery(q)
        setInteracted(true)
      }}
      onBrowse={(focus) => {
        setInitialFocus(focus)
        setInteracted(true)
      }}
    />
  ) : error ? (
    <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
      Failed to load search data — please reload. ({error})
    </div>
  ) : !bundle ? (
    <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
      Loading search data…
    </div>
  ) : (
    <SearchResults
      bundle={bundle}
      initialFocus={initialFocus}
      pendingQuery={pendingQuery}
      onPendingApplied={() => setPendingQuery(null)}
    />
  )

  return <div className="mx-auto max-w-6xl px-4 py-6">{content}</div>
}

function SearchResults({
  bundle,
  initialFocus,
  pendingQuery,
  onPendingApplied,
}: {
  bundle: SearchDataBundle
  initialFocus: Focus | undefined
  pendingQuery: string | null
  onPendingApplied: () => void
}) {
  const { state, results, facets, updateState, clearAll, filteredHistogram } =
    useSearchState(bundle)

  useEffect(() => {
    if (pendingQuery === null) return
    updateState({ q: pendingQuery })
    onPendingApplied()
    // Runs once per buffered query — updateState/onPendingApplied identity
    // isn't relevant to when this should fire.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pendingQuery])

  return (
    <>
      <header className="mb-6">
        <h1 className="font-heading text-2xl font-bold">
          Digital Prosopography of the Roman Republic
        </h1>
        <p className="text-sm text-muted-foreground">
          Search and browse {bundle.payload.summaries.length.toLocaleString()}{" "}
          persons (509–31 BC)
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
          histogram={filteredHistogram}
          state={state}
          onUpdate={updateState}
          officeHierarchy={bundle.payload.officeHierarchy}
          provinceHierarchy={bundle.payload.provinceHierarchy}
          initialFocus={initialFocus}
        />
        <main className="min-w-0 flex-1">
          <ResultsList results={results} />
        </main>
      </div>
    </>
  )
}
