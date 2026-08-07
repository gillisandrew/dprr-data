// site/src/routes/index.tsx
import { useMemo } from "react"
import { createFileRoute } from "@tanstack/react-router"
import MiniSearch from "minisearch"
import { getAllPersonIds } from "@/server/data"
import { useSearchState } from "@/lib/search"
import { useSearchData } from "@/lib/use-search-data"
import { MINISEARCH_OPTIONS } from "@/data/search-index"
import type { PersonSummary } from "@/data/types"
import { SearchInput } from "@/components/search-input"
import { ActiveFilterChips } from "@/components/active-filter-chips"
import { FacetSidebar } from "@/components/facet-sidebar"
import { ResultsList } from "@/components/results-list"
import { SITE_URL } from "@/lib/site"

export const Route = createFileRoute("/")({
  validateSearch: (search: Record<string, unknown>) => search,
  loader: () => getAllPersonIds(),
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

const EMPTY_SUMMARIES: PersonSummary[] = []

function SearchPage() {
  const personIds = Route.useLoaderData()
  const { bundle, error } = useSearchData(true)

  // useSearchState must be called unconditionally (Rules of Hooks) even
  // while the static JSON bundle hasn't loaded yet, so fall back to an
  // empty summaries array and a freshly-constructed (empty) MiniSearch
  // instance until it does — the loading/error UI below hides the results
  // in the meantime.
  const emptyMiniSearch = useMemo(
    () => new MiniSearch<PersonSummary>(MINISEARCH_OPTIONS),
    []
  )
  const { state, results, facets, updateState, clearAll } = useSearchState(
    bundle?.payload.summaries ?? EMPTY_SUMMARIES,
    bundle?.miniSearch ?? emptyMiniSearch
  )

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      {error ? (
        <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
          Failed to load search data — please reload. ({error})
        </div>
      ) : !bundle ? (
        <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
          Loading search data…
        </div>
      ) : (
        <>
          <header className="mb-6">
            <h1 className="font-heading text-2xl font-bold">
              Digital Prosopography of the Roman Republic
            </h1>
            <p className="text-sm text-muted-foreground">
              Search and browse{" "}
              {bundle.payload.summaries.length.toLocaleString()} persons (509–31
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
              officeHierarchy={bundle.payload.officeHierarchy}
              provinceHierarchy={bundle.payload.provinceHierarchy}
            />
            <main className="min-w-0 flex-1">
              <ResultsList results={results} />
            </main>
          </div>
        </>
      )}

      {/* Hidden links for static prerender crawler — stopgap loader until
          the /directory/ page (later task) replaces this. */}
      <div className="hidden" aria-hidden="true">
        {personIds.map((id) => (
          <a key={id} href={`${import.meta.env.BASE_URL}persons/${id}`}>
            {id}
          </a>
        ))}
      </div>
    </div>
  )
}
