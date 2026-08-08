// site/src/routes/index.tsx
import { useEffect, useReducer, useState } from "react"
import { createFileRoute } from "@tanstack/react-router"
import { parseSearchParams, toSearchParams, useSearchState } from "@/lib/search"
import { useSearchData, type SearchDataBundle } from "@/lib/use-search-data"
import {
  landingBufferReducer,
  initialLandingBufferState,
} from "@/lib/landing-state"
import { SearchInput } from "@/components/search-input"
import { ActiveFilterChips } from "@/components/active-filter-chips"
import { EraTimeline } from "@/components/era-timeline"
import { FacetSidebar } from "@/components/facet-sidebar"
import { ResultsHeader, ResultsList } from "@/components/results-list"
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
  const [{ interacted, pendingQuery }, dispatch] = useReducer(
    landingBufferReducer,
    initialLandingBufferState,
    (init) => (hasParams ? { ...init, interacted: true } : init)
  )
  const [initialFocus, setInitialFocus] = useState<Focus | undefined>()
  const showLanding = !hasParams && !interacted
  const { bundle, error } = useSearchData(!showLanding)

  useEffect(() => {
    if (hasParams && !interacted) dispatch({ type: "interact" })
  }, [hasParams, interacted])

  const content = showLanding ? (
    <SearchLanding
      onQueryChange={(q) => dispatch({ type: "type", query: q })}
      onBrowse={(focus) => {
        setInitialFocus(focus)
        dispatch({ type: "interact" })
      }}
    />
  ) : error ? (
    <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
      Failed to load search data — please reload. ({error})
    </div>
  ) : !bundle ? (
    <LoadingSearch
      query={pendingQuery ?? ""}
      onQueryChange={(q) => dispatch({ type: "type", query: q })}
    />
  ) : (
    <SearchResults
      bundle={bundle}
      initialFocus={initialFocus}
      pendingQuery={pendingQuery}
      onPendingApplied={() => dispatch({ type: "apply-pending" })}
    />
  )

  return <div className="mx-auto max-w-6xl px-4 py-6">{content}</div>
}

/** Loading state after the user has interacted (typed, or asked to browse)
 * but before the ~600KB search bundle has finished loading. Renders the
 * same chrome as the results page with a live, autofocused search input
 * bound to the buffered query — replacing a bare "Loading…" message here
 * used to strand any characters typed after the first keystroke, since
 * that message had nowhere for further typing to go. */
function LoadingSearch({
  query,
  onQueryChange,
}: {
  query: string
  onQueryChange: (q: string) => void
}) {
  return (
    <>
      <header className="mb-6">
        <h1 className="font-heading text-2xl font-bold">
          Digital Prosopography of the Roman Republic
        </h1>
        <p className="text-sm text-muted-foreground">Loading search data…</p>
      </header>
      <div className="rule-lead pb-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="min-w-64 flex-1">
            <SearchInput value={query} onChange={onQueryChange} autoFocus />
          </div>
        </div>
      </div>
    </>
  )
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

      <div className="rule-lead pb-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="min-w-64 flex-1">
            <SearchInput
              value={state.q}
              onChange={(q) => updateState({ q })}
              autoFocus={pendingQuery !== null}
            />
          </div>
          <ResultsHeader
            count={results.length}
            sort={state.sort}
            hasQuery={state.q.trim().length > 0}
            onSortChange={(sort) => updateState({ sort })}
          />
        </div>
        <div className="mt-2 empty:hidden">
          <ActiveFilterChips
            state={state}
            onRemove={updateState}
            onClearAll={clearAll}
          />
        </div>
      </div>

      <div className="mt-4 space-y-1">
        <p className="text-sm font-semibold">Time period</p>
        <EraTimeline
          histogram={filteredHistogram}
          from={state.eraFrom}
          to={state.eraTo}
          onChange={(eraFrom, eraTo) => updateState({ eraFrom, eraTo })}
        />
      </div>

      <div className="mt-4 flex gap-6">
        <FacetSidebar
          facets={facets}
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
