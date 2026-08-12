// site/src/components/results-list.tsx
import { useState } from "react"
import { FastiRow } from "./fasti-row"
import type { PersonSummary, SearchState } from "@/data/types"

const PAGE_SIZE = 50

export function ResultsHeader({
  count,
  sort,
  hasQuery,
  onSortChange,
}: {
  count: number
  sort: SearchState["sort"]
  hasQuery: boolean
  onSortChange: (sort: SearchState["sort"]) => void
}) {
  return (
    <div className="flex items-baseline gap-3">
      <p className="text-sm text-muted-foreground">
        {count.toLocaleString()} result
        {count !== 1 && "s"}
      </p>
      <label className="text-xs text-muted-foreground">
        Sort{" "}
        <select
          value={sort ?? (hasQuery ? "relevance" : "earliest")}
          onChange={(e) => onSortChange(e.target.value as SearchState["sort"])}
          className="rounded-md border bg-transparent px-1 py-0.5"
        >
          <option value="earliest">Earliest first</option>
          <option value="latest">Latest first</option>
          <option value="name">Name A–Z</option>
          {hasQuery && <option value="relevance">Relevance</option>}
        </select>
      </label>
    </div>
  )
}

export function ResultsList({ results }: { results: PersonSummary[] }) {
  const [page, setPage] = useState(0)

  // Reset to first page when results change (new search/filter). Done during
  // render rather than in an effect so the first page is what commits — an
  // effect would paint page N of the old list for a frame first. `prevResults`
  // is only ever compared, never rendered.
  const [prevResults, setPrevResults] = useState(results)
  if (results !== prevResults) {
    setPrevResults(results)
    setPage(0)
  }

  const totalPages = Math.ceil(results.length / PAGE_SIZE)
  const visible = results.slice(0, (page + 1) * PAGE_SIZE)

  return (
    <div>
      {results.length === 0 ? (
        <p className="py-8 text-center text-sm text-muted-foreground">
          No persons match — try removing a filter.
        </p>
      ) : (
        <div>
          {visible.map((person) => (
            <FastiRow key={person.id} person={person} />
          ))}
        </div>
      )}
      {page + 1 < totalPages && (
        <button
          onClick={() => setPage((p) => p + 1)}
          className="mt-4 text-sm text-accent-ink hover:underline"
        >
          Show more ({results.length - visible.length} remaining)
        </button>
      )}
    </div>
  )
}
