// site/src/lib/search.ts
import { useCallback, useMemo } from "react"
import { useNavigate, useSearch } from "@tanstack/react-router"
import type { PersonSummary, SearchState, FacetValue } from "@/data/types"
import { matchesFacets, type FilterContext } from "./filter"
import { sortResults } from "./order"
import { buildHistogram } from "./histogram"
import {
  normalizeState,
  parseSearchParams,
  toSearchParams,
} from "./search-params"
import type { SearchDataBundle } from "./use-search-data"

/**
 * Order candidates by MiniSearch relevance rank (ascending index = best
 * match first), dropping any candidate that isn't present in the search
 * results. Pure and independent of React so it can be exercised directly —
 * `summaries.filter(...)` alone (as used pre-fix) discards MiniSearch's
 * ranking and falls back to the candidates' original (ID) order.
 */
export function orderByQueryRank<T extends { id: string }>(
  candidates: T[],
  searchResults: { id: string }[]
): T[] {
  const rank = new Map(searchResults.map((r, i) => [r.id, i]))
  return candidates
    .filter((c) => rank.has(c.id))
    .sort((a, b) => rank.get(a.id)! - rank.get(b.id)!)
}

/** Exported for tests — tie-broken alphabetically so equal-count facet
 * values render in a stable, predictable order rather than Map iteration
 * order. */
export function computeFacetValues(
  persons: PersonSummary[],
  field: keyof PersonSummary
): FacetValue[] {
  const counts = new Map<string, number>()
  for (const p of persons) {
    const val = p[field]
    if (Array.isArray(val)) {
      for (const v of val) {
        counts.set(v, (counts.get(v) ?? 0) + 1)
      }
    } else if (typeof val === "string" && val) {
      counts.set(val, (counts.get(val) ?? 0) + 1)
    }
  }
  return [...counts]
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value))
}

export function useSearchState(bundle: SearchDataBundle) {
  const { payload, miniSearch } = bundle
  const summaries = payload.summaries
  const rawParams = useSearch({ strict: false }) as Record<string, string>
  const navigate = useNavigate()
  const state = useMemo(() => parseSearchParams(rawParams), [rawParams])

  const ctx: FilterContext = useMemo(
    () => ({
      parentOf: payload.officeHierarchy,
      careers: payload.careers,
      officeNames: payload.officeNames,
    }),
    [payload]
  )

  // MiniSearch is only re-queried when the query text itself changes, then
  // reused by both `filtered` and every `countWith` call below — previously
  // each of those called miniSearch.search independently (8 calls per
  // render with a query active).
  const queryCandidates = useMemo(() => {
    if (!state.q.trim()) return summaries
    return orderByQueryRank(summaries, miniSearch.search(state.q))
  }, [state.q, summaries, miniSearch])

  const filtered = useMemo(
    () => queryCandidates.filter((p) => matchesFacets(p, state, ctx)),
    [queryCandidates, state, ctx]
  )

  const results = useMemo(
    () => sortResults(filtered, state.sort, state.q.trim().length > 0),
    [filtered, state.sort, state.q]
  )

  // Disjunctive facet counting: each facet's counts are computed with
  // that facet's own filter removed but all other filters kept. This
  // lets users see how many results other values would produce.
  const facets = useMemo(() => {
    function countWith(exclude: keyof SearchState, field: keyof PersonSummary) {
      const relaxed = {
        ...state,
        [exclude]: Array.isArray(state[exclude]) ? [] : null,
      }
      const filteredForCount = queryCandidates.filter((p) =>
        matchesFacets(p, relaxed, ctx)
      )
      return computeFacetValues(filteredForCount, field)
    }

    return {
      office: countWith("office", "offices"),
      nomen: countWith("nomen", "nomen"),
      sex: countWith("sex", "sex"),
      tribe: countWith("tribe", "tribes"),
      province: countWith("province", "provinces"),
      event: countWith("event", "lifeEvents"),
      praenomen: countWith("praenomen", "praenomen"),
      cognomen: countWith("cognomen", "cognomen"),
    }
  }, [state, queryCandidates, ctx])

  const filteredHistogram = useMemo(() => {
    // A sort-only URL counts as unfiltered.
    const anyFilter =
      Object.keys(toSearchParams({ ...state, sort: null })).length > 0
    if (!anyFilter) return payload.histogram
    const ranges: [number | null, number | null][] = []
    for (const p of filtered) {
      for (const [, s, e] of payload.careers[p.id] ?? []) ranges.push([s, e])
    }
    return buildHistogram(ranges)
  }, [state, filtered, payload])

  const updateState = useCallback(
    (updates: Partial<SearchState>) => {
      const newState = normalizeState({ ...state, ...updates })
      void navigate({
        to: "/",
        search: toSearchParams(newState),
        replace: true,
      })
    },
    [state, navigate]
  )

  const clearAll = useCallback(() => {
    void navigate({ to: "/", search: {}, replace: true })
  }, [navigate])

  return { state, results, facets, updateState, clearAll, filteredHistogram }
}
