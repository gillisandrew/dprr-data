// site/src/lib/search.ts
import { useCallback, useMemo } from "react"
import { useNavigate, useSearch } from "@tanstack/react-router"
import type { PersonSummary, SearchState, FacetValue } from "@/data/types"
import { matchesFacets, type FilterContext } from "./filter"
import { sortResults } from "./order"
import { buildHistogram } from "./histogram"
import type { SearchDataBundle } from "./use-search-data"

/** Split a comma-joined, per-value-encoded facet param back into raw values. */
function splitFacetParam(value: string | undefined): string[] {
  return value ? value.split(",").map((v) => decodeURIComponent(v)) : []
}

/** Join facet values into a comma-separated param, encoding each value first
 * so commas embedded in a value (e.g. an office name) don't corrupt the
 * split on the way back. */
function joinFacetParam(values: string[]): string {
  return values.map((v) => encodeURIComponent(v)).join(",")
}

export function parseSearchParams(params: Record<string, string>): SearchState {
  return {
    q: params.q ?? "",
    office: splitFacetParam(params.office),
    nomen: splitFacetParam(params.nomen),
    sex: splitFacetParam(params.sex),
    patrician:
      params.patrician === "true"
        ? true
        : params.patrician === "false"
          ? false
          : null,
    nobilis:
      params.nobilis === "true"
        ? true
        : params.nobilis === "false"
          ? false
          : null,
    tribe: splitFacetParam(params.tribe),
    province: splitFacetParam(params.province),
    eraFrom: params.eraFrom ? Number(params.eraFrom) : null,
    eraTo: params.eraTo ? Number(params.eraTo) : null,
    event: splitFacetParam(params.event),
    praenomen: splitFacetParam(params.praenomen),
    cognomen: splitFacetParam(params.cognomen),
    re: params.re ?? "",
    officeMode: params.officeMode === "all" ? "all" : "any",
    officeInRange: params.officeInRange === "true",
    sort:
      params.sort === "earliest" ||
      params.sort === "latest" ||
      params.sort === "name" ||
      params.sort === "relevance"
        ? params.sort
        : null,
  }
}

/**
 * Reconcile mutually-inconsistent parts of search state after an update.
 * An explicit "relevance" sort only makes sense while a query is active —
 * if the query clears (or was cleared in the same update) while "relevance"
 * is still selected, drop back to the default sort so the sort <select>
 * doesn't render with no matching option and `sortResults` doesn't fall
 * through to an arbitrary passthrough order.
 */
export function normalizeState(state: SearchState): SearchState {
  if (!state.q.trim() && state.sort === "relevance") {
    return { ...state, sort: null }
  }
  return state
}

export function toSearchParams(state: SearchState): Record<string, string> {
  const params: Record<string, string> = {}
  if (state.q) params.q = state.q
  if (state.office.length) params.office = joinFacetParam(state.office)
  if (state.nomen.length) params.nomen = joinFacetParam(state.nomen)
  if (state.sex.length) params.sex = joinFacetParam(state.sex)
  if (state.patrician !== null) params.patrician = String(state.patrician)
  if (state.nobilis !== null) params.nobilis = String(state.nobilis)
  if (state.tribe.length) params.tribe = joinFacetParam(state.tribe)
  if (state.province.length) params.province = joinFacetParam(state.province)
  if (state.eraFrom !== null) params.eraFrom = String(state.eraFrom)
  if (state.eraTo !== null) params.eraTo = String(state.eraTo)
  if (state.event.length) params.event = joinFacetParam(state.event)
  if (state.praenomen.length) params.praenomen = joinFacetParam(state.praenomen)
  if (state.cognomen.length) params.cognomen = joinFacetParam(state.cognomen)
  if (state.re) params.re = state.re
  if (state.officeMode !== "any") params.officeMode = state.officeMode
  if (state.officeInRange) params.officeInRange = "true"
  if (state.sort) params.sort = state.sort
  return params
}

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

function computeFacetValues(
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
    .sort((a, b) => b.count - a.count)
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
