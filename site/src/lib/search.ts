// site/src/lib/search.ts
import { useCallback, useMemo } from "react"
import { useNavigate, useSearch } from "@tanstack/react-router"
import MiniSearch from "minisearch"
import type { PersonSummary, SearchState, FacetValue } from "@/data/types"
import { MINISEARCH_OPTIONS } from "@/data/search-index"

function parseSearchParams(params: Record<string, string>): SearchState {
  return {
    q: params.q ?? "",
    office: params.office ? params.office.split(",") : [],
    nomen: params.nomen ? params.nomen.split(",") : [],
    sex: params.sex ? params.sex.split(",") : [],
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
    tribe: params.tribe ? params.tribe.split(",") : [],
    eraFrom: params.eraFrom ? Number(params.eraFrom) : null,
    eraTo: params.eraTo ? Number(params.eraTo) : null,
  }
}

function toSearchParams(state: SearchState): Record<string, string> {
  const params: Record<string, string> = {}
  if (state.q) params.q = state.q
  if (state.office.length) params.office = state.office.join(",")
  if (state.nomen.length) params.nomen = state.nomen.join(",")
  if (state.sex.length) params.sex = state.sex.join(",")
  if (state.patrician !== null) params.patrician = String(state.patrician)
  if (state.nobilis !== null) params.nobilis = String(state.nobilis)
  if (state.tribe.length) params.tribe = state.tribe.join(",")
  if (state.eraFrom !== null) params.eraFrom = String(state.eraFrom)
  if (state.eraTo !== null) params.eraTo = String(state.eraTo)
  return params
}

function matchesFacets(person: PersonSummary, state: SearchState): boolean {
  if (
    state.office.length > 0 &&
    !state.office.some((o) => person.offices.includes(o))
  )
    return false
  if (state.nomen.length > 0 && !state.nomen.includes(person.nomen))
    return false
  if (state.sex.length > 0 && !state.sex.includes(person.sex)) return false
  if (state.patrician !== null && person.isPatrician !== state.patrician)
    return false
  if (state.nobilis !== null && person.isNobilis !== state.nobilis) return false
  if (
    state.tribe.length > 0 &&
    (!person.tribe || !state.tribe.includes(person.tribe))
  )
    return false
  if (
    state.eraFrom !== null &&
    (person.eraTo === null || person.eraTo < state.eraFrom)
  )
    return false
  if (
    state.eraTo !== null &&
    (person.eraFrom === null || person.eraFrom > state.eraTo)
  )
    return false
  return true
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

export function useSearchState(
  summaries: PersonSummary[],
  searchIndexJson: object
) {
  const rawParams = useSearch({ strict: false }) as Record<string, string>
  const navigate = useNavigate()
  const state = useMemo(() => parseSearchParams(rawParams), [rawParams])

  const miniSearch = useMemo(() => {
    return MiniSearch.loadJSON<PersonSummary>(
      JSON.stringify(searchIndexJson),
      MINISEARCH_OPTIONS
    )
  }, [searchIndexJson])

  const results = useMemo(() => {
    let candidates: PersonSummary[]

    if (state.q.trim()) {
      const searchResults = miniSearch.search(state.q)
      const idSet = new Set(searchResults.map((r) => r.id))
      candidates = summaries.filter((p) => idSet.has(p.id))
    } else {
      candidates = summaries
    }

    return candidates.filter((p) => matchesFacets(p, state))
  }, [state, summaries, miniSearch])

  // Disjunctive facet counting: each facet's counts are computed with
  // that facet's own filter removed but all other filters kept. This
  // lets users see how many results other values would produce.
  const facets = useMemo(() => {
    function countWith(exclude: keyof SearchState, field: keyof PersonSummary) {
      const relaxed = {
        ...state,
        [exclude]: Array.isArray(state[exclude]) ? [] : null,
      }
      let candidates: PersonSummary[]
      if (state.q.trim()) {
        const searchResults = miniSearch.search(state.q)
        const idSet = new Set(searchResults.map((r) => r.id))
        candidates = summaries.filter((p) => idSet.has(p.id))
      } else {
        candidates = summaries
      }
      const filtered = candidates.filter((p) => matchesFacets(p, relaxed))
      return computeFacetValues(filtered, field)
    }

    return {
      office: countWith("office", "offices"),
      nomen: countWith("nomen", "nomen"),
      sex: countWith("sex", "sex"),
      tribe: countWith("tribe", "tribe"),
    }
  }, [state, summaries, miniSearch])

  const updateState = useCallback(
    (updates: Partial<SearchState>) => {
      const newState = { ...state, ...updates }
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

  return { state, results, facets, updateState, clearAll }
}
