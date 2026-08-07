// site/src/lib/search.ts
import { useCallback, useMemo } from "react"
import { useNavigate, useSearch } from "@tanstack/react-router"
import type MiniSearch from "minisearch"
import type { PersonSummary, SearchState, FacetValue } from "@/data/types"

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
  }
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
  return params
}

function matchesFacets(person: PersonSummary, state: SearchState): boolean {
  if (
    state.office.length > 0 &&
    !state.office.some((o) => person.offices.includes(o))
  )
    return false
  if (
    state.province.length > 0 &&
    !state.province.some((pr) => person.provinces.includes(pr))
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
    !state.tribe.some((t) => person.tribes.includes(t))
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
  miniSearch: MiniSearch<PersonSummary>
) {
  const rawParams = useSearch({ strict: false }) as Record<string, string>
  const navigate = useNavigate()
  const state = useMemo(() => parseSearchParams(rawParams), [rawParams])

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
      tribe: countWith("tribe", "tribes"),
      province: countWith("province", "provinces"),
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
