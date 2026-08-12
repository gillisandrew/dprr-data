// site/src/lib/search-params.ts
// URL search-param serialization for the search page: SearchState <-> ?params.
import type { SearchState } from "@/data/types"

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

export function parseSearchParams(
  rawParams: Record<string, string>
): SearchState {
  // TanStack Router JSON-parses the search string, so a numeric-looking
  // value like `?q=509` or a boolean-looking value like `?patrician=true`
  // arrives here as an actual number/boolean, not a string — coerce
  // everything to string up front so `.trim()` and the `=== "true"` checks
  // below don't crash or silently mis-parse. null/undefined are dropped
  // rather than coerced to the strings "null"/"undefined".
  const params = Object.fromEntries(
    Object.entries(rawParams)
      .filter(([, v]) => v !== null && v !== undefined)
      .map(([k, v]) => [k, String(v)])
  ) as Record<string, string>
  return {
    q: params.q ?? "",
    office: splitFacetParam(params.office),
    nomen: splitFacetParam(params.nomen),
    sex: splitFacetParam(params.sex),
    status: [
      ...new Set([
        ...splitFacetParam(params.status),
        // Pre-status-facet URLs used boolean patrician/nobilis params;
        // keep shipped links working by folding them in. Dedup since a URL
        // could combine both spellings (?status=Patrician&patrician=true).
        ...(params.patrician === "true" ? ["Patrician"] : []),
        ...(params.nobilis === "true" ? ["Nobilis"] : []),
      ]),
    ],
    father: splitFacetParam(params.father),
    grandfather: splitFacetParam(params.grandfather),
    tribe: splitFacetParam(params.tribe),
    province: splitFacetParam(params.province),
    eraFrom: params.eraFrom ? Number(params.eraFrom) : null,
    eraTo: params.eraTo ? Number(params.eraTo) : null,
    event: splitFacetParam(params.event),
    praenomen: splitFacetParam(params.praenomen),
    cognomen: splitFacetParam(params.cognomen),
    re: (params.re ?? "").trim(),
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
  if (state.status.length) params.status = joinFacetParam(state.status)
  if (state.father.length) params.father = joinFacetParam(state.father)
  if (state.grandfather.length)
    params.grandfather = joinFacetParam(state.grandfather)
  if (state.tribe.length) params.tribe = joinFacetParam(state.tribe)
  if (state.province.length) params.province = joinFacetParam(state.province)
  if (state.eraFrom !== null) params.eraFrom = String(state.eraFrom)
  if (state.eraTo !== null) params.eraTo = String(state.eraTo)
  if (state.event.length) params.event = joinFacetParam(state.event)
  if (state.praenomen.length) params.praenomen = joinFacetParam(state.praenomen)
  if (state.cognomen.length) params.cognomen = joinFacetParam(state.cognomen)
  if (state.re.trim()) params.re = state.re.trim()
  if (state.officeMode !== "any") params.officeMode = state.officeMode
  if (state.officeInRange) params.officeInRange = "true"
  if (state.sort) params.sort = state.sort
  return params
}
