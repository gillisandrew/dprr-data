// site/src/data/search-payload.ts
import { toSummaries } from "./loader"
import { buildSearchIndex, MINISEARCH_OPTIONS } from "./search-index"
import { buildNameHierarchy } from "./aggregate-references"
import { buildHistogram, type Histogram } from "../lib/histogram"
import type { Person, PersonSummary, ReferenceMaps } from "./types"

export interface SearchPayload {
  summaries: PersonSummary[]
  /** Office name table; career tuples index into it. */
  officeNames: string[]
  /** personId → [officeNameIndex, dateStart, dateEnd][] (dated + undated assertions). */
  careers: Record<string, [number, number | null, number | null][]>
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  histogram: Histogram
}

export interface SerializableOptions {
  fields: string[]
  storeFields: string[]
  idField: string
  searchOptions: {
    prefix: boolean
    fuzzy: number
    boost: Record<string, number>
  }
}

export interface SearchIndexPayload {
  index: object
  options: SerializableOptions
}

export function buildSearchPayload(
  persons: Person[],
  refs: ReferenceMaps
): SearchPayload {
  const officeNamesSet = new Set<string>()
  const rawCareers = new Map<string, [string, number | null, number | null][]>()
  const allRanges: [number | null, number | null][] = []

  for (const p of persons) {
    const tuples: [string, number | null, number | null][] = []
    for (const pa of p.postAssertions) {
      if (!pa.officeName) continue
      officeNamesSet.add(pa.officeName)
      tuples.push([pa.officeName, pa.dateStart, pa.dateEnd])
      allRanges.push([pa.dateStart, pa.dateEnd])
    }
    if (tuples.length > 0) rawCareers.set(p.id, tuples)
  }

  // Sorted (rather than insertion-order) so index lookups stay valid even
  // if a consumer sorts the returned officeNames array in place.
  const officeNames = [...officeNamesSet].sort()
  const officeIndex = new Map(officeNames.map((name, i) => [name, i]))

  const careers: SearchPayload["careers"] = {}
  for (const [personId, tuples] of rawCareers) {
    careers[personId] = tuples.map(([name, s, e]) => [
      officeIndex.get(name)!,
      s,
      e,
    ])
  }

  return {
    summaries: toSummaries(persons),
    officeNames,
    careers,
    officeHierarchy: buildNameHierarchy(refs.offices),
    provinceHierarchy: buildNameHierarchy(refs.provinces),
    histogram: buildHistogram(allRanges),
  }
}

export function buildSearchIndexPayload(
  summaries: PersonSummary[]
): SearchIndexPayload {
  return {
    index: buildSearchIndex(summaries),
    options: {
      fields: MINISEARCH_OPTIONS.fields as string[],
      storeFields: MINISEARCH_OPTIONS.storeFields as string[],
      idField: MINISEARCH_OPTIONS.idField as string,
      searchOptions: {
        prefix: MINISEARCH_OPTIONS.searchOptions?.prefix as boolean,
        fuzzy: MINISEARCH_OPTIONS.searchOptions?.fuzzy as number,
        boost: MINISEARCH_OPTIONS.searchOptions?.boost as Record<
          string,
          number
        >,
      },
    },
  }
}
