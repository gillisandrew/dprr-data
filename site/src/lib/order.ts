// site/src/lib/order.ts
import type { PersonSummary, SearchState } from "@/data/types"

export const UNDATED = Number.MAX_SAFE_INTEGER

/** Earliest-known-year key; undated → UNDATED (sorts last ascending). */
export function eraKey(p: {
  eraFrom: number | null
  eraTo: number | null
}): number {
  return p.eraFrom ?? p.eraTo ?? UNDATED
}

const stripId = (name: string) => name.replace(/^[A-Z]{4}\d+ /, "")
// Roman names are alphabetized by nomen (family name), not by the leading
// praenomen abbreviation (e.g. "L.", "Ti.", "App.") — drop it before comparing.
const stripPraenomen = (name: string) => name.replace(/^[A-Za-z]+\.\s+/, "")

export function compareByName(
  a: { name: string },
  b: { name: string }
): number {
  return stripPraenomen(stripId(a.name)).localeCompare(
    stripPraenomen(stripId(b.name))
  )
}

/** sort=null resolves to "relevance" when hasQuery else "earliest". */
export function sortResults(
  results: PersonSummary[],
  sort: SearchState["sort"],
  hasQuery: boolean
): PersonSummary[] {
  const resolved = sort ?? (hasQuery ? "relevance" : "earliest")
  const copy = [...results]
  switch (resolved) {
    case "relevance":
      return copy
    case "name":
      return copy.sort(compareByName)
    case "latest":
      return copy.sort(
        (a, b) =>
          (eraKey(b) === UNDATED ? -UNDATED : eraKey(b)) -
            (eraKey(a) === UNDATED ? -UNDATED : eraKey(a)) ||
          compareByName(a, b)
      )
    case "earliest":
      return copy.sort((a, b) => eraKey(a) - eraKey(b) || compareByName(a, b))
  }
}
