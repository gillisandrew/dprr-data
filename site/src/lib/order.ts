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

// DPRR IDs are normally 4 uppercase letters + digits (e.g. "IUNI0001"), but
// a handful of uncertain-gens entries use 1-4 letters + a hyphen instead of
// digits (e.g. "AN-3071", "B-3088").
const stripId = (name: string) => name.replace(/^[A-Z]{1,4}[-\d]\S* /, "")
// Roman names are alphabetized by nomen (family name), not by the leading
// praenomen abbreviation (e.g. "L.", "Ti.", "App.") — drop it before
// comparing. Also handles unknown praenomen markers ("-."), the apostrophe
// in "M'." (Manius), and names with no praenomen at all (left with a
// leading space after the ID is stripped).
const stripPraenomen = (name: string) =>
  name.replace(/^(?:[A-Za-z]+['?]?\.|-\.)?\s*/, "")

export function compareByName(
  a: { name: string },
  b: { name: string }
): number {
  return stripPraenomen(stripId(a.name)).localeCompare(
    stripPraenomen(stripId(b.name))
  )
}

/**
 * Compare two era keys for a given direction (1 = ascending/earliest-first,
 * -1 = descending/latest-first). Undated entries (UNDATED) always sort last
 * regardless of direction — branched explicitly rather than folded into the
 * numeric subtraction, since negating/comparing against MAX_SAFE_INTEGER via
 * plain arithmetic is easy to get subtly wrong.
 */
function compareEraKeys(ka: number, kb: number, direction: 1 | -1): number {
  if (ka === UNDATED && kb === UNDATED) return 0
  if (ka === UNDATED) return 1
  if (kb === UNDATED) return -1
  return direction * (ka - kb)
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
          compareEraKeys(eraKey(a), eraKey(b), -1) || compareByName(a, b)
      )
    case "earliest":
      return copy.sort(
        (a, b) => compareEraKeys(eraKey(a), eraKey(b), 1) || compareByName(a, b)
      )
  }
}
