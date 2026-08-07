// site/src/lib/filter.ts
import type { PersonSummary, SearchState } from "@/data/types"

export interface FilterContext {
  /** office child name → parent name (from payload.officeHierarchy). */
  parentOf: Record<string, string | null>
  careers: Record<string, [number, number | null, number | null][]>
  officeNames: string[]
}

// The fixed-point expansion below walks the entire hierarchy (e.g. ~200
// office nodes), and matchesFacets calls descendantSet once per selected
// value per candidate person. Without caching, a single filter pass over
// thousands of people re-walks the hierarchy from scratch for every one of
// them. Cache per (parentOf object identity, value) so each pair is
// expanded exactly once for the lifetime of a given hierarchy object.
const descendantCache = new WeakMap<
  Record<string, string | null>,
  Map<string, Set<string>>
>()

/** Selected value → the set of it plus all hierarchy descendants. */
export function descendantSet(
  value: string,
  parentOf: Record<string, string | null>
): Set<string> {
  let cache = descendantCache.get(parentOf)
  if (!cache) {
    cache = new Map()
    descendantCache.set(parentOf, cache)
  }
  const cached = cache.get(value)
  if (cached) return cached

  const result = new Set<string>([value])
  let added = true
  while (added) {
    added = false
    for (const [child, parent] of Object.entries(parentOf)) {
      if (parent !== null && result.has(parent) && !result.has(child)) {
        result.add(child)
        added = true
      }
    }
  }
  cache.set(value, result)
  return result
}

function officeSelectionSets(
  selected: string[],
  parentOf: Record<string, string | null>
): Set<string>[] {
  return selected.map((s) => descendantSet(s, parentOf))
}

function intersects(set: Set<string>, values: string[]): boolean {
  return values.some((v) => set.has(v))
}

function assertionMatches(
  set: Set<string>,
  tuples: [number, number | null, number | null][],
  officeNames: string[],
  from: number | null,
  to: number | null
): boolean {
  for (const [idx, dateStart, dateEnd] of tuples) {
    if (!set.has(officeNames[idx])) continue
    const start = dateStart ?? dateEnd
    const end = dateEnd ?? dateStart
    if (start === null || end === null) continue
    if (from !== null && end < from) continue
    if (to !== null && start > to) continue
    return true
  }
  return false
}

export function matchesFacets(
  person: PersonSummary,
  state: SearchState,
  ctx: FilterContext
): boolean {
  const inRangeMode =
    state.officeInRange &&
    state.office.length > 0 &&
    (state.eraFrom !== null || state.eraTo !== null)

  if (state.office.length > 0) {
    const sets = officeSelectionSets(state.office, ctx.parentOf)
    if (inRangeMode) {
      const tuples = ctx.careers[person.id] ?? []
      const check = (set: Set<string>) =>
        assertionMatches(
          set,
          tuples,
          ctx.officeNames,
          state.eraFrom,
          state.eraTo
        )
      if (state.officeMode === "all" ? !sets.every(check) : !sets.some(check))
        return false
    } else {
      const check = (set: Set<string>) => intersects(set, person.offices)
      if (state.officeMode === "all" ? !sets.every(check) : !sets.some(check))
        return false
    }
  }

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
    state.province.length > 0 &&
    !state.province.some((pr) => person.provinces.includes(pr))
  )
    return false
  if (
    state.event.length > 0 &&
    !state.event.some((e) => person.lifeEvents.includes(e))
  )
    return false
  if (state.praenomen.length > 0 && !state.praenomen.includes(person.praenomen))
    return false
  if (
    state.cognomen.length > 0 &&
    (!person.cognomen || !state.cognomen.includes(person.cognomen))
  )
    return false
  if (
    state.re &&
    !(person.reNumber ?? "").toLowerCase().includes(state.re.toLowerCase())
  )
    return false

  if (!inRangeMode) {
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
  }
  return true
}
