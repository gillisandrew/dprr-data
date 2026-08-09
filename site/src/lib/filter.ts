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

/** Empty selection matches everyone; otherwise the value must be selected. */
function matchesSelection(selected: string[], value: string): boolean {
  return selected.length === 0 || selected.includes(value)
}

/** Empty selection matches everyone; a person with no cognomen never matches. */
function matchesCognomen(selected: string[], cognomen: string | null): boolean {
  if (selected.length === 0) return true
  return !!cognomen && selected.includes(cognomen)
}

/** Empty selection matches everyone; otherwise any person value must be selected. */
function matchesAnySelection(selected: string[], values: string[]): boolean {
  return selected.length === 0 || selected.some((v) => values.includes(v))
}

/** null = facet not active. */
function matchesFlag(selected: boolean | null, value: boolean): boolean {
  return selected === null || value === selected
}

function matchesReNumber(query: string, reNumber: string | null): boolean {
  return !query || (reNumber ?? "").toLowerCase().includes(query.toLowerCase())
}

/** The person's attested era must overlap the selected [from, to] range. */
function matchesEra(
  person: PersonSummary,
  from: number | null,
  to: number | null
): boolean {
  if (from !== null && (person.eraTo === null || person.eraTo < from))
    return false
  if (to !== null && (person.eraFrom === null || person.eraFrom > to))
    return false
  return true
}

function matchesOffices(
  person: PersonSummary,
  state: SearchState,
  ctx: FilterContext,
  inRangeMode: boolean
): boolean {
  if (state.office.length === 0) return true
  const sets = officeSelectionSets(state.office, ctx.parentOf)
  let check: (set: Set<string>) => boolean
  if (inRangeMode) {
    const tuples = ctx.careers[person.id] ?? []
    check = (set) =>
      assertionMatches(set, tuples, ctx.officeNames, state.eraFrom, state.eraTo)
  } else {
    check = (set) => intersects(set, person.offices)
  }
  return state.officeMode === "all" ? sets.every(check) : sets.some(check)
}

export function matchesFacets(
  person: PersonSummary,
  state: SearchState,
  ctx: FilterContext
): boolean {
  // In-range mode scopes the era filter to the selected offices' dates
  // (handled inside matchesOffices) instead of the person's overall era.
  const inRangeMode =
    state.officeInRange &&
    state.office.length > 0 &&
    (state.eraFrom !== null || state.eraTo !== null)

  return (
    matchesOffices(person, state, ctx, inRangeMode) &&
    matchesSelection(state.nomen, person.nomen) &&
    matchesSelection(state.sex, person.sex) &&
    matchesFlag(state.patrician, person.isPatrician) &&
    matchesFlag(state.nobilis, person.isNobilis) &&
    matchesAnySelection(state.tribe, person.tribes) &&
    matchesAnySelection(state.province, person.provinces) &&
    matchesAnySelection(state.event, person.lifeEvents) &&
    matchesSelection(state.praenomen, person.praenomen) &&
    matchesCognomen(state.cognomen, person.cognomen) &&
    matchesReNumber(state.re, person.reNumber) &&
    (inRangeMode || matchesEra(person, state.eraFrom, state.eraTo))
  )
}
