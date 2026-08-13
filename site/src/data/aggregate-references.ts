// site/src/data/aggregate-references.ts
import { slugify } from "../lib/slug"
import { toSummaries, toRowSummaries } from "./loader"
import type {
  Person,
  PersonSummary,
  FastiRowSummary,
  ReferenceMaps,
} from "./types"

export interface OfficeIndexEntry {
  slug: string
  name: string
  abbreviation: string | null
  holderCount: number
  category: string
}
export interface OfficeHolder {
  /** The source PostAssertion id — unique per tenure, so rows keep their
   * identity across renders (a person can hold the same office twice). */
  id: string
  personId: string
  personName: string
  dateStart: number | null
  dateEnd: number | null
  secondarySource: string
  isUncertain: boolean
}
export interface OfficeDetail {
  slug: string
  name: string
  abbreviation: string | null
  holders: OfficeHolder[]
}
export interface TribeIndexEntry {
  slug: string
  name: string
  memberCount: number
}
export interface TribeDetail {
  slug: string
  name: string
  members: PersonSummary[]
}
export interface GensIndexEntry {
  slug: string
  name: string
  memberCount: number
}
export interface GensDetail {
  slug: string
  name: string
  members: PersonSummary[]
}
export interface ProvinceIndexEntry {
  slug: string
  name: string
  personCount: number
}
export interface ProvinceAssertion {
  /** The source PostAssertion id — unique per assignment (see OfficeHolder). */
  id: string
  personId: string
  personName: string
  officeName: string
  dateStart: number | null
  dateEnd: number | null
  isUncertain: boolean
}
export interface ProvinceDetail {
  slug: string
  name: string
  assertions: ProvinceAssertion[]
}

export interface SourceIndexEntry {
  slug: string
  name: string
  abbreviation: string | null
  personCount: number
}
export interface SourceDetail {
  slug: string
  name: string
  abbreviation: string | null
  biblio: string | null
  persons: FastiRowSummary[]
}

/**
 * Every secondary source a person cites, anywhere in their record. Sources
 * appear on posts, statuses, relationships, dates, notes and tribes, so a
 * "cited by" list that only looked at careers would undercount badly.
 */
function citedSources(p: Person): Set<string> {
  const cited = new Set<string>()
  const add = (s: string | null | undefined) => {
    if (s) cited.add(s)
  }
  for (const pa of p.postAssertions) add(pa.secondarySource)
  for (const sa of p.statusAssertions) add(sa.secondarySource)
  for (const rel of p.relationships) add(rel.secondarySource)
  for (const d of p.dateInformation) add(d.secondarySource)
  for (const n of p.personNotes) add(n.secondarySource)
  for (const t of p.tribeAssertions) add(t.secondarySource)
  return cited
}

export function buildSourceIndex(
  persons: Person[],
  sources: ReferenceMaps["sources"]
): SourceIndexEntry[] {
  const counts = new Map<string, number>()
  for (const p of persons) {
    for (const name of citedSources(p)) {
      counts.set(name, (counts.get(name) ?? 0) + 1)
    }
  }
  const names = [...sources.values()].map((s) => s.name)
  assertUniqueSlugs(names, "Source")
  return [...sources.values()]
    .map((s) => ({
      slug: slugify(s.name),
      name: s.name,
      abbreviation: s.abbreviation,
      personCount: counts.get(s.name) ?? 0,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildSourceDetail(
  persons: Person[],
  sources: ReferenceMaps["sources"],
  slug: string
): SourceDetail | null {
  const source = [...sources.values()].find((s) => slugify(s.name) === slug)
  if (!source) return null
  const matching = persons.filter((p) => citedSources(p).has(source.name))
  return {
    slug,
    name: source.name,
    abbreviation: source.abbreviation,
    biblio: source.biblio,
    persons: toRowSummaries(matching).sort((a, b) =>
      a.name.localeCompare(b.name)
    ),
  }
}

export interface PraenomenIndexEntry {
  slug: string
  name: string
  abbreviation: string | null
  personCount: number
}
export interface PraenomenDetail {
  slug: string
  name: string
  abbreviation: string | null
  persons: FastiRowSummary[]
}

export function buildPraenomenIndex(
  persons: Person[],
  praenomina: ReferenceMaps["praenomina"]
): PraenomenIndexEntry[] {
  const counts = new Map<string, number>()
  for (const p of persons) {
    if (!p.praenomen) continue
    counts.set(p.praenomen, (counts.get(p.praenomen) ?? 0) + 1)
  }
  assertUniqueSlugs(
    [...praenomina.values()].map((p) => p.name),
    "Praenomen"
  )
  return [...praenomina.values()]
    .map((p) => ({
      slug: slugify(p.name),
      name: p.name,
      abbreviation: p.abbreviation,
      personCount: counts.get(p.name) ?? 0,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildPraenomenDetail(
  persons: Person[],
  praenomina: ReferenceMaps["praenomina"],
  slug: string
): PraenomenDetail | null {
  const praenomen = [...praenomina.values()].find(
    (p) => slugify(p.name) === slug
  )
  if (!praenomen) return null
  const matching = persons.filter((p) => p.praenomen === praenomen.name)
  return {
    slug,
    name: praenomen.name,
    abbreviation: praenomen.abbreviation,
    persons: toRowSummaries(matching).sort((a, b) =>
      a.name.localeCompare(b.name)
    ),
  }
}

export interface RelationshipIndexEntry {
  slug: string
  name: string
  inverseName: string | null
  pairCount: number
}
export interface RelationshipPair {
  personId: string
  personName: string
  relatedPersonId: string
  relatedPersonName: string
  isUncertain: boolean
}
export interface RelationshipDetail {
  slug: string
  name: string
  inverseName: string | null
  pairs: RelationshipPair[]
}

/** Curated display order (hasOrderNumber); unordered types sort last. */
function byOrderNumber(
  a: { orderNumber: number | null; name: string },
  b: { orderNumber: number | null; name: string }
): number {
  return (
    (a.orderNumber ?? Number.MAX_SAFE_INTEGER) -
      (b.orderNumber ?? Number.MAX_SAFE_INTEGER) || a.name.localeCompare(b.name)
  )
}

export function buildRelationshipIndex(
  persons: Person[],
  relationships: ReferenceMaps["relationships"]
): RelationshipIndexEntry[] {
  const counts = new Map<string, number>()
  for (const p of persons) {
    for (const rel of p.relationships) {
      counts.set(
        rel.relationshipType,
        (counts.get(rel.relationshipType) ?? 0) + 1
      )
    }
  }
  assertUniqueSlugs(
    [...relationships.values()].map((r) => r.name),
    "Relationship"
  )
  return [...relationships.values()].sort(byOrderNumber).map((r) => ({
    slug: slugify(r.name),
    name: r.name,
    inverseName: r.inverseName,
    pairCount: counts.get(r.name) ?? 0,
  }))
}

export function buildRelationshipDetail(
  persons: Person[],
  relationships: ReferenceMaps["relationships"],
  slug: string
): RelationshipDetail | null {
  const type = [...relationships.values()].find((r) => slugify(r.name) === slug)
  if (!type) return null
  const pairs: RelationshipPair[] = []
  for (const p of persons) {
    for (const rel of p.relationships) {
      if (rel.relationshipType !== type.name) continue
      pairs.push({
        personId: p.id,
        personName: p.name,
        relatedPersonId: rel.relatedPersonId,
        relatedPersonName: rel.relatedPersonName,
        isUncertain: rel.isUncertain,
      })
    }
  }
  pairs.sort((a, b) => a.personName.localeCompare(b.personName))
  return { slug, name: type.name, inverseName: type.inverseName, pairs }
}

/** Chronological sort key: earliest known date, undated entries last. */
function dateKey(dateStart: number | null, dateEnd: number | null): number {
  return dateStart ?? dateEnd ?? Number.MAX_SAFE_INTEGER
}

/** Chronological order, ties broken by person name. */
function byDateThenName(
  a: { dateStart: number | null; dateEnd: number | null; personName: string },
  b: { dateStart: number | null; dateEnd: number | null; personName: string }
): number {
  return (
    dateKey(a.dateStart, a.dateEnd) - dateKey(b.dateStart, b.dateEnd) ||
    a.personName.localeCompare(b.personName)
  )
}

function assertUniqueSlugs(names: Iterable<string>, kind: string): void {
  const seen = new Map<string, string>()
  for (const name of names) {
    const slug = slugify(name)
    const prior = seen.get(slug)
    if (prior !== undefined && prior !== name) {
      throw new Error(
        `${kind} slug collision: "${prior}" and "${name}" both to "${slug}"`
      )
    }
    seen.set(slug, name)
  }
}

/** child name -> parent name (null for roots), from a ReferenceMaps-style map */
export function buildNameHierarchy(
  entries: Map<string, { name: string; parent: string | null }>
): Record<string, string | null> {
  const parentOf: Record<string, string | null> = {}
  for (const { name, parent } of entries.values()) {
    parentOf[name] = parent ? (entries.get(parent)?.name ?? null) : null
  }
  return parentOf
}

/** Walk parentOf to the root; returns the root name (or name itself if unknown). */
export function categoryOf(
  name: string,
  parentOf: Record<string, string | null>
): string {
  let current = name
  const seen = new Set<string>()
  while (parentOf[current] != null && !seen.has(current)) {
    seen.add(current)
    current = parentOf[current] as string
  }
  return current
}

export function buildOfficeIndex(
  persons: Person[],
  parentOf: Record<string, string | null>
): OfficeIndexEntry[] {
  const byName = new Map<
    string,
    { abbreviation: string | null; holderIds: Set<string> }
  >()
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      if (!pa.officeName) continue
      let entry = byName.get(pa.officeName)
      if (!entry) {
        entry = { abbreviation: pa.officeAbbreviation, holderIds: new Set() }
        byName.set(pa.officeName, entry)
      }
      entry.abbreviation ??= pa.officeAbbreviation
      entry.holderIds.add(p.id)
    }
  }
  assertUniqueSlugs(byName.keys(), "Office")
  return [...byName]
    .map(([name, { abbreviation, holderIds }]) => ({
      slug: slugify(name),
      name,
      abbreviation,
      holderCount: holderIds.size,
      category: categoryOf(name, parentOf),
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildOfficeDetail(
  persons: Person[],
  slug: string
): OfficeDetail | null {
  let officeName: string | null = null
  let abbreviation: string | null = null
  const holders: OfficeHolder[] = []
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      if (!pa.officeName || slugify(pa.officeName) !== slug) continue
      officeName = pa.officeName
      abbreviation ??= pa.officeAbbreviation
      holders.push({
        id: pa.id,
        personId: p.id,
        personName: p.name,
        dateStart: pa.dateStart,
        dateEnd: pa.dateEnd,
        secondarySource: pa.secondarySource,
        isUncertain: pa.isUncertain,
      })
    }
  }
  if (officeName === null) return null
  holders.sort(byDateThenName)
  return { slug, name: officeName, abbreviation, holders }
}

export function buildTribeIndex(persons: Person[]): TribeIndexEntry[] {
  const byName = new Map<string, number>()
  for (const p of persons) {
    for (const tribe of p.tribes) {
      byName.set(tribe, (byName.get(tribe) ?? 0) + 1)
    }
  }
  assertUniqueSlugs(byName.keys(), "Tribe")
  return [...byName]
    .map(([name, memberCount]) => ({ slug: slugify(name), name, memberCount }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildTribeDetail(
  persons: Person[],
  slug: string
): TribeDetail | null {
  const matching = persons.filter((p) =>
    p.tribes.some((t) => slugify(t) === slug)
  )
  if (matching.length === 0) return null
  const members = toSummaries(matching).sort((a, b) =>
    a.name.localeCompare(b.name)
  )
  const name = matching[0].tribes.find((t) => slugify(t) === slug) as string
  return { slug, name, members }
}

/**
 * Assigns unique slugs to a set of names, preferring the plain (no
 * punctuation) spelling for the bare slug and suffixing variants
 * (`-2`, `-3`, ...) alphabetically. Gens names in the source data include
 * uncertain-attribution variants like "(Cornelius)" or "(Cornelius?)" that
 * slugify identically to the confirmed "Cornelius" — unlike offices, tribes,
 * and provinces, these are legitimate distinct gentes rather than data
 * errors, so collisions are disambiguated instead of rejected.
 */
function buildDisambiguatedSlugs(names: Iterable<string>): Map<string, string> {
  const hasPunctuation = (name: string) => /[^A-Za-z0-9 ]/.test(name)
  const sorted = [...new Set(names)].sort(
    (a, b) =>
      Number(hasPunctuation(a)) - Number(hasPunctuation(b)) ||
      a.localeCompare(b)
  )
  const used = new Set<string>()
  const slugOf = new Map<string, string>()
  for (const name of sorted) {
    const base = slugify(name)
    let slug = base
    let n = 2
    while (used.has(slug)) {
      slug = `${base}-${n}`
      n++
    }
    used.add(slug)
    slugOf.set(name, slug)
  }
  return slugOf
}

/**
 * Names that slugify to "" (e.g. the bare "-" used for unattributed gentes)
 * can't produce a usable detail-page URL — trimmed first so the five
 * trailing-space variants of a name (e.g. "Antestius ") merge with their
 * clean twin instead of forming a separate one-member group.
 */
function nomenGroupingName(nomen: string): string | null {
  const trimmed = nomen.trim()
  if (!trimmed || slugify(trimmed) === "") return null
  return trimmed
}

export function buildGensIndex(persons: Person[]): GensIndexEntry[] {
  const byName = new Map<string, number>()
  for (const p of persons) {
    if (!p.nomen) continue
    const name = nomenGroupingName(p.nomen)
    if (name === null) continue
    byName.set(name, (byName.get(name) ?? 0) + 1)
  }
  const slugOf = buildDisambiguatedSlugs(byName.keys())
  return [...byName]
    .map(([name, memberCount]) => ({
      slug: slugOf.get(name) as string,
      name,
      memberCount,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildGensDetail(
  persons: Person[],
  slug: string
): GensDetail | null {
  if (!slug) return null
  const names = new Set<string>()
  for (const p of persons) {
    if (!p.nomen) continue
    const name = nomenGroupingName(p.nomen)
    if (name !== null) names.add(name)
  }
  const slugOf = buildDisambiguatedSlugs(names)
  let name: string | null = null
  for (const [candidate, candidateSlug] of slugOf) {
    if (candidateSlug === slug) {
      name = candidate
      break
    }
  }
  if (name === null) return null
  const matching = persons.filter(
    (p) => p.nomen && nomenGroupingName(p.nomen) === name
  )
  const members = toSummaries(matching).sort((a, b) =>
    a.name.localeCompare(b.name)
  )
  return { slug, name, members }
}

export function buildProvinceIndex(persons: Person[]): ProvinceIndexEntry[] {
  const byName = new Map<string, Set<string>>()
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      for (const province of pa.provinces) {
        let holderIds = byName.get(province)
        if (!holderIds) {
          holderIds = new Set()
          byName.set(province, holderIds)
        }
        holderIds.add(p.id)
      }
    }
  }
  assertUniqueSlugs(byName.keys(), "Province")
  return [...byName]
    .map(([name, holderIds]) => ({
      slug: slugify(name),
      name,
      personCount: holderIds.size,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildProvinceDetail(
  persons: Person[],
  slug: string
): ProvinceDetail | null {
  let provinceName: string | null = null
  const assertions: ProvinceAssertion[] = []
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      const match = pa.provinces.find((pr) => slugify(pr) === slug)
      if (!match) continue
      provinceName = match
      assertions.push({
        id: pa.id,
        personId: p.id,
        personName: p.name,
        officeName: pa.officeName,
        dateStart: pa.dateStart,
        dateEnd: pa.dateEnd,
        isUncertain: pa.isUncertain,
      })
    }
  }
  if (provinceName === null) return null
  assertions.sort(byDateThenName)
  return { slug, name: provinceName, assertions }
}
