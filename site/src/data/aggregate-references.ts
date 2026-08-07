// site/src/data/aggregate-references.ts
import { slugify } from "../lib/slug"
import { toSummaries } from "./loader"
import type { Person, PersonSummary } from "./types"

export interface OfficeIndexEntry {
  slug: string
  name: string
  abbreviation: string | null
  holderCount: number
}
export interface OfficeHolder {
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
export interface ProvinceIndexEntry {
  slug: string
  name: string
  assertionCount: number
}
export interface ProvinceAssertion {
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

/** Chronological sort key: earliest known date, undated entries last. */
function dateKey(dateStart: number | null, dateEnd: number | null): number {
  return dateStart ?? dateEnd ?? Number.POSITIVE_INFINITY
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

export function buildOfficeIndex(persons: Person[]): OfficeIndexEntry[] {
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
  holders.sort(
    (a, b) =>
      dateKey(a.dateStart, a.dateEnd) - dateKey(b.dateStart, b.dateEnd) ||
      a.personName.localeCompare(b.personName)
  )
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

export function buildProvinceIndex(persons: Person[]): ProvinceIndexEntry[] {
  const byName = new Map<string, number>()
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      for (const province of pa.provinces) {
        byName.set(province, (byName.get(province) ?? 0) + 1)
      }
    }
  }
  assertUniqueSlugs(byName.keys(), "Province")
  return [...byName]
    .map(([name, assertionCount]) => ({
      slug: slugify(name),
      name,
      assertionCount,
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
  assertions.sort(
    (a, b) =>
      dateKey(a.dateStart, a.dateEnd) - dateKey(b.dateStart, b.dateEnd) ||
      a.personName.localeCompare(b.personName)
  )
  return { slug, name: provinceName, assertions }
}
