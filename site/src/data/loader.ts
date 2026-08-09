// site/src/data/loader.ts
import { readFile, readdir } from "node:fs/promises"
import { join, basename } from "node:path"
import { parseReferenceTtl } from "./parse-references"
import { parseConcordanceTtl } from "./parse-concordances"
import { parsePersonTtl } from "./parse-persons"
import { collectUnmappedProvinces } from "./province-mapping"
import { buildContextLine } from "./context-line"
import type { Person, PersonSummary, ReferenceMaps, Concordance } from "./types"

// Path from site/src/data/ to repo root (3 levels up)
// Using process.cwd() as fallback since import.meta.dirname may point
// to bundled output at build time rather than source directory.
const REPO_ROOT = join(process.cwd(), "..")

let _cache: Promise<{ persons: Person[]; refs: ReferenceMaps }> | null = null

async function readTtl(path: string): Promise<string> {
  return readFile(join(REPO_ROOT, path), "utf-8")
}

async function loadAllPersonFiles(): Promise<string[]> {
  const personsDir = join(REPO_ROOT, "persons")
  const gensDirs = await readdir(personsDir)

  // Collect all file paths first, then read in parallel
  const filePaths: string[] = []
  for (const gens of gensDirs) {
    const gensPath = join(personsDir, gens)
    const files = await readdir(gensPath)
    for (const file of files) {
      if (file.endsWith(".ttl")) {
        filePaths.push(join(gensPath, file))
      }
    }
  }

  // Read files in batches to avoid file descriptor exhaustion on CI
  // (~4,900 files, ~4KB each; default FD limit on many CI systems is 1024)
  const BATCH_SIZE = 200
  const results: string[] = []
  for (let i = 0; i < filePaths.length; i += BATCH_SIZE) {
    const batch = filePaths.slice(i, i + BATCH_SIZE)
    const batchResults = await Promise.all(
      batch.map((fp) => readFile(fp, "utf-8"))
    )
    results.push(...batchResults)
  }
  return results
}

async function loadConcordances(): Promise<Map<string, Concordance[]>> {
  const concordDir = join(REPO_ROOT, "concordances")
  const files = await readdir(concordDir)
  const merged = new Map<string, Concordance[]>()

  for (const file of files) {
    if (!file.endsWith(".ttl")) continue
    const system = basename(file, ".ttl")
    const content = await readFile(join(concordDir, file), "utf-8")
    const parsed = parseConcordanceTtl(system, content)
    for (const [personId, links] of parsed) {
      const existing = merged.get(personId) ?? []
      existing.push(...links)
      merged.set(personId, existing)
    }
  }

  return merged
}

/** Score a person record by how much data it carries, most-significant field first. */
function richness(p: Person): number {
  return (
    p.postAssertions.length * 1000 +
    p.relationships.length +
    p.dateInformation.length +
    p.personNotes.length
  )
}

/**
 * Deduplicate persons parsed from multiple TTL files (a person can appear
 * as a related-person stub in relatives' files). Keeps the richest record
 * per ID, then sorts by ID.
 */
export function dedupePersons(persons: Person[]): Person[] {
  const byId = new Map<string, Person>()
  for (const p of persons) {
    const existing = byId.get(p.id)
    if (!existing || richness(p) > richness(existing)) {
      byId.set(p.id, p)
    }
  }

  return [...byId.values()].sort((a, b) => a.id.localeCompare(b.id))
}

export function loadAllData(): Promise<{
  persons: Person[]
  refs: ReferenceMaps
}> {
  _cache ??= loadAllDataUncached().catch((err: unknown) => {
    _cache = null
    throw err
  })
  return _cache
}

async function loadAllDataUncached(): Promise<{
  persons: Person[]
  refs: ReferenceMaps
}> {
  // 1. Parse reference files
  const [offices, sources, praenomina, tribes, relationships, misc, provinces] =
    await Promise.all([
      readTtl("reference/offices.ttl"),
      readTtl("reference/sources.ttl"),
      readTtl("reference/praenomina.ttl"),
      readTtl("reference/tribes.ttl"),
      readTtl("reference/relationships.ttl"),
      readTtl("reference/misc.ttl"),
      readTtl("reference/provinces.ttl"),
    ])

  const refs = await parseReferenceTtl({
    offices,
    sources,
    praenomina,
    tribes,
    relationships,
    misc,
    provinces,
  })

  // 2. Parse concordances
  const concordanceMap = await loadConcordances()

  // 3. Parse all person files
  const personTtls = await loadAllPersonFiles()
  const allPersons: Person[] = []

  for (const ttl of personTtls) {
    const persons = parsePersonTtl(ttl, refs, concordanceMap)
    allPersons.push(...persons)
  }

  const persons = dedupePersons(allPersons)

  // 4. Second pass: resolve cross-file relationship references.
  // Relationships may point to persons in other TTL files whose
  // names/IDs were unavailable during the first parse. The parser
  // stores the raw person URI in relatedPersonId when it can't
  // resolve the name locally. Now we can resolve using the full set.
  const personByUri = new Map<string, Person>()
  for (const p of persons) {
    personByUri.set(p.uri, p)
  }
  for (const p of persons) {
    for (const rel of p.relationships) {
      if (rel.relatedPersonName && rel.relatedPersonId) continue
      // relatedPersonId may contain a raw URI if unresolved
      const resolved = personByUri.get(rel.relatedPersonId)
      if (resolved) {
        rel.relatedPersonId = resolved.id
        rel.relatedPersonName = resolved.name
      }
    }
  }

  // Surface province curation gaps: raw strings not in the curated mapping
  // are excluded from the facet but still displayed on person pages.
  const rawProvinceTexts = persons.flatMap((p) =>
    p.postAssertions
      .map((pa) => pa.provinceOriginal)
      .filter((v): v is string => v !== null)
  )
  const unmapped = collectUnmappedProvinces(rawProvinceTexts)
  if (unmapped.length > 0) {
    console.warn(
      `[data] ${unmapped.length} unmapped province strings (excluded from facet):`,
      unmapped.join("; ")
    )
  }

  return { persons, refs }
}

/** Extract compact summaries for search/faceting. */
export function toSummaries(persons: Person[]): PersonSummary[] {
  const byId = new Map(persons.map((p) => [p.id, p]))
  return persons.map((p) => ({
    id: p.id,
    name: p.name,
    praenomen: p.praenomen,
    nomen: p.nomen,
    cognomen: p.cognomen,
    otherNames: p.otherNames,
    sex: p.sex,
    highestOffice: p.highestOffice,
    eraFrom: p.eraFrom,
    eraTo: p.eraTo,
    tribes: p.tribes,
    offices: p.offices,
    provinces: p.provinces,
    reNumber: p.reNumber,
    filiation: p.filiation,
    lifeEvents: p.lifeEvents,
    statuses: p.statuses,
    father: p.father,
    grandfather: p.grandfather,
    contextLine: buildContextLine(p, byId),
  }))
}
