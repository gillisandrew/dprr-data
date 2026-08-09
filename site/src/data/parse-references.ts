// site/src/data/parse-references.ts
import { DPRR, first, groupSubjects } from "./ttl"
import type { QuadGroup } from "./ttl"
import type { ReferenceMaps } from "./types"

interface RawTtlInputs {
  offices: string
  sources: string
  praenomina: string
  tribes: string
  relationships: string
  misc: string
  provinces: string
}

/** uri → build(name, group) for every subject that has a hasName. */
function namedMap<T>(
  ttl: string,
  build: (name: string, g: QuadGroup) => T
): Map<string, T> {
  const map = new Map<string, T>()
  for (const [uri, g] of groupSubjects(ttl)) {
    const name = first(g, "hasName")
    if (name !== null) map.set(uri, build(name, g))
  }
  return map
}

/** Sex, NoteType, DateType, and Status entities co-located in one file. */
function parseMisc(ttl: string) {
  const sexes = new Map<string, string>()
  const noteTypes = new Map<string, string>()
  const dateTypes = new Map<string, string>()
  const statuses = new Map<
    string,
    { name: string; abbreviation: string | null }
  >()
  for (const [uri, g] of groupSubjects(ttl)) {
    const name = first(g, "hasName")
    if (name === null) continue
    switch (g.type) {
      case `${DPRR}Sex`:
        sexes.set(uri, name)
        break
      case `${DPRR}NoteType`:
        noteTypes.set(uri, name)
        break
      case `${DPRR}DateType`:
        dateTypes.set(uri, name)
        break
      case `${DPRR}Status`:
        statuses.set(uri, { name, abbreviation: first(g, "hasAbbreviation") })
        break
    }
  }
  return { sexes, noteTypes, dateTypes, statuses }
}

export async function parseReferenceTtl(
  inputs: RawTtlInputs
): Promise<ReferenceMaps> {
  return {
    offices: namedMap(inputs.offices, (name, g) => ({
      name,
      abbreviation: first(g, "hasAbbreviation"),
      parent: first(g, "hasParent"),
    })),
    sources: namedMap(inputs.sources, (name, g) => ({
      name,
      abbreviation: first(g, "hasAbbreviation"),
      biblio: first(g, "hasBiblio"),
    })),
    praenomina: namedMap(inputs.praenomina, (name) => name),
    tribes: namedMap(inputs.tribes, (name, g) => ({
      name,
      abbreviation: first(g, "hasAbbreviation"),
    })),
    relationships: namedMap(inputs.relationships, (name) => name),
    provinces: namedMap(inputs.provinces, (name, g) => ({
      name,
      parent: first(g, "hasParent"),
    })),
    ...parseMisc(inputs.misc),
  }
}
