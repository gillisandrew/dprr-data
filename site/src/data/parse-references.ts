// site/src/data/parse-references.ts
import { Parser } from "n3"
import type { ReferenceMaps } from "./types"

const DPRR = "http://romanrepublic.ac.uk/rdf/ontology#"

interface RawTtlInputs {
  offices: string
  sources: string
  praenomina: string
  tribes: string
  relationships: string
  misc: string
  provinces: string
}

function parseTtl(ttl: string) {
  const parser = new Parser()
  return parser.parse(ttl)
}

export async function parseReferenceTtl(
  inputs: RawTtlInputs
): Promise<ReferenceMaps> {
  const offices = new Map<
    string,
    { name: string; abbreviation: string | null; parent: string | null }
  >()
  const sources = new Map<
    string,
    {
      name: string
      abbreviation: string | null
      biblio: string | null
    }
  >()
  const praenomina = new Map<string, string>()
  const tribes = new Map<
    string,
    { name: string; abbreviation: string | null }
  >()
  const relationships = new Map<string, string>()
  const noteTypes = new Map<string, string>()
  const dateTypes = new Map<string, string>()
  const sexes = new Map<string, string>()
  const statuses = new Map<
    string,
    { name: string; abbreviation: string | null }
  >()
  const provinces = new Map<string, { name: string; parent: string | null }>()

  // Group quads by subject for each file
  function groupBySubject(quads: ReturnType<typeof parseTtl>) {
    const map = new Map<string, Map<string, string>>()
    for (const q of quads) {
      const subj = q.subject.value
      if (!map.has(subj)) map.set(subj, new Map())
      map.get(subj)!.set(q.predicate.value, q.object.value)
    }
    return map
  }

  // Offices
  if (inputs.offices) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.offices))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        offices.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
          parent: props.get(`${DPRR}hasParent`) ?? null,
        })
      }
    }
  }

  // Sources
  if (inputs.sources) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.sources))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        sources.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
          biblio: props.get(`${DPRR}hasBiblio`) ?? null,
        })
      }
    }
  }

  // Praenomina
  if (inputs.praenomina) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.praenomina))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) praenomina.set(uri, name)
    }
  }

  // Tribes
  if (inputs.tribes) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.tribes))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        tribes.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
        })
      }
    }
  }

  // Relationships
  if (inputs.relationships) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.relationships))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) relationships.set(uri, name)
    }
  }

  // Provinces
  if (inputs.provinces) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.provinces))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        provinces.set(uri, {
          name,
          parent: props.get(`${DPRR}hasParent`) ?? null,
        })
      }
    }
  }

  // Misc: Sex, NoteType, DateType, Status
  if (inputs.misc) {
    const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    const quads = parseTtl(inputs.misc)
    const grouped = new Map<
      string,
      { type: string | null; props: Map<string, string> }
    >()
    for (const q of quads) {
      const subj = q.subject.value
      if (!grouped.has(subj))
        grouped.set(subj, { type: null, props: new Map() })
      const entry = grouped.get(subj)!
      if (q.predicate.value === RDF_TYPE) {
        entry.type = q.object.value
      } else {
        entry.props.set(q.predicate.value, q.object.value)
      }
    }
    for (const [uri, { type, props }] of grouped) {
      const name = props.get(`${DPRR}hasName`)
      if (!name) continue
      if (type === `${DPRR}Sex`) {
        sexes.set(uri, name)
      } else if (type === `${DPRR}NoteType`) {
        noteTypes.set(uri, name)
      } else if (type === `${DPRR}DateType`) {
        dateTypes.set(uri, name)
      } else if (type === `${DPRR}Status`) {
        statuses.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
        })
      }
    }
  }

  return {
    offices,
    sources,
    praenomina,
    tribes,
    relationships,
    noteTypes,
    dateTypes,
    sexes,
    statuses,
    provinces,
  }
}
