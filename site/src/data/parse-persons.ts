// site/src/data/parse-persons.ts
import { Parser } from "n3"
import { mapProvinceText } from "./province-mapping"
import type {
  Person,
  PostAssertion,
  PostAssertionNote,
  Relationship,
  RelationshipReference,
  DateInfo,
  Note,
  Concordance,
  ReferenceMaps,
} from "./types"

const DPRR = "http://romanrepublic.ac.uk/rdf/ontology#"
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
const PERSON_TYPE = `${DPRR}Person`
const POST_ASSERTION_TYPE = `${DPRR}PostAssertion`
const POST_ASSERTION_NOTE_TYPE = `${DPRR}PostAssertionNote`
const RELATIONSHIP_ASSERTION_TYPE = `${DPRR}RelationshipAssertion`
const RELATIONSHIP_REF_TYPE = `${DPRR}RelationshipAssertionReference`
const DATE_INFO_TYPE = `${DPRR}DateInformation`
const PERSON_NOTE_TYPE = `${DPRR}PersonNote`
const PRIMARY_SOURCE_REF_TYPE = `${DPRR}PrimarySourceReference`
const PERSON_PREFIX = "http://romanrepublic.ac.uk/rdf/entity/Person/"

interface QuadGroup {
  type: string | null
  props: Map<string, string[]>
}

function groupBySubject(
  quads: {
    subject: { value: string }
    predicate: { value: string }
    object: { value: string }
  }[]
): Map<string, QuadGroup> {
  const map = new Map<string, QuadGroup>()
  for (const q of quads) {
    const subj = q.subject.value
    if (!map.has(subj)) map.set(subj, { type: null, props: new Map() })
    const entry = map.get(subj)!
    if (q.predicate.value === RDF_TYPE) {
      entry.type = q.object.value
    } else {
      const pred = q.predicate.value
      if (!entry.props.has(pred)) entry.props.set(pred, [])
      entry.props.get(pred)!.push(q.object.value)
    }
  }
  return map
}

function first(group: QuadGroup, pred: string): string | null {
  return group.props.get(`${DPRR}${pred}`)?.[0] ?? null
}

function firstNum(group: QuadGroup, pred: string): number | null {
  const v = first(group, pred)
  if (v === null) return null
  const n = Number(v)
  return Number.isNaN(n) ? null : n
}

function all(group: QuadGroup, pred: string): string[] {
  return group.props.get(`${DPRR}${pred}`) ?? []
}

/**
 * Parse a person TTL file and return fully resolved Person records.
 * A single TTL file may contain multiple Person entities (e.g., a related
 * person whose data is co-located).
 */
export function parsePersonTtl(
  ttl: string,
  refs: ReferenceMaps,
  concordanceMap: Map<string, Concordance[]>
): Person[] {
  const parser = new Parser()
  const quads = parser.parse(ttl)
  const grouped = groupBySubject(quads)

  // Collect auxiliary entities by type
  const postAssertionGroups = new Map<string, QuadGroup>()
  const postAssertionNoteGroups = new Map<string, QuadGroup>()
  const relationshipGroups = new Map<string, QuadGroup>()
  const relationshipRefGroups = new Map<string, QuadGroup>()
  const dateInfoGroups = new Map<string, QuadGroup>()
  const personNoteGroups = new Map<string, QuadGroup>()
  const primarySourceRefGroups = new Map<string, QuadGroup>()
  const personGroups = new Map<string, QuadGroup>()

  for (const [uri, group] of grouped) {
    switch (group.type) {
      case PERSON_TYPE:
        personGroups.set(uri, group)
        break
      case POST_ASSERTION_TYPE:
        postAssertionGroups.set(uri, group)
        break
      case POST_ASSERTION_NOTE_TYPE:
        postAssertionNoteGroups.set(uri, group)
        break
      case RELATIONSHIP_ASSERTION_TYPE:
        relationshipGroups.set(uri, group)
        break
      case RELATIONSHIP_REF_TYPE:
        relationshipRefGroups.set(uri, group)
        break
      case DATE_INFO_TYPE:
        dateInfoGroups.set(uri, group)
        break
      case PERSON_NOTE_TYPE:
        personNoteGroups.set(uri, group)
        break
      case PRIMARY_SOURCE_REF_TYPE:
        primarySourceRefGroups.set(uri, group)
        break
    }
  }

  // Resolve a source URI to a display name
  function resolveSource(uri: string | null): string {
    if (!uri) return ""
    return refs.sources.get(uri)?.name ?? uri
  }

  // Build PostAssertionNote from a note URI
  function buildPANote(noteUri: string): PostAssertionNote | null {
    const g = postAssertionNoteGroups.get(noteUri)
    if (!g) return null
    const noteTypeUri = first(g, "hasNoteType")
    return {
      type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
      text: first(g, "hasNoteText") ?? "",
      secondarySource: resolveSource(first(g, "hasSecondarySourceForNote")),
      extraInfo: first(g, "hasExtraInfo"),
    }
  }

  // Build PostAssertions for a person URI
  function buildPostAssertions(personUri: string): PostAssertion[] {
    const results: PostAssertion[] = []
    for (const [paUri, g] of postAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const officeUri = first(g, "hasOffice")
      const office = officeUri ? refs.offices.get(officeUri) : null

      // Collect notes
      const noteUris = all(g, "hasPostAssertionNote")
      const notes = noteUris
        .map(buildPANote)
        .filter((n): n is PostAssertionNote => n !== null)

      // Collect primary source refs
      const primaryRefs: string[] = []
      for (const [, psrGroup] of primarySourceRefGroups) {
        if (first(psrGroup, "forAssertion") === paUri) {
          const text = first(psrGroup, "hasNoteText")
          if (text) primaryRefs.push(text)
        }
      }

      // The export carries only free-text province strings; the curated
      // mapping in province-mapping.ts resolves them to canonical names.
      const provinceOriginal =
        first(g, "hasProvinceOriginal") ??
        first(g, "hasProvinceOriginalExpanded")
      const provinceExpanded = first(g, "hasProvinceOriginalExpanded")
      const provinces = [
        ...new Set(
          [provinceOriginal, provinceExpanded]
            .filter((v): v is string => v !== null)
            .flatMap((v) => mapProvinceText(v) ?? [])
        ),
      ]

      results.push({
        id: paUri,
        officeName: office?.name ?? "",
        officeAbbreviation: office?.abbreviation ?? null,
        dateStart: firstNum(g, "hasDateStart"),
        dateEnd: firstNum(g, "hasDateEnd"),
        dateSecondarySource: resolveSource(first(g, "hasDateSecondarySource")),
        originalText: first(g, "hasOriginalText"),
        provinceOriginal,
        provinces,
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
        notes,
        primarySourceRefs: primaryRefs,
      })
    }
    return results
  }

  // Build RelationshipReference from a ref URI
  function buildRelRef(refUri: string): RelationshipReference | null {
    const g = relationshipRefGroups.get(refUri)
    if (!g) return null
    const noteTypeUri = first(g, "hasNoteType")
    return {
      type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
      extraInfo: first(g, "hasExtraInfo"),
      secondarySource: resolveSource(first(g, "hasSecondarySourceForNote")),
    }
  }

  // Build Relationships for a person URI
  function buildRelationships(personUri: string): Relationship[] {
    const results: Relationship[] = []
    for (const [raUri, g] of relationshipGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const relTypeUri = first(g, "hasRelationship")
      const relatedUri = first(g, "hasRelatedPerson")

      // Resolve related person name from co-located entities.
      // If the related person is in a different file, store the raw
      // URI in relatedPersonId — the loader's second pass will resolve it.
      let relatedPersonId = relatedUri ?? ""
      let relatedPersonName = ""
      if (relatedUri) {
        const relatedGroup = personGroups.get(relatedUri)
        if (relatedGroup) {
          relatedPersonName = first(relatedGroup, "hasPersonName") ?? ""
          relatedPersonId = first(relatedGroup, "hasDprrID") ?? relatedUri
        }
      }

      const refUris = all(g, "hasRelationshipReference")
      const references = refUris
        .map(buildRelRef)
        .filter((r): r is RelationshipReference => r !== null)

      results.push({
        id: raUri,
        relationshipType:
          (relTypeUri && refs.relationships.get(relTypeUri)) ?? "",
        relatedPersonId,
        relatedPersonName,
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
        references,
      })
    }
    return results
  }

  // Build DateInformation for a person URI
  function buildDateInfo(personUri: string): DateInfo[] {
    const results: DateInfo[] = []
    for (const [, g] of dateInfoGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const dateTypeUri = first(g, "hasDateType")
      results.push({
        type: (dateTypeUri && refs.dateTypes.get(dateTypeUri)) ?? "",
        value: firstNum(g, "hasValue") ?? 0,
        interval: first(g, "hasDateInterval") || null,
        isUncertain: first(g, "isUncertain") === "true",
        notes: first(g, "hasNotes"),
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
      })
    }
    return results
  }

  // Build PersonNotes for a person URI
  function buildPersonNotes(personUri: string): Note[] {
    const noteUris = personGroups.get(personUri)
      ? all(personGroups.get(personUri)!, "hasPersonNote")
      : []
    return noteUris
      .map((uri) => {
        const g = personNoteGroups.get(uri)
        if (!g) return null
        const noteTypeUri = first(g, "hasNoteType")
        return {
          type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
          text: first(g, "hasNoteText") ?? "",
          secondarySource: resolveSource(first(g, "hasSecondarySourceForNote")),
        }
      })
      .filter((n): n is Note => n !== null)
  }

  // Build Person records
  const persons: Person[] = []
  for (const [personUri, g] of personGroups) {
    const dprrId = first(g, "hasDprrID")
    if (!dprrId) continue

    const sexUri = first(g, "isSex")
    const praenomenUri = first(g, "hasPraenomen")
    const tribeUri = first(g, "hasTribe")
    const personNumericId = personUri.startsWith(PERSON_PREFIX)
      ? personUri.slice(PERSON_PREFIX.length)
      : ""

    const postAssertions = buildPostAssertions(personUri)
    const officeNames = [
      ...new Set(postAssertions.map((pa) => pa.officeName).filter(Boolean)),
    ]
    const provinceNames = [
      ...new Set(postAssertions.flatMap((pa) => pa.provinces)),
    ]

    const filiation = first(g, "hasFiliation")

    persons.push({
      id: dprrId,
      uri: personUri,
      name: first(g, "hasPersonName") ?? dprrId,
      praenomen: (praenomenUri && refs.praenomina.get(praenomenUri)) ?? "",
      nomen: first(g, "hasNomen") ?? "",
      cognomen: first(g, "hasCognomen"),
      otherNames: first(g, "hasOtherNames"),
      filiation: filiation || null,
      reNumber: first(g, "hasReNumber"),
      sex: ((sexUri && refs.sexes.get(sexUri)) ?? "Male") as "Male" | "Female",
      isPatrician: first(g, "isPatrician") === "true",
      isNobilis: first(g, "isNobilis") === "true",
      nobilisNotes: first(g, "hasNobilisNotes"),
      highestOffice: first(g, "hasHighestOffice"),
      eraFrom: firstNum(g, "hasEraFrom"),
      eraTo: firstNum(g, "hasEraTo"),
      tribe: (tribeUri && refs.tribes.get(tribeUri)?.name) ?? null,
      offices: officeNames,
      provinces: provinceNames,
      postAssertions,
      relationships: buildRelationships(personUri),
      dateInformation: buildDateInfo(personUri),
      personNotes: buildPersonNotes(personUri),
      concordances: concordanceMap.get(personNumericId) ?? [],
    })
  }

  return persons
}
