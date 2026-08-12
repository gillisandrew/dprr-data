// site/src/data/parse-persons.ts
import { mapProvinceText } from "./province-mapping"
import { parseFiliation } from "./parse-filiation"
import { DPRR, all, first, firstNum, groupSubjects } from "./ttl"
import type { QuadGroup } from "./ttl"
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

const PERSON_TYPE = `${DPRR}Person`
const POST_ASSERTION_TYPE = `${DPRR}PostAssertion`
const POST_ASSERTION_NOTE_TYPE = `${DPRR}PostAssertionNote`
const RELATIONSHIP_ASSERTION_TYPE = `${DPRR}RelationshipAssertion`
const RELATIONSHIP_REF_TYPE = `${DPRR}RelationshipAssertionReference`
const DATE_INFO_TYPE = `${DPRR}DateInformation`
const TRIBE_ASSERTION_TYPE = `${DPRR}TribeAssertion`
const STATUS_ASSERTION_TYPE = `${DPRR}StatusAssertion`
const PERSON_NOTE_TYPE = `${DPRR}PersonNote`
const PRIMARY_SOURCE_REF_TYPE = `${DPRR}PrimarySourceReference`
const PERSON_PREFIX = "http://romanrepublic.ac.uk/rdf/entity/Person/"

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
  const grouped = groupSubjects(ttl)

  // Collect auxiliary entities by type
  const postAssertionGroups = new Map<string, QuadGroup>()
  const postAssertionNoteGroups = new Map<string, QuadGroup>()
  const relationshipGroups = new Map<string, QuadGroup>()
  const relationshipRefGroups = new Map<string, QuadGroup>()
  const dateInfoGroups = new Map<string, QuadGroup>()
  const tribeAssertionGroups = new Map<string, QuadGroup>()
  const statusAssertionGroups = new Map<string, QuadGroup>()
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
      case TRIBE_ASSERTION_TYPE:
        tribeAssertionGroups.set(uri, group)
        break
      case STATUS_ASSERTION_TYPE:
        statusAssertionGroups.set(uri, group)
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

  // Shared shape of PersonNote and PostAssertionNote entities
  function buildNoteFields(g: QuadGroup): Note {
    const noteTypeUri = first(g, "hasNoteType")
    return {
      type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
      text: first(g, "hasNoteText") ?? "",
      secondarySource: resolveSource(first(g, "hasSecondarySourceForNote")),
    }
  }

  // Build PostAssertionNote from a note URI
  function buildPANote(noteUri: string): PostAssertionNote | null {
    const g = postAssertionNoteGroups.get(noteUri)
    if (!g) return null
    return { ...buildNoteFields(g), extraInfo: first(g, "hasExtraInfo") }
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
        isUncertain: first(g, "isUncertain") === "true",
        isDateStartUncertain: first(g, "isDateStartUncertain") === "true",
        isDateEndUncertain: first(g, "isDateEndUncertain") === "true",
      })
    }
    // Chronological career order (undated entries last), matching DPRR
    results.sort(
      (a, b) =>
        (a.dateStart ?? a.dateEnd ?? Number.MAX_SAFE_INTEGER) -
        (b.dateStart ?? b.dateEnd ?? Number.MAX_SAFE_INTEGER)
    )
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
          (relTypeUri && refs.relationships.get(relTypeUri)?.name) ?? "",
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
        return g ? buildNoteFields(g) : null
      })
      .filter((n): n is Note => n !== null)
  }

  // Build tribes for a person URI from TribeAssertion entities
  function buildTribes(personUri: string): string[] {
    const names: string[] = []
    for (const [, g] of tribeAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const tribeUri = first(g, "hasTribe")
      const name = tribeUri ? refs.tribes.get(tribeUri)?.name : null
      if (name && !names.includes(name)) names.push(name)
    }
    return names
  }

  // Build raw status names for a person URI from StatusAssertion entities
  function buildStatusAssertions(personUri: string): string[] {
    const names: string[] = []
    for (const [, g] of statusAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const statusUri = first(g, "hasStatus")
      const name = statusUri ? refs.statuses.get(statusUri)?.name : null
      if (name && !names.includes(name)) names.push(name)
    }
    return names
  }

  // Build Person records
  const persons: Person[] = []
  for (const [personUri, g] of personGroups) {
    const dprrId = first(g, "hasDprrID")
    if (!dprrId) continue

    const sexUri = first(g, "isSex")
    const praenomenUri = first(g, "hasPraenomen")
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
    const dateInformation = buildDateInfo(personUri)
    const lifeEvents = [
      ...new Set(
        dateInformation.map((d) => d.type).filter((t) => t && t !== "attested")
      ),
    ]

    const isPatrician = first(g, "isPatrician") === "true"
    const isNobilis = first(g, "isNobilis") === "true"
    const isNovus = first(g, "isNovus") === "true"
    const statusAssertions = buildStatusAssertions(personUri)
    const statuses = [
      ...(isPatrician ? ["Patrician"] : []),
      ...(isNobilis ? ["Nobilis"] : []),
      ...(isNovus ? ["Novus"] : []),
      // "eques Romanus" → "Eques Romanus"
      ...statusAssertions.map((s) => s.charAt(0).toUpperCase() + s.slice(1)),
    ]
    const { father, grandfather } = parseFiliation(filiation || null)

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
      isPatrician,
      isNobilis,
      isNovus,
      statusAssertions,
      statuses,
      father,
      grandfather,
      contextLine: null,
      nobilisNotes: first(g, "hasNobilisNotes"),
      highestOffice: first(g, "hasHighestOffice"),
      eraFrom: firstNum(g, "hasEraFrom"),
      eraTo: firstNum(g, "hasEraTo"),
      tribes: buildTribes(personUri),
      offices: officeNames,
      provinces: provinceNames,
      postAssertions,
      relationships: buildRelationships(personUri),
      dateInformation,
      personNotes: buildPersonNotes(personUri),
      concordances: concordanceMap.get(personNumericId) ?? [],
      lifeEvents,
    })
  }

  return persons
}
