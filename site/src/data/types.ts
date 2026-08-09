// site/src/data/types.ts

/** Compact person record for search results and faceting. */
export interface PersonSummary {
  id: string
  name: string
  praenomen: string
  nomen: string
  cognomen: string | null
  otherNames: string | null
  sex: "Male" | "Female"
  highestOffice: string | null
  eraFrom: number | null
  eraTo: number | null
  /** Tribe names from TribeAssertion entities (multiple = differing source claims). */
  tribes: string[]
  /** Flattened list of office names held (for faceting). */
  offices: string[]
  /** Distinct canonical provinces across all post assertions (for faceting). */
  provinces: string[]
  reNumber: string | null
  filiation: string | null
  /** Distinct DateInformation type names (e.g. "death - violent"), excluding "attested". */
  lifeEvents: string[]
  /** Display statuses for faceting: Patrician, Nobilis, Novus, Eques Romanus, Senator. */
  statuses: string[]
  /** Father's / grandfather's praenomen parsed from the filiation string. */
  father: string | null
  grandfather: string | null
  /** "father of Ap. Claudius (321), cos. 495" for career-less persons; null otherwise. */
  contextLine: string | null
}

/** Full person record with all scholarly data. */
export interface Person extends PersonSummary {
  uri: string
  isPatrician: boolean
  isNobilis: boolean
  isNovus: boolean
  /** Raw StatusAssertion names ("eques Romanus", "senator"). */
  statusAssertions: string[]
  nobilisNotes: string | null
  postAssertions: PostAssertion[]
  relationships: Relationship[]
  dateInformation: DateInfo[]
  personNotes: Note[]
  concordances: Concordance[]
}

export interface PostAssertion {
  id: string
  officeName: string
  officeAbbreviation: string | null
  dateStart: number | null
  dateEnd: number | null
  dateSecondarySource: string | null
  originalText: string | null
  /** Raw province text from the secondary source (may be unmapped). */
  provinceOriginal: string | null
  /** Canonical province names resolved via the curated mapping. */
  provinces: string[]
  secondarySource: string
  notes: PostAssertionNote[]
  primarySourceRefs: string[]
  /** True when the source scholarship marks this post itself as uncertain. */
  isUncertain: boolean
  isDateStartUncertain: boolean
  isDateEndUncertain: boolean
}

export interface PostAssertionNote {
  type: string
  text: string
  secondarySource: string
  extraInfo: string | null
}

export interface Relationship {
  id: string
  relationshipType: string
  relatedPersonId: string
  relatedPersonName: string
  secondarySource: string
  references: RelationshipReference[]
}

export interface RelationshipReference {
  type: string
  extraInfo: string | null
  secondarySource: string
}

export interface DateInfo {
  type: string
  value: number
  interval: string | null
  isUncertain: boolean
  notes: string | null
  secondarySource: string
}

export interface Note {
  type: string
  text: string
  secondarySource: string
}

export interface Concordance {
  system: string
  uri: string
  predicate: "owl:sameAs" | "skos:exactMatch"
}

/** Lookup maps built from reference/*.ttl files. */
export interface ReferenceMaps {
  offices: Map<
    string,
    { name: string; abbreviation: string | null; parent: string | null }
  >
  sources: Map<
    string,
    { name: string; abbreviation: string | null; biblio: string | null }
  >
  praenomina: Map<string, string>
  tribes: Map<string, { name: string; abbreviation: string | null }>
  relationships: Map<string, string>
  noteTypes: Map<string, string>
  dateTypes: Map<string, string>
  sexes: Map<string, string>
  statuses: Map<string, { name: string; abbreviation: string | null }>
  provinces: Map<string, { name: string; parent: string | null }>
}

/** A single facet value with its count. */
export interface FacetValue {
  value: string
  count: number
}

/** Active search/filter state derived from URL query params. */
export interface SearchState {
  q: string
  office: string[]
  nomen: string[]
  sex: string[]
  /** AND-semantics status facet (Patrician, Nobilis, Novus, Eques Romanus, Senator). */
  status: string[]
  father: string[]
  grandfather: string[]
  tribe: string[]
  province: string[]
  eraFrom: number | null
  eraTo: number | null
  event: string[]
  praenomen: string[]
  cognomen: string[]
  /** Case-insensitive substring match against reNumber. */
  re: string
  officeMode: "any" | "all"
  officeInRange: boolean
  sort: "earliest" | "latest" | "name" | "relevance" | null
}
