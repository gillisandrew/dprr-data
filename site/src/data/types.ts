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
  isPatrician: boolean
  isNobilis: boolean
  highestOffice: string | null
  eraFrom: number | null
  eraTo: number | null
  tribe: string | null
  /** Flattened list of office names held (for faceting). */
  offices: string[]
}

/** Full person record with all scholarly data. */
export interface Person extends PersonSummary {
  uri: string
  filiation: string | null
  reNumber: string | null
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
  secondarySource: string
  notes: PostAssertionNote[]
  primarySourceRefs: string[]
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
  patrician: boolean | null
  nobilis: boolean | null
  tribe: string[]
  eraFrom: number | null
  eraTo: number | null
}
