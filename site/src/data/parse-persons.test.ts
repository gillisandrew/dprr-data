// site/src/data/parse-persons.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parsePersonTtl } from "./parse-persons"
import type { ReferenceMaps } from "./types"

const SIMPLE_PERSON_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1459> rdfs:label "Post Assertion: #1459" ;
  a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1375> ;
  dprr:hasSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> ;
  dprr:hasOriginalText "C. Aburius (1)" ;
  dprr:hasOffice <http://romanrepublic.ac.uk/rdf/entity/Office/17> ;
  dprr:hasDateStart -171 ;
  dprr:hasDateEnd -171 ;
  dprr:hasDateSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1375> a dprr:Person ;
  dprr:isSex <http://romanrepublic.ac.uk/rdf/entity/Sex/Male> ;
  dprr:hasPraenomen <http://romanrepublic.ac.uk/rdf/entity/Praenomen/Gaius> ;
  dprr:hasPersonName "ABUR1375 C. Aburius (1)" ;
  dprr:hasNomen "Aburius" ;
  dprr:hasHighestOffice "leg. 171" ;
  dprr:hasFiliation "" ;
  dprr:hasEraTo -171 ;
  dprr:hasEraFrom -171 ;
  dprr:hasDprrID "ABUR1375" .
`

function makeRefs(): ReferenceMaps {
  return {
    offices: new Map([
      [
        "http://romanrepublic.ac.uk/rdf/entity/Office/17",
        { name: "legatus", abbreviation: "leg.", parent: null },
      ],
    ]),
    sources: new Map([
      [
        "http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1",
        {
          name: "Broughton MRR",
          abbreviation: "MRR",
          biblio: null,
        },
      ],
    ]),
    praenomina: new Map([
      [
        "http://romanrepublic.ac.uk/rdf/entity/Praenomen/Gaius",
        { name: "Gaius", abbreviation: "C." },
      ],
    ]),
    tribes: new Map(),
    relationships: new Map(),
    noteTypes: new Map(),
    dateTypes: new Map(),
    sexes: new Map([
      ["http://romanrepublic.ac.uk/rdf/entity/Sex/Male", "Male"],
    ]),
    statuses: new Map([
      [
        "http://romanrepublic.ac.uk/rdf/entity/Status/1",
        { name: "eques Romanus", abbreviation: null },
      ],
    ]),
    provinces: new Map(),
  }
}

describe("parsePersonTtl", () => {
  test("extracts basic person fields", () => {
    const persons = parsePersonTtl(SIMPLE_PERSON_TTL, makeRefs(), new Map())
    expect(persons).toHaveLength(1)
    const p = persons[0]
    expect(p.id).toBe("ABUR1375")
    expect(p.nomen).toBe("Aburius")
    expect(p.praenomen).toBe("Gaius")
    expect(p.sex).toBe("Male")
    expect(p.eraFrom).toBe(-171)
    expect(p.eraTo).toBe(-171)
  })

  test("resolves post assertions", () => {
    const persons = parsePersonTtl(SIMPLE_PERSON_TTL, makeRefs(), new Map())
    const p = persons[0]
    expect(p.postAssertions).toHaveLength(1)
    expect(p.postAssertions[0].officeName).toBe("legatus")
    expect(p.postAssertions[0].dateStart).toBe(-171)
    expect(p.postAssertions[0].secondarySource).toBe("Broughton MRR")
  })

  test("builds offices facet array", () => {
    const persons = parsePersonTtl(SIMPLE_PERSON_TTL, makeRefs(), new Map())
    expect(persons[0].offices).toEqual(["legatus"])
  })
})

describe("tribe assertions", () => {
  test("resolves tribes from TribeAssertion entities", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/TribeAssertion/1> a dprr:TribeAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasTribe <http://romanrepublic.ac.uk/rdf/entity/Tribe/23> .
<http://romanrepublic.ac.uk/rdf/entity/TribeAssertion/2> a dprr:TribeAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasTribe <http://romanrepublic.ac.uk/rdf/entity/Tribe/24> .
`
    const refs = makeRefs()
    refs.tribes.set("http://romanrepublic.ac.uk/rdf/entity/Tribe/23", {
      name: "Fabia",
      abbreviation: "Fab.",
    })
    refs.tribes.set("http://romanrepublic.ac.uk/rdf/entity/Tribe/24", {
      name: "Cornelia",
      abbreviation: "Cor.",
    })
    const persons = parsePersonTtl(ttl, refs, new Map())
    expect(persons[0].tribes).toEqual(["Fabia", "Cornelia"])
  })
})

describe("province extraction", () => {
  test("resolves provinceOriginal through the curated mapping", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasProvinceOriginal "Sicily" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasProvinceOriginal "not-a-real-province" .
`
    const persons = parsePersonTtl(ttl, makeRefs(), new Map())
    expect(persons).toHaveLength(1)
    const byOriginal = Object.fromEntries(
      persons[0].postAssertions.map((pa) => [pa.provinceOriginal, pa])
    )
    expect(byOriginal["Sicily"].provinces).toEqual(["Sicilia"])
    expect(byOriginal["not-a-real-province"].provinces).toEqual([])
    expect(persons[0].provinces).toEqual(["Sicilia"])
  })
})

describe("life events", () => {
  test("derives distinct life events from date information, excluding 'attested'", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/DateInformation/1> a dprr:DateInformation ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateType <http://romanrepublic.ac.uk/rdf/entity/DateType/4> ;
  dprr:hasValue "-42"^^xsd:integer .
<http://romanrepublic.ac.uk/rdf/entity/DateInformation/2> a dprr:DateInformation ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateType <http://romanrepublic.ac.uk/rdf/entity/DateType/4> ;
  dprr:hasValue "-42"^^xsd:integer .
<http://romanrepublic.ac.uk/rdf/entity/DateInformation/3> a dprr:DateInformation ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateType <http://romanrepublic.ac.uk/rdf/entity/DateType/1> ;
  dprr:hasValue "-60"^^xsd:integer .
`
    const refs = makeRefs()
    refs.dateTypes.set(
      "http://romanrepublic.ac.uk/rdf/entity/DateType/4",
      "death - violent"
    )
    refs.dateTypes.set(
      "http://romanrepublic.ac.uk/rdf/entity/DateType/1",
      "attested"
    )
    const persons = parsePersonTtl(ttl, refs, new Map())
    expect(persons[0].lifeEvents).toEqual(["death - violent"])
  })
})

describe("uncertainty and career order", () => {
  test("reads uncertainty flags and sorts assertions chronologically", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateStart "-100"^^xsd:integer ;
  dprr:isUncertain true ;
  dprr:isDateStartUncertain true .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateStart "-200"^^xsd:integer .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/3> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> .
`
    const persons = parsePersonTtl(ttl, makeRefs(), new Map())
    const pas = persons[0].postAssertions
    // Chronological: -200 first, -100 second, undated last
    expect(pas.map((pa) => pa.dateStart)).toEqual([-200, -100, null])
    expect(pas[1].isUncertain).toBe(true)
    expect(pas[1].isDateStartUncertain).toBe(true)
    expect(pas[1].isDateEndUncertain).toBe(false)
    expect(pas[0].isUncertain).toBe(false)
  })
})

describe("statuses and ancestors", () => {
  test("parses StatusAssertions and isNovus into statuses", () => {
    const ttl = `
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertion/1> a dprr:StatusAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasStatus <http://romanrepublic.ac.uk/rdf/entity/Status/1> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" ;
  dprr:isPatrician "true" ;
  dprr:isNovus "true" .
`
    const [person] = parsePersonTtl(ttl, makeRefs(), new Map())
    expect(person.statusAssertions.map((sa) => sa.statusName)).toEqual([
      "eques Romanus",
    ])
    expect(person.isNovus).toBe(true)
    expect(person.statuses).toEqual(["Patrician", "Novus", "Eques Romanus"])
  })

  test("parses father and grandfather from filiation", () => {
    const ttl = `
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" ;
  dprr:hasFiliation "Q. f. Ser. n." .
`
    const [person] = parsePersonTtl(ttl, makeRefs(), new Map())
    expect(person.father).toBe("Quintus")
    expect(person.grandfather).toBe("Servius")
    expect(person.contextLine).toBeNull()
  })
})

const ENRICHED_TTL = `@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/42> a dprr:Person ;
  dprr:hasDprrID "TEST0042" ;
  dprr:hasNomen "Testius" ;
  dprr:isNomenUncertain true ;
  dprr:isCognomenUncertain true ;
  dprr:hasOrigin "Tusculum" ;
  dprr:isNovus true ;
  dprr:hasNovusNotes "Cic. Mur. 17" ;
  dprr:hasEraFrom -100 ;
  dprr:hasEraTo -50 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasPosition 2 ;
  dprr:hasOfficeXref "Pr. 66" ;
  dprr:hasDateSourceText "before Kal. Ian." ;
  dprr:hasDateStart -66 ; dprr:hasDateEnd -66 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasPosition 1 ;
  dprr:hasDateStart -63 ; dprr:hasDateEnd -63 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/3> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasDateStart -70 ; dprr:hasDateEnd -70 .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertion/9> a dprr:StatusAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasStatus <http://romanrepublic.ac.uk/rdf/entity/Status/2> ;
  dprr:hasDateStart -70 ; dprr:hasDateEnd -65 ;
  dprr:isDateStartUncertain true ;
  dprr:hasSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> ;
  dprr:hasStatusAssertionNote <http://romanrepublic.ac.uk/rdf/entity/StatusAssertionNote/5> .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertionNote/5> a dprr:StatusAssertionNote ;
  dprr:hasNoteText "listed among the equites" ;
  dprr:hasSecondarySourceForNote <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> .
`

function makeEnrichedRefs(): ReferenceMaps {
  const refs = makeRefs()
  refs.statuses.set("http://romanrepublic.ac.uk/rdf/entity/Status/2", {
    name: "eques Romanus",
    abbreviation: null,
  })
  refs.sources.set("http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1", {
    name: "Broughton MRR",
    abbreviation: "MRR",
    biblio: null,
  })
  return refs
}

const TIE_TTL = `@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/43> a dprr:Person ;
  dprr:hasDprrID "TEST0043" ;
  dprr:hasNomen "Testius" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/11> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/43> ;
  dprr:hasPosition 1 ;
  dprr:hasDateStart -60 ; dprr:hasDateEnd -60 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/12> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/43> ;
  dprr:hasPosition 1 ;
  dprr:hasDateStart -70 ; dprr:hasDateEnd -70 .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertion/13> a dprr:StatusAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/43> ;
  dprr:hasStatus <http://romanrepublic.ac.uk/rdf/entity/Status/2> ;
  dprr:isUncertain true .
`

describe("career position, Broughton labels, and status details", () => {
  test("equal positions fall back to chronological order", () => {
    const [p] = parsePersonTtl(TIE_TTL, makeEnrichedRefs(), new Map())
    expect(p.postAssertions.map((pa) => pa.dateStart)).toEqual([-70, -60])
  })

  test("status assertion isUncertain parses", () => {
    const [p] = parsePersonTtl(TIE_TTL, makeEnrichedRefs(), new Map())
    expect(p.statusAssertions).toHaveLength(1)
    expect(p.statusAssertions[0].isUncertain).toBe(true)
    expect(p.statusAssertions[0].dateStart).toBeNull()
  })

  test("career sorts by position, positionless fall back chronologically after", () => {
    const [p] = parsePersonTtl(ENRICHED_TTL, makeEnrichedRefs(), new Map())
    expect(p.postAssertions.map((pa) => pa.id)).toEqual([
      "http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2", // position 1
      "http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1", // position 2
      "http://romanrepublic.ac.uk/rdf/entity/PostAssertion/3", // no position
    ])
    expect(p.postAssertions[1].officeXref).toBe("Pr. 66")
    expect(p.postAssertions[1].dateSourceText).toBe("before Kal. Ian.")
    expect(p.postAssertions[0].officeXref).toBeNull()
  })

  test("status assertions carry dates, uncertainty, source, and notes", () => {
    const [p] = parsePersonTtl(ENRICHED_TTL, makeEnrichedRefs(), new Map())
    expect(p.statusAssertions).toHaveLength(1)
    const sa = p.statusAssertions[0]
    expect(sa.statusName).toBe("eques Romanus")
    expect(sa.dateStart).toBe(-70)
    expect(sa.isDateStartUncertain).toBe(true)
    expect(sa.notes[0].text).toBe("listed among the equites")
    // statuses summary still derives capitalized names + boolean flags
    expect(p.statuses).toContain("Eques Romanus")
    expect(p.statuses).toContain("Novus")
  })

  test("origin, novusNotes, and name-part uncertainty flags parse", () => {
    const [p] = parsePersonTtl(ENRICHED_TTL, makeEnrichedRefs(), new Map())
    expect(p.origin).toBe("Tusculum")
    expect(p.novusNotes).toBe("Cic. Mur. 17")
    expect(p.isNomenUncertain).toBe(true)
    expect(p.isCognomenUncertain).toBe(true)
    expect(p.isPraenomenUncertain).toBe(false)
  })
})

const TRIBE_REL_TTL = `@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/50> a dprr:Person ;
  dprr:hasDprrID "TEST0050" ;
  dprr:hasNomen "Testius" .
<http://romanrepublic.ac.uk/rdf/entity/Person/51> a dprr:Person ;
  dprr:hasDprrID "TEST0051" ;
  dprr:hasPersonName "TEST0051 T. Testius Junior" ;
  dprr:hasNomen "Testius" .
<http://romanrepublic.ac.uk/rdf/entity/TribeAssertion/7> a dprr:TribeAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/50> ;
  dprr:hasTribe <http://romanrepublic.ac.uk/rdf/entity/Tribe/38> ;
  dprr:hasSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> ;
  dprr:hasNotes "p187." ;
  dprr:isUncertain true .
<http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/8> a dprr:RelationshipAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/50> ;
  dprr:isUncertain true ;
  dprr:hasRelationship <http://romanrepublic.ac.uk/rdf/entity/Relationship/3> ;
  dprr:hasRelatedPerson <http://romanrepublic.ac.uk/rdf/entity/Person/51> .
`

describe("tribe assertion records and relationship uncertainty", () => {
  function tribeRefs(): ReferenceMaps {
    const refs = makeRefs()
    refs.tribes.set("http://romanrepublic.ac.uk/rdf/entity/Tribe/38", {
      name: "Camilia",
      abbreviation: "Cam.",
    })
    refs.relationships.set(
      "http://romanrepublic.ac.uk/rdf/entity/Relationship/3",
      { name: "father of", orderNumber: 1, inverseName: null }
    )
    return refs
  }

  test("tribe assertions carry source, notes, and uncertainty", () => {
    const persons = parsePersonTtl(TRIBE_REL_TTL, tribeRefs(), new Map())
    const p = persons.find((x) => x.id === "TEST0050")!
    expect(p.tribeAssertions).toEqual([
      {
        tribeName: "Camilia",
        secondarySource: "Broughton MRR",
        notes: "p187.",
        isUncertain: true,
      },
    ])
    // Flat facet list still derives from the same assertions
    expect(p.tribes).toEqual(["Camilia"])
  })

  test("relationship isUncertain parses", () => {
    const persons = parsePersonTtl(TRIBE_REL_TTL, tribeRefs(), new Map())
    const p = persons.find((x) => x.id === "TEST0050")!
    expect(p.relationships).toHaveLength(1)
    expect(p.relationships[0].isUncertain).toBe(true)
  })
})

describe("relationship order numbers", () => {
  test("parses relationshipNumber and typeOrderNumber from the reference map", () => {
    const ttl = `
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/1> a dprr:RelationshipAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasRelationship <http://romanrepublic.ac.uk/rdf/entity/Relationship/12> ;
  dprr:hasRelationshipNumber 2 .
<http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/2> a dprr:RelationshipAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasRelationship <http://romanrepublic.ac.uk/rdf/entity/Relationship/12> ;
  dprr:hasRelationshipNumber 1 .
`
    const refs = makeRefs()
    refs.relationships.set(
      "http://romanrepublic.ac.uk/rdf/entity/Relationship/12",
      { name: "son of", orderNumber: 3, inverseName: null }
    )
    const [p] = parsePersonTtl(ttl, refs, new Map())
    const byId = Object.fromEntries(p.relationships.map((r) => [r.id, r]))
    expect(
      byId["http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/1"]
        .relationshipNumber
    ).toBe(2)
    expect(
      byId["http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/2"]
        .relationshipNumber
    ).toBe(1)
    expect(
      byId["http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/1"]
        .typeOrderNumber
    ).toBe(3)
  })
})
