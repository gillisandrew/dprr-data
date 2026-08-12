import { expect, test, describe } from "vite-plus/test"
import { dedupePersons, resolveCrossFileRelationships } from "./loader"
import type { Person, PostAssertion, Relationship } from "./types"

function makeAssertion(over: Partial<PostAssertion>): PostAssertion {
  return {
    id: "pa1",
    officeName: "",
    officeAbbreviation: null,
    dateStart: null,
    dateEnd: null,
    dateSecondarySource: null,
    originalText: null,
    secondarySource: "Broughton MRR",
    notes: [],
    primarySourceRefs: [],
    provinceOriginal: null,
    provinces: [],
    isUncertain: false,
    isDateStartUncertain: false,
    isDateEndUncertain: false,
    position: null,
    officeXref: null,
    dateSourceText: null,
    ...over,
  }
}

function makeRelationship(over: Partial<Relationship>): Relationship {
  return {
    id: "rel1",
    relationshipType: "father",
    relatedPersonId: "TEST0002",
    relatedPersonName: "TEST0002 T. Testius",
    secondarySource: "Broughton MRR",
    references: [],
    typeOrderNumber: null,
    relationshipNumber: null,
    isUncertain: false,
    ...over,
  }
}

function makePerson(over: Partial<Person>): Person {
  return {
    id: "TEST0001",
    uri: "http://example.org/1",
    name: "TEST0001 T. Testius",
    praenomen: "Titus",
    nomen: "Testius",
    cognomen: null,
    otherNames: null,
    filiation: null,
    reNumber: null,
    sex: "Male",
    isPatrician: false,
    isNobilis: false,
    isNovus: false,
    statusAssertions: [],
    statuses: [],
    father: null,
    grandfather: null,
    contextLine: null,
    nobilisNotes: null,
    highestOffice: null,
    eraFrom: null,
    eraTo: null,
    tribes: [],
    offices: [],
    provinces: [],
    lifeEvents: [],
    postAssertions: [],
    relationships: [],
    tribeAssertions: [],
    dateInformation: [],
    personNotes: [],
    concordances: [],
    origin: null,
    novusNotes: null,
    isNomenUncertain: false,
    isCognomenUncertain: false,
    isPraenomenUncertain: false,
    isFiliationUncertain: false,
    isOtherNamesUncertain: false,
    ...over,
  }
}

describe("dedupePersons", () => {
  test("keeps the richer record when postAssertions tie at zero, regardless of parse order", () => {
    const stub = makePerson({ id: "CLUE4548", uri: "http://example.org/stub" })
    const rich = makePerson({
      id: "CLUE4548",
      uri: "http://example.org/rich",
      relationships: [
        makeRelationship({ id: "rel1" }),
        makeRelationship({ id: "rel2" }),
      ],
    })

    const stubFirst = dedupePersons([stub, rich])
    expect(stubFirst).toHaveLength(1)
    expect(stubFirst[0].uri).toBe("http://example.org/rich")
    expect(stubFirst[0].relationships).toHaveLength(2)

    const richFirst = dedupePersons([rich, stub])
    expect(richFirst).toHaveLength(1)
    expect(richFirst[0].uri).toBe("http://example.org/rich")
    expect(richFirst[0].relationships).toHaveLength(2)
  })

  test("more postAssertions still wins over more relationships", () => {
    const fewerAssertionsMoreRelationships = makePerson({
      id: "TEST0003",
      uri: "http://example.org/relationships",
      postAssertions: [makeAssertion({ id: "pa1" })],
      relationships: [
        makeRelationship({ id: "rel1" }),
        makeRelationship({ id: "rel2" }),
        makeRelationship({ id: "rel3" }),
      ],
    })
    const moreAssertions = makePerson({
      id: "TEST0003",
      uri: "http://example.org/assertions",
      postAssertions: [
        makeAssertion({ id: "pa1" }),
        makeAssertion({ id: "pa2" }),
      ],
      relationships: [],
    })

    const result = dedupePersons([
      fewerAssertionsMoreRelationships,
      moreAssertions,
    ])
    expect(result).toHaveLength(1)
    expect(result[0].uri).toBe("http://example.org/assertions")
  })
})

describe("resolveCrossFileRelationships", () => {
  test("resolves a raw-URI relationship target to DPRR ID and name", () => {
    const target = makePerson({
      id: "MINU2466",
      uri: "http://romanrepublic.ac.uk/rdf/entity/Person/2466",
      name: "MINU2466 L. Minucius (38) Vel. Basilus",
    })
    const source = makePerson({
      id: "SATR4945",
      uri: "http://romanrepublic.ac.uk/rdf/entity/Person/4945",
      relationships: [
        makeRelationship({
          relatedPersonId: "http://romanrepublic.ac.uk/rdf/entity/Person/2466",
          relatedPersonName: "",
        }),
      ],
    })
    resolveCrossFileRelationships([source, target])
    expect(source.relationships[0].relatedPersonId).toBe("MINU2466")
    expect(source.relationships[0].relatedPersonName).toBe(
      "MINU2466 L. Minucius (38) Vel. Basilus"
    )
  })

  test("leaves already-resolved relationships untouched", () => {
    const source = makePerson({
      relationships: [makeRelationship({})],
    })
    resolveCrossFileRelationships([source])
    expect(source.relationships[0].relatedPersonId).toBe("TEST0002")
    expect(source.relationships[0].relatedPersonName).toBe(
      "TEST0002 T. Testius"
    )
  })
})
