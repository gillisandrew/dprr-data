// site/src/data/context-line.test.ts
import { expect, test, describe } from "vite-plus/test"
import { buildContextLine } from "./context-line"
import type { Person, Relationship } from "./types"

function makePerson(over: Partial<Person>): Person {
  return {
    id: "TEST0001",
    uri: "urn:test:1",
    name: "TEST0001 T. Testius",
    praenomen: "Titus",
    nomen: "Testius",
    cognomen: null,
    otherNames: null,
    sex: "Male",
    isPatrician: false,
    isNobilis: false,
    isNovus: false,
    statusAssertions: [],
    statuses: [],
    father: null,
    grandfather: null,
    contextLine: null,
    highestOffice: null,
    eraFrom: null,
    eraTo: null,
    tribes: [],
    offices: [],
    provinces: [],
    reNumber: null,
    filiation: null,
    lifeEvents: [],
    nobilisNotes: null,
    postAssertions: [],
    relationships: [],
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

function makeRel(over: Partial<Relationship>): Relationship {
  return {
    id: "rel1",
    relationshipType: "father of",
    relatedPersonId: "TEST0002",
    relatedPersonName: "TEST0002 A. Testius",
    secondarySource: "",
    references: [],
    typeOrderNumber: null,
    relationshipNumber: null,
    ...over,
  }
}

describe("buildContextLine", () => {
  test("names the most notable relative with their highest office", () => {
    const relative = makePerson({
      id: "TEST0002",
      name: "TEST0002 A. Testius",
      highestOffice: "cos. 495",
    })
    const person = makePerson({ relationships: [makeRel({})] })
    const byId = new Map([[relative.id, relative]])
    expect(buildContextLine(person, byId)).toBe(
      "father of A. Testius, cos. 495"
    )
  })

  test("null for persons with a career of their own", () => {
    const person = makePerson({
      relationships: [makeRel({})],
      postAssertions: [{} as never],
    })
    expect(buildContextLine(person, new Map())).toBeNull()
  })

  test("null when no relative has an office", () => {
    const relative = makePerson({ id: "TEST0002", highestOffice: null })
    const person = makePerson({ relationships: [makeRel({})] })
    expect(
      buildContextLine(person, new Map([[relative.id, relative]]))
    ).toBeNull()
  })

  test("prefers the earlier-era relative when several qualify", () => {
    const early = makePerson({
      id: "TEST0002",
      name: "TEST0002 A. Testius",
      highestOffice: "cos. 495",
      eraFrom: -500,
    })
    const late = makePerson({
      id: "TEST0003",
      name: "TEST0003 B. Testius",
      highestOffice: "pr. 100",
      eraFrom: -120,
    })
    const person = makePerson({
      relationships: [
        makeRel({ relatedPersonId: "TEST0003", relationshipType: "son of" }),
        makeRel({ relatedPersonId: "TEST0002" }),
      ],
    })
    const byId = new Map([
      [early.id, early],
      [late.id, late],
    ])
    expect(buildContextLine(person, byId)).toBe(
      "father of A. Testius, cos. 495"
    )
  })
})
