import { expect, test, describe } from "vite-plus/test"
import { buildSearchPayload, buildSearchIndexPayload } from "./search-payload"
import type { Person, PostAssertion, ReferenceMaps } from "./types"

function emptyRefMaps(): ReferenceMaps {
  return {
    offices: new Map(),
    sources: new Map(),
    praenomina: new Map(),
    tribes: new Map(),
    relationships: new Map(),
    noteTypes: new Map(),
    dateTypes: new Map(),
    sexes: new Map(),
    statuses: new Map(),
    provinces: new Map(),
  }
}

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

describe("buildSearchPayload", () => {
  test("builds office table and career tuples per person", () => {
    const consul = makeAssertion({
      id: "pa1",
      officeName: "consul",
      dateStart: -100,
      dateEnd: -100,
    })
    const praetor = makeAssertion({
      id: "pa2",
      officeName: "praetor",
      dateStart: -104,
      dateEnd: null,
    })
    const p = makePerson({
      id: "AAAA0001",
      postAssertions: [praetor, consul],
      offices: ["praetor", "consul"],
    })
    const payload = buildSearchPayload([p], emptyRefMaps())
    expect(payload.officeNames.sort()).toEqual(["consul", "praetor"])
    const tuples = payload.careers["AAAA0001"]
    expect(tuples).toHaveLength(2)
    const byOffice = Object.fromEntries(
      tuples.map(([idx, s, e]) => [payload.officeNames[idx], [s, e]])
    )
    expect(byOffice["consul"]).toEqual([-100, -100])
    expect(byOffice["praetor"]).toEqual([-104, null])
    expect(payload.summaries[0].id).toBe("AAAA0001")
    expect(payload.histogram.counts.reduce((a, b) => a + b, 0)).toBe(2)
  })

  test("assertions with empty office names are excluded from tuples", () => {
    const anon = makeAssertion({ id: "pa3", officeName: "", dateStart: -50 })
    const p = makePerson({ id: "BBBB0001", postAssertions: [anon] })
    const payload = buildSearchPayload([p], emptyRefMaps())
    expect(payload.careers["BBBB0001"]).toBeUndefined()
  })
})

describe("buildSearchIndexPayload", () => {
  test("emits a loadable index and serializable options", () => {
    const p = makePerson({ id: "CCCC0001", name: "CCCC0001 C. Cornelius" })
    const { index, options } = buildSearchIndexPayload([p])
    expect(options.idField).toBe("id")
    expect(typeof index).toBe("object")
  })
})
