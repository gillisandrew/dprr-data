import { expect, test, describe } from "vite-plus/test"
import {
  buildOfficeIndex,
  buildOfficeDetail,
  buildTribeIndex,
  buildTribeDetail,
  buildProvinceIndex,
  buildProvinceDetail,
} from "./aggregate-references"
import type { Person, PostAssertion } from "./types"

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
    nobilisNotes: null,
    highestOffice: null,
    eraFrom: null,
    eraTo: null,
    tribe: null,
    offices: [],
    provinces: [],
    postAssertions: [],
    relationships: [],
    dateInformation: [],
    personNotes: [],
    concordances: [],
    ...over,
  }
}

const consul100 = makeAssertion({
  id: "pa-consul-100",
  officeName: "consul",
  officeAbbreviation: "cos.",
  dateStart: -100,
  dateEnd: -100,
  provinceOriginal: "Sicily",
  provinces: ["Sicilia"],
})
const consul90 = makeAssertion({
  id: "pa-consul-90",
  officeName: "consul",
  officeAbbreviation: "cos.",
  dateStart: -90,
  dateEnd: -90,
})

const personA = makePerson({
  id: "AAAA0001",
  name: "AAAA0001 A. Aulus",
  tribe: "Fabia",
  offices: ["consul"],
  provinces: ["Sicilia"],
  postAssertions: [consul100],
})
const personB = makePerson({
  id: "BBBB0001",
  name: "BBBB0001 B. Brutus",
  offices: ["consul"],
  postAssertions: [consul90],
})

describe("offices", () => {
  test("index lists offices alphabetically with distinct-person counts", () => {
    const index = buildOfficeIndex([personA, personB])
    expect(index).toEqual([
      { slug: "consul", name: "consul", abbreviation: "cos.", holderCount: 2 },
    ])
  })

  test("detail sorts holders chronologically", () => {
    const detail = buildOfficeDetail([personB, personA], "consul")
    expect(detail?.holders.map((h) => h.personId)).toEqual([
      "AAAA0001",
      "BBBB0001",
    ])
    expect(detail?.holders[0].dateStart).toBe(-100)
  })

  test("unknown slug returns null", () => {
    expect(buildOfficeDetail([personA], "praetor")).toBeNull()
  })
})

describe("tribes", () => {
  test("index and detail", () => {
    expect(buildTribeIndex([personA, personB])).toEqual([
      { slug: "fabia", name: "Fabia", memberCount: 1 },
    ])
    const detail = buildTribeDetail([personA, personB], "fabia")
    expect(detail?.members.map((m) => m.id)).toEqual(["AAAA0001"])
    // members are summaries — no heavy fields
    expect(detail?.members[0]).not.toHaveProperty("postAssertions")
  })
})

describe("provinces", () => {
  test("index counts assertions and detail lists them chronologically", () => {
    expect(buildProvinceIndex([personA, personB])).toEqual([
      { slug: "sicilia", name: "Sicilia", assertionCount: 1 },
    ])
    const detail = buildProvinceDetail([personA, personB], "sicilia")
    expect(detail?.assertions).toEqual([
      {
        personId: "AAAA0001",
        personName: "AAAA0001 A. Aulus",
        officeName: "consul",
        dateStart: -100,
        dateEnd: -100,
      },
    ])
  })
})
