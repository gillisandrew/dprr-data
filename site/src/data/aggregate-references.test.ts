import { expect, test, describe } from "vite-plus/test"
import {
  buildOfficeIndex,
  buildOfficeDetail,
  buildTribeIndex,
  buildTribeDetail,
  buildGensIndex,
  buildGensDetail,
  buildProvinceIndex,
  buildProvinceDetail,
  buildNameHierarchy,
  categoryOf,
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
  tribes: ["Fabia"],
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
    const index = buildOfficeIndex([personA, personB], {})
    expect(index).toEqual([
      {
        slug: "consul",
        name: "consul",
        abbreviation: "cos.",
        holderCount: 2,
        category: "consul",
      },
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

const personC = makePerson({
  id: "CCCC0001",
  name: "CCCC0001 C. Aulius",
  nomen: "Aulius",
})
const personD = makePerson({
  id: "DDDD0001",
  name: "DDDD0001 D. Noname",
  nomen: "",
})

describe("gentes", () => {
  test("index lists distinct gentes alphabetically with member counts, excluding blanks", () => {
    expect(buildGensIndex([personA, personB, personC, personD])).toEqual([
      { slug: "aulius", name: "Aulius", memberCount: 1 },
      { slug: "testius", name: "Testius", memberCount: 2 },
    ])
  })

  test("detail lists members sorted by name", () => {
    const detail = buildGensDetail([personA, personB, personC], "testius")
    expect(detail?.name).toBe("Testius")
    expect(detail?.members.map((m) => m.id)).toEqual(["AAAA0001", "BBBB0001"])
    // members are summaries — no heavy fields
    expect(detail?.members[0]).not.toHaveProperty("postAssertions")
  })

  test("unknown slug returns null", () => {
    expect(buildGensDetail([personA], "nosuchgens")).toBeNull()
  })

  test("excludes nomina that slugify to empty (e.g. the bare '-')", () => {
    const personDash = makePerson({
      id: "FFFF0001",
      name: "FFFF0001 F. Nomen",
      nomen: "-",
    })
    const index = buildGensIndex([personA, personB, personDash])
    expect(index.some((g) => g.slug === "")).toBe(false)
    expect(index.map((g) => g.name)).not.toContain("-")
    // The person is simply excluded from the gens index/detail — they
    // still exist and are reachable via their own person page.
    expect(buildGensDetail([personA, personDash], "")).toBeNull()
  })

  test("merges nomina that differ only by leading/trailing whitespace", () => {
    const personTrailing = makePerson({
      id: "GGGG0001",
      name: "GGGG0001 G. Antestius",
      nomen: "Antestius ",
    })
    const personClean = makePerson({
      id: "HHHH0001",
      name: "HHHH0001 H. Antestius",
      nomen: "Antestius",
    })
    const index = buildGensIndex([personTrailing, personClean])
    expect(index).toEqual([
      { slug: "antestius", name: "Antestius", memberCount: 2 },
    ])
    const detail = buildGensDetail([personTrailing, personClean], "antestius")
    expect(detail?.members.map((m) => m.id).sort()).toEqual([
      "GGGG0001",
      "HHHH0001",
    ])
  })

  test("disambiguates gentes whose names slugify identically", () => {
    // Uncertain-attribution variants like "(Testius)" slugify the same as
    // the plain name — real data has this for e.g. Cornelius/(Cornelius).
    const personE = makePerson({
      id: "EEEE0001",
      name: "EEEE0001 E. Testius",
      nomen: "(Testius)",
    })
    const index = buildGensIndex([personA, personB, personE])
    const slugs = index.map((g) => g.slug)
    expect(new Set(slugs).size).toBe(slugs.length)

    const plain = index.find((g) => g.name === "Testius")
    const uncertain = index.find((g) => g.name === "(Testius)")
    expect(plain).toEqual({ slug: "testius", name: "Testius", memberCount: 2 })
    expect(uncertain?.memberCount).toBe(1)
    expect(uncertain?.slug).not.toBe("testius")

    const plainDetail = buildGensDetail(
      [personA, personB, personE],
      plain?.slug as string
    )
    expect(plainDetail?.members.map((m) => m.id)).toEqual([
      "AAAA0001",
      "BBBB0001",
    ])
    const uncertainDetail = buildGensDetail(
      [personA, personB, personE],
      uncertain?.slug as string
    )
    expect(uncertainDetail?.members.map((m) => m.id)).toEqual(["EEEE0001"])
  })
})

describe("provinces", () => {
  test("index counts distinct persons and detail lists assertions chronologically", () => {
    expect(buildProvinceIndex([personA, personB])).toEqual([
      { slug: "sicilia", name: "Sicilia", personCount: 1 },
    ])
    const detail = buildProvinceDetail([personA, personB], "sicilia")
    expect(detail?.assertions).toEqual([
      {
        personId: "AAAA0001",
        personName: "AAAA0001 A. Aulus",
        officeName: "consul",
        dateStart: -100,
        dateEnd: -100,
        isUncertain: false,
      },
    ])
  })
})

describe("hierarchy", () => {
  const refMap = new Map([
    ["uri:root", { name: "Magisterial Posts", parent: null }],
    ["uri:consul", { name: "consul", parent: "uri:root" }],
    ["uri:suff", { name: "consul suffectus", parent: "uri:consul" }],
  ])

  test("buildNameHierarchy maps child names to parent names", () => {
    expect(buildNameHierarchy(refMap)).toEqual({
      "Magisterial Posts": null,
      consul: "Magisterial Posts",
      "consul suffectus": "consul",
    })
  })

  test("categoryOf walks to the root", () => {
    const h = buildNameHierarchy(refMap)
    expect(categoryOf("consul suffectus", h)).toBe("Magisterial Posts")
    expect(categoryOf("consul", h)).toBe("Magisterial Posts")
    expect(categoryOf("unknown office", h)).toBe("unknown office")
  })

  test("office index carries the category", () => {
    const h = buildNameHierarchy(refMap)
    const index = buildOfficeIndex([personA, personB], h)
    expect(index[0].category).toBe("Magisterial Posts")
  })
})
