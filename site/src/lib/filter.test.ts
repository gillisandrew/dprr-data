import { expect, test, describe } from "vite-plus/test"
import { descendantSet, matchesFacets, type FilterContext } from "./filter"
import { parseSearchParams } from "./search-params"
import type { PersonSummary } from "@/data/types"

const parentOf = {
  "Magisterial Posts": null,
  consul: "Magisterial Posts",
  "consul suffectus": "consul",
  praetor: "Magisterial Posts",
}

function makeSummary(over: Partial<PersonSummary>): PersonSummary {
  return {
    id: "TEST0001",
    name: "TEST0001 T. Testius",
    praenomen: "Titus",
    nomen: "Testius",
    cognomen: null,
    otherNames: null,
    sex: "Male",
    highestOffice: null,
    eraFrom: -120,
    eraTo: -80,
    tribes: [],
    offices: [],
    provinces: [],
    reNumber: null,
    filiation: null,
    lifeEvents: [],
    statuses: [],
    father: null,
    grandfather: null,
    contextLine: null,
    ...over,
  }
}

function ctx(over: Partial<FilterContext> = {}): FilterContext {
  return { parentOf, careers: {}, officeNames: [], ...over }
}

function state(params: Record<string, string>) {
  return parseSearchParams(params)
}

describe("descendantSet", () => {
  test("includes the value and all descendants", () => {
    expect(descendantSet("consul", parentOf)).toEqual(
      new Set(["consul", "consul suffectus"])
    )
    expect(descendantSet("Magisterial Posts", parentOf)).toEqual(
      new Set(["Magisterial Posts", "consul", "consul suffectus", "praetor"])
    )
    expect(descendantSet("unknown", parentOf)).toEqual(new Set(["unknown"]))
  })
})

describe("office matching", () => {
  const suffect = makeSummary({ offices: ["consul suffectus"] })

  test("subtree: selecting a parent matches descendant holders", () => {
    expect(matchesFacets(suffect, state({ office: "consul" }), ctx())).toBe(
      true
    )
    expect(
      matchesFacets(suffect, state({ office: "Magisterial%20Posts" }), ctx())
    ).toBe(true)
    expect(matchesFacets(suffect, state({ office: "praetor" }), ctx())).toBe(
      false
    )
  })

  test("officeMode all requires every selection", () => {
    const both = makeSummary({ offices: ["consul", "praetor"] })
    const one = makeSummary({ offices: ["consul"] })
    const s = state({ office: "consul,praetor", officeMode: "all" })
    expect(matchesFacets(both, s, ctx())).toBe(true)
    expect(matchesFacets(one, s, ctx())).toBe(false)
    // default OR still matches
    expect(matchesFacets(one, state({ office: "consul,praetor" }), ctx())).toBe(
      true
    )
  })

  test("officeInRange matches at the assertion level", () => {
    const p = makeSummary({ offices: ["consul"], eraFrom: -150, eraTo: -80 })
    const c = ctx({
      officeNames: ["consul"],
      careers: { TEST0001: [[0, -140, -140]] },
    })
    const inRange = state({
      office: "consul",
      eraFrom: "-145",
      eraTo: "-135",
      officeInRange: "true",
    })
    const outOfRange = state({
      office: "consul",
      eraFrom: "-100",
      eraTo: "-90",
      officeInRange: "true",
    })
    const personLevel = state({
      office: "consul",
      eraFrom: "-100",
      eraTo: "-90",
    })
    expect(matchesFacets(p, inRange, c)).toBe(true)
    expect(matchesFacets(p, outOfRange, c)).toBe(false)
    // without the toggle, person-level era overlap still matches
    expect(matchesFacets(p, personLevel, c)).toBe(true)
  })

  test("officeInRange ignores undated assertions", () => {
    const p = makeSummary({ offices: ["consul"] })
    const c = ctx({
      officeNames: ["consul"],
      careers: { TEST0001: [[0, null, null]] },
    })
    const s = state({
      office: "consul",
      eraFrom: "-145",
      eraTo: "-135",
      officeInRange: "true",
    })
    expect(matchesFacets(p, s, c)).toBe(false)
  })
})

describe("status, father, grandfather facets", () => {
  test("status facet requires every selected status", () => {
    const patricianSenator = makeSummary({
      statuses: ["Patrician", "Senator"],
    })
    const plainSenator = makeSummary({ id: "TEST0002", statuses: ["Senator"] })
    const s = state({ status: "Patrician,Senator" })
    expect(matchesFacets(patricianSenator, s, ctx())).toBe(true)
    expect(matchesFacets(plainSenator, s, ctx())).toBe(false)
  })

  test("father and grandfather facets match parsed ancestors", () => {
    const person = makeSummary({ father: "Quintus", grandfather: "Servius" })
    expect(matchesFacets(person, state({ father: "Quintus" }), ctx())).toBe(
      true
    )
    expect(matchesFacets(person, state({ father: "Lucius" }), ctx())).toBe(
      false
    )
    expect(
      matchesFacets(
        makeSummary({ father: null }),
        state({ father: "Quintus" }),
        ctx()
      )
    ).toBe(false)
    expect(
      matchesFacets(person, state({ grandfather: "Servius" }), ctx())
    ).toBe(true)
  })
})

describe("events and name parts", () => {
  test("life events filter disjunctively", () => {
    const p = makeSummary({ lifeEvents: ["exiled"] })
    expect(matchesFacets(p, state({ event: "exiled,proscribed" }), ctx())).toBe(
      true
    )
    expect(matchesFacets(p, state({ event: "proscribed" }), ctx())).toBe(false)
  })

  test("praenomen, cognomen, and RE substring", () => {
    const p = makeSummary({
      praenomen: "Lucius",
      cognomen: "Brutus",
      reNumber: "RE 46a",
    })
    expect(matchesFacets(p, state({ praenomen: "Lucius" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ praenomen: "Gaius" }), ctx())).toBe(false)
    expect(matchesFacets(p, state({ cognomen: "Brutus" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ re: "46A" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ re: "99" }), ctx())).toBe(false)
  })
})
