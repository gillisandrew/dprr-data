import { expect, test, describe } from "vite-plus/test"
import { parseSearchParams, toSearchParams } from "./search"

describe("search param round-trip", () => {
  test("province parses from comma-separated param", () => {
    const state = parseSearchParams({ province: "Sicilia,Asia" })
    expect(state.province).toEqual(["Sicilia", "Asia"])
  })

  test("province serializes back to the URL", () => {
    const state = parseSearchParams({})
    expect(state.province).toEqual([])
    const params = toSearchParams({ ...state, province: ["Sicilia"] })
    expect(params.province).toBe("Sicilia")
  })

  test("facet values containing a comma survive the URL round-trip", () => {
    const state = parseSearchParams({})
    const params = toSearchParams({ ...state, office: ["a, b office"] })
    expect(params.office).toBe(encodeURIComponent("a, b office"))
    expect(parseSearchParams(params).office).toEqual(["a, b office"])
  })

  test("full round-trip preserves all facets", () => {
    const input = {
      q: "brutus",
      office: "consul,praetor",
      province: "Sicilia",
      tribe: "Fabia",
      sex: "Male",
      patrician: "true",
      eraFrom: "-200",
      eraTo: "-100",
    }
    expect(toSearchParams(parseSearchParams(input))).toEqual({
      ...input,
      nomen: undefined,
      nobilis: undefined,
    })
  })
})

describe("advanced params round-trip", () => {
  test("new multi-value and scalar params parse and serialize", () => {
    const input = {
      event: "death%20-%20violent,exiled",
      praenomen: "Lucius",
      cognomen: "Brutus",
      re: "46a",
      officeMode: "all",
      officeInRange: "true",
      sort: "latest",
    }
    const state = parseSearchParams(input)
    expect(state.event).toEqual(["death - violent", "exiled"])
    expect(state.officeMode).toBe("all")
    expect(state.officeInRange).toBe(true)
    expect(state.sort).toBe("latest")
    expect(toSearchParams(state)).toEqual(input)
  })

  test("defaults are omitted from the URL", () => {
    const state = parseSearchParams({})
    expect(state.officeMode).toBe("any")
    expect(state.officeInRange).toBe(false)
    expect(state.sort).toBeNull()
    const params = toSearchParams(state)
    expect(params.officeMode).toBeUndefined()
    expect(params.officeInRange).toBeUndefined()
    expect(params.sort).toBeUndefined()
  })

  test("unknown sort values parse as null", () => {
    expect(parseSearchParams({ sort: "bogus" }).sort).toBeNull()
  })
})
