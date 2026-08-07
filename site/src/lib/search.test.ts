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
