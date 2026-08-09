import { expect, test, describe } from "vite-plus/test"
import {
  parseSearchParams,
  toSearchParams,
  normalizeState,
} from "./search-params"
import { orderByQueryRank, computeFacetValues } from "./search"
import type { PersonSummary } from "@/data/types"

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
    eraFrom: null,
    eraTo: null,
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

describe("parseSearchParams coerces non-string values", () => {
  // TanStack Router JSON-parses the search string, so `?q=509` arrives here
  // as the number 509, not the string "509" — parseSearchParams used to
  // call `.trim()` on it directly and crash.
  test("a numeric-looking q param doesn't crash and parses as a string", () => {
    expect(parseSearchParams({ q: 509 as unknown as string }).q).toBe("509")
  })

  test("a boolean patrician param parses as boolean true", () => {
    expect(
      parseSearchParams({ patrician: true as unknown as string }).patrician
    ).toBe(true)
  })

  test("null/undefined values are omitted rather than coerced to strings", () => {
    const state = parseSearchParams({
      q: null as unknown as string,
      nomen: undefined as unknown as string,
    })
    expect(state.q).toBe("")
    expect(state.nomen).toEqual([])
  })
})

describe("computeFacetValues", () => {
  test("sorts by count descending, then alphabetically for ties", () => {
    const persons = [
      makeSummary({ id: "A", offices: ["consul"] }),
      makeSummary({ id: "B", offices: ["praetor"] }),
      makeSummary({ id: "C", offices: ["aedile"] }),
      makeSummary({ id: "D", offices: ["consul"] }),
    ]
    expect(computeFacetValues(persons, "offices")).toEqual([
      { value: "consul", count: 2 },
      { value: "aedile", count: 1 },
      { value: "praetor", count: 1 },
    ])
  })
})

describe("normalizeState", () => {
  test("drops an explicit relevance sort once the query is empty", () => {
    const state = parseSearchParams({ sort: "relevance" })
    expect(normalizeState({ ...state, q: "" }).sort).toBeNull()
  })

  test("drops relevance when the query is whitespace-only", () => {
    const state = parseSearchParams({ sort: "relevance" })
    expect(normalizeState({ ...state, q: "   " }).sort).toBeNull()
  })

  test("leaves relevance sort intact while a query is active", () => {
    const state = parseSearchParams({ q: "brutus", sort: "relevance" })
    expect(normalizeState(state).sort).toBe("relevance")
  })

  test("leaves non-relevance sorts untouched regardless of query", () => {
    const state = parseSearchParams({ sort: "latest" })
    expect(normalizeState({ ...state, q: "" }).sort).toBe("latest")
  })

  test("is a no-op when there is nothing to reconcile", () => {
    const state = parseSearchParams({ q: "brutus" })
    expect(normalizeState(state)).toEqual(state)
  })
})

describe("orderByQueryRank", () => {
  test("preserves MiniSearch relevance order, not the candidates' input order", () => {
    const candidates = [{ id: "A" }, { id: "B" }, { id: "C" }]
    // MiniSearch ranks C highest, then A, then B — the opposite of ID order.
    const searchResults = [{ id: "C" }, { id: "A" }, { id: "B" }]
    expect(
      orderByQueryRank(candidates, searchResults).map((c) => c.id)
    ).toEqual(["C", "A", "B"])
  })

  test("drops candidates absent from the search results", () => {
    const candidates = [{ id: "A" }, { id: "B" }]
    expect(
      orderByQueryRank(candidates, [{ id: "B" }]).map((c) => c.id)
    ).toEqual(["B"])
  })
})
