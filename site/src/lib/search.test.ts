import { expect, test, describe } from "vite-plus/test"
import {
  parseSearchParams,
  toSearchParams,
  normalizeState,
} from "./search-params"
import { orderByQueryRank, computeFacetValues } from "./search"
import { matchesFacets, type FilterContext } from "./filter"
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

  test("whitespace-only RE number is inert everywhere", () => {
    expect(parseSearchParams({ re: "  " }).re).toBe("")
    const state = parseSearchParams({})
    expect(toSearchParams({ ...state, re: "  " }).re).toBeUndefined()
    expect(toSearchParams({ ...state, re: " 46a " }).re).toBe("46a")
  })

  test("full round-trip preserves all facets", () => {
    const input = {
      q: "brutus",
      office: "consul,praetor",
      province: "Sicilia",
      tribe: "Fabia",
      sex: "Male",
      status: "Patrician",
      eraFrom: "-200",
      eraTo: "-100",
    }
    expect(toSearchParams(parseSearchParams(input))).toEqual({
      ...input,
      nomen: undefined,
    })
  })
})

describe("status, father, grandfather round-trip", () => {
  test("round-trips status, father, grandfather", () => {
    const state = parseSearchParams({
      status: "Patrician,Eques%20Romanus",
      father: "Quintus",
      grandfather: "Servius",
    })
    expect(state.status).toEqual(["Patrician", "Eques Romanus"])
    expect(state.father).toEqual(["Quintus"])
    expect(state.grandfather).toEqual(["Servius"])
    const params = toSearchParams(state)
    expect(params.status).toBe("Patrician,Eques%20Romanus")
    expect(params.father).toBe("Quintus")
    expect(params.grandfather).toBe("Servius")
  })

  test("legacy patrician/nobilis params alias into status", () => {
    const state = parseSearchParams({ patrician: "true", nobilis: "true" })
    expect(state.status).toEqual(["Patrician", "Nobilis"])
    const params = toSearchParams(state)
    expect(params.status).toBe("Patrician,Nobilis")
    expect(params.patrician).toBeUndefined()
    expect(params.nobilis).toBeUndefined()
  })

  test("combining the new status param with the legacy alias dedupes", () => {
    const state = parseSearchParams({ status: "Patrician", patrician: "true" })
    expect(state.status).toEqual(["Patrician"])
    expect(toSearchParams(state).status).toBe("Patrician")
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

  test("a boolean patrician param parses into the status facet", () => {
    expect(
      parseSearchParams({ patrician: true as unknown as string }).status
    ).toEqual(["Patrician"])
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

describe("conjunctive status facet counts", () => {
  // Mirrors the `facets.status` computation in useSearchState: counting the
  // relaxed (status-ignored) candidate set — as every other, disjunctive,
  // facet does via countWith — would show Eques Romanus's count as if
  // Senator weren't selected. Since matchesAllStatuses is an AND, the
  // correct "count if added" is computeFacetValues over the persons that
  // already satisfy the full, un-relaxed state.
  const ctx: FilterContext = { parentOf: {}, careers: {}, officeNames: [] }

  test("status counts reflect AND semantics, not the fully-relaxed OR count", () => {
    const persons = [
      makeSummary({ id: "A", statuses: ["Senator", "Eques Romanus"] }),
      makeSummary({ id: "B", statuses: ["Senator"] }),
      makeSummary({ id: "C", statuses: ["Eques Romanus"] }),
    ]
    const state = parseSearchParams({ status: "Senator" })

    // Disjunctive (relaxed) counting, as the other facets use — this is
    // the misleading count the fix replaces for `status`.
    const relaxedCounts = computeFacetValues(persons, "statuses")
    expect(relaxedCounts.find((f) => f.value === "Eques Romanus")?.count).toBe(
      2
    )

    // Conjunctive (un-relaxed) counting: only persons who already satisfy
    // the full current state (Senator selected) are counted, so Eques
    // Romanus correctly shows 1 (A), not 2.
    const fullyMatching = persons.filter((p) => matchesFacets(p, state, ctx))
    const conjunctiveCounts = computeFacetValues(fullyMatching, "statuses")
    expect(
      conjunctiveCounts.find((f) => f.value === "Eques Romanus")?.count
    ).toBe(1)
    // The already-selected value collapses to the current result count.
    expect(conjunctiveCounts.find((f) => f.value === "Senator")?.count).toBe(2)
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
