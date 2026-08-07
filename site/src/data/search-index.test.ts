// site/src/data/search-index.test.ts
import { expect, test, describe } from "vite-plus/test"
import MiniSearch from "minisearch"
import { buildSearchIndex, MINISEARCH_OPTIONS } from "./search-index"
import type { PersonSummary } from "./types"

const SUMMARIES: PersonSummary[] = [
  {
    id: "IUNI0001",
    name: "IUNI0001 L. Iunius (46a) M. f. Brutus",
    praenomen: "Lucius",
    nomen: "Iunius",
    cognomen: "Brutus",
    otherNames: null,
    sex: "Male",
    isPatrician: true,
    isNobilis: true,
    highestOffice: "cos. 509",
    eraFrom: -540,
    eraTo: -509,
    tribes: [],
    offices: ["consul"],
    provinces: [],
  },
  {
    id: "CORN0123",
    name: "CORN0123 P. Cornelius Scipio Africanus",
    praenomen: "Publius",
    nomen: "Cornelius",
    cognomen: "Scipio Africanus",
    otherNames: null,
    sex: "Male",
    isPatrician: true,
    isNobilis: true,
    highestOffice: "cos. 205",
    eraFrom: -236,
    eraTo: -183,
    tribes: [],
    offices: ["consul", "proconsul"],
    provinces: [],
  },
]

describe("buildSearchIndex", () => {
  test("returns a serializable index", () => {
    const json = buildSearchIndex(SUMMARIES)
    expect(json).toBeDefined()
    // Should be deserializable by MiniSearch
    const ms = MiniSearch.loadJSON(JSON.stringify(json), MINISEARCH_OPTIONS)
    expect(ms.documentCount).toBe(2)
  })

  test("search by cognomen returns matches", () => {
    const json = buildSearchIndex(SUMMARIES)
    const ms = MiniSearch.loadJSON(JSON.stringify(json), MINISEARCH_OPTIONS)
    const results = ms.search("Brutus")
    expect(results.length).toBeGreaterThan(0)
    expect(results[0].id).toBe("IUNI0001")
  })

  test("search by nomen returns matches", () => {
    const json = buildSearchIndex(SUMMARIES)
    const ms = MiniSearch.loadJSON(JSON.stringify(json), MINISEARCH_OPTIONS)
    const results = ms.search("Cornelius")
    expect(results.length).toBeGreaterThan(0)
    expect(results[0].id).toBe("CORN0123")
  })

  test("search by otherNames returns matches", () => {
    const withAltName: PersonSummary[] = [
      {
        ...SUMMARIES[0],
        id: "IUNI2459",
        name: "IUNI2459 M. Iunius (53) M. f. Brutus",
        otherNames: "= Q. Servilius Caepio Brutus",
      },
    ]
    const json = buildSearchIndex(withAltName)
    const ms = MiniSearch.loadJSON(JSON.stringify(json), MINISEARCH_OPTIONS)
    const results = ms.search("Caepio")
    expect(results.length).toBeGreaterThan(0)
    expect(results[0].id).toBe("IUNI2459")
  })
})
