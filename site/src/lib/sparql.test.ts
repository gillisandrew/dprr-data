import { expect, test, describe } from "vite-plus/test"
import {
  shortenIri,
  parseSelectResults,
  buildPersonNumberMap,
  personRouteForIri,
} from "./sparql"

describe("shortenIri", () => {
  test("uses the known prefixes", () => {
    expect(
      shortenIri("http://romanrepublic.ac.uk/rdf/ontology#hasOffice")
    ).toBe("dprr:hasOffice")
    expect(shortenIri("http://www.w3.org/2000/01/rdf-schema#label")).toBe(
      "rdfs:label"
    )
  })

  test("prefers the longest matching namespace", () => {
    expect(shortenIri("http://romanrepublic.ac.uk/rdf/entity/Office/155")).toBe(
      "entity:Office/155"
    )
  })

  test("passes unknown IRIs through unchanged", () => {
    expect(shortenIri("http://example.com/x")).toBe("http://example.com/x")
  })
})

describe("parseSelectResults", () => {
  test("extracts vars and binding rows", () => {
    const json = JSON.stringify({
      head: { vars: ["name", "year"] },
      results: {
        bindings: [
          {
            name: { type: "literal", value: "POMP3573 L. Pomponius" },
            year: {
              type: "literal",
              value: "-118",
              datatype: "http://www.w3.org/2001/XMLSchema#integer",
            },
          },
          { name: { type: "literal", value: "No year row" } },
        ],
      },
    })
    const result = parseSelectResults(json)
    expect(result.vars).toEqual(["name", "year"])
    expect(result.rows).toHaveLength(2)
    expect(result.rows[0].year?.value).toBe("-118")
    expect(result.rows[1].year).toBeUndefined()
  })
})

describe("person IRI mapping", () => {
  const map = buildPersonNumberMap(["CORN0076", "GALL4325", "IUNI0001"])

  test("maps DPRR id digits to numbers", () => {
    expect(map.get(76)).toBe("CORN0076")
    expect(map.get(4325)).toBe("GALL4325")
    expect(map.get(1)).toBe("IUNI0001")
  })

  test("resolves Person IRIs to local routes", () => {
    expect(
      personRouteForIri("http://romanrepublic.ac.uk/rdf/entity/Person/76", map)
    ).toBe("/persons/CORN0076")
  })

  test("returns null for non-person or unknown IRIs", () => {
    expect(
      personRouteForIri("http://romanrepublic.ac.uk/rdf/entity/Office/155", map)
    ).toBeNull()
    expect(
      personRouteForIri(
        "http://romanrepublic.ac.uk/rdf/entity/Person/999999",
        map
      )
    ).toBeNull()
  })
})
