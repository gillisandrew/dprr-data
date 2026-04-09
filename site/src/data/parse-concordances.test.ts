// site/src/data/parse-concordances.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parseConcordanceTtl } from "./parse-concordances"

const WIKIDATA_TTL = `
@prefix owl: <http://www.w3.org/2002/07/owl#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> owl:sameAs <http://www.wikidata.org/entity/Q223440> .
<http://romanrepublic.ac.uk/rdf/entity/Person/2459> owl:sameAs <http://www.wikidata.org/entity/Q41813> .
`

const VIAF_TTL = `
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> skos:exactMatch <https://viaf.org/viaf/89203858> .
`

describe("parseConcordanceTtl", () => {
  test("parses owl:sameAs links", () => {
    const result = parseConcordanceTtl("wikidata", WIKIDATA_TTL)
    expect(result.get("1")).toEqual([
      {
        system: "wikidata",
        uri: "http://www.wikidata.org/entity/Q223440",
        predicate: "owl:sameAs",
      },
    ])
  })

  test("parses skos:exactMatch links", () => {
    const result = parseConcordanceTtl("viaf", VIAF_TTL)
    expect(result.get("1")).toEqual([
      {
        system: "viaf",
        uri: "https://viaf.org/viaf/89203858",
        predicate: "skos:exactMatch",
      },
    ])
  })

  test("extracts person numeric ID from URI", () => {
    const result = parseConcordanceTtl("wikidata", WIKIDATA_TTL)
    expect(result.has("2459")).toBe(true)
  })
})
