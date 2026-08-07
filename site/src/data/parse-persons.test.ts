// site/src/data/parse-persons.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parsePersonTtl } from "./parse-persons"
import type { ReferenceMaps } from "./types"

const SIMPLE_PERSON_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1459> rdfs:label "Post Assertion: #1459" ;
  a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1375> ;
  dprr:hasSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> ;
  dprr:hasOriginalText "C. Aburius (1)" ;
  dprr:hasOffice <http://romanrepublic.ac.uk/rdf/entity/Office/17> ;
  dprr:hasDateStart -171 ;
  dprr:hasDateEnd -171 ;
  dprr:hasDateSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1375> a dprr:Person ;
  dprr:isSex <http://romanrepublic.ac.uk/rdf/entity/Sex/Male> ;
  dprr:hasPraenomen <http://romanrepublic.ac.uk/rdf/entity/Praenomen/Gaius> ;
  dprr:hasPersonName "ABUR1375 C. Aburius (1)" ;
  dprr:hasNomen "Aburius" ;
  dprr:hasHighestOffice "leg. 171" ;
  dprr:hasFiliation "" ;
  dprr:hasEraTo -171 ;
  dprr:hasEraFrom -171 ;
  dprr:hasDprrID "ABUR1375" .
`

function makeRefs(): ReferenceMaps {
  return {
    offices: new Map([
      [
        "http://romanrepublic.ac.uk/rdf/entity/Office/17",
        { name: "legatus", abbreviation: "leg.", parent: null },
      ],
    ]),
    sources: new Map([
      [
        "http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1",
        {
          name: "Broughton MRR",
          abbreviation: "MRR",
          biblio: null,
        },
      ],
    ]),
    praenomina: new Map([
      ["http://romanrepublic.ac.uk/rdf/entity/Praenomen/Gaius", "Gaius"],
    ]),
    tribes: new Map(),
    relationships: new Map(),
    noteTypes: new Map(),
    dateTypes: new Map(),
    sexes: new Map([
      ["http://romanrepublic.ac.uk/rdf/entity/Sex/Male", "Male"],
    ]),
    statuses: new Map(),
    provinces: new Map(),
  }
}

describe("parsePersonTtl", () => {
  test("extracts basic person fields", () => {
    const persons = parsePersonTtl(SIMPLE_PERSON_TTL, makeRefs(), new Map())
    expect(persons).toHaveLength(1)
    const p = persons[0]
    expect(p.id).toBe("ABUR1375")
    expect(p.nomen).toBe("Aburius")
    expect(p.praenomen).toBe("Gaius")
    expect(p.sex).toBe("Male")
    expect(p.eraFrom).toBe(-171)
    expect(p.eraTo).toBe(-171)
  })

  test("resolves post assertions", () => {
    const persons = parsePersonTtl(SIMPLE_PERSON_TTL, makeRefs(), new Map())
    const p = persons[0]
    expect(p.postAssertions).toHaveLength(1)
    expect(p.postAssertions[0].officeName).toBe("legatus")
    expect(p.postAssertions[0].dateStart).toBe(-171)
    expect(p.postAssertions[0].secondarySource).toBe("Broughton MRR")
  })

  test("builds offices facet array", () => {
    const persons = parsePersonTtl(SIMPLE_PERSON_TTL, makeRefs(), new Map())
    expect(persons[0].offices).toEqual(["legatus"])
  })
})

describe("province extraction", () => {
  test("resolves provinceOriginal through the curated mapping", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasProvinceOriginal "Sicily" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasProvinceOriginal "not-a-real-province" .
`
    const persons = parsePersonTtl(ttl, makeRefs(), new Map())
    expect(persons).toHaveLength(1)
    const byOriginal = Object.fromEntries(
      persons[0].postAssertions.map((pa) => [pa.provinceOriginal, pa])
    )
    expect(byOriginal["Sicily"].provinces).toEqual(["Sicilia"])
    expect(byOriginal["not-a-real-province"].provinces).toEqual([])
    expect(persons[0].provinces).toEqual(["Sicilia"])
  })
})
