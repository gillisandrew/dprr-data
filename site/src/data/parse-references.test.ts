// site/src/data/parse-references.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parseReferenceTtl } from "./parse-references"

const OFFICE_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/Office/3> rdfs:label "Office: consul" ;
  a dprr:Office ;
  dprr:hasParent <http://romanrepublic.ac.uk/rdf/entity/Office/2> ;
  dprr:hasName "consul" ;
  dprr:hasAbbreviation "cos." .
`

const SOURCE_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> rdfs:label "Secondary Source: Broughton MRR" ;
  a dprr:SecondarySource ;
  dprr:hasName "Broughton MRR" ;
  dprr:hasAbbreviation "MRR" ;
  dprr:hasBiblio "T.R.S. Broughton, Magistrates of the Roman Republic" .
`

const PRAENOMEN_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/Praenomen/Lucius> rdfs:label "Praenomen: Lucius" ;
  a dprr:Praenomen ;
  dprr:hasName "Lucius" ;
  dprr:Abbreviation "L." .
`

// The relationships file is the only reference file using owl:inverseOf.
const RELATIONSHIP_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/Relationship/6> rdfs:label "Relationship: brother of" ;
  owl:inverseOf <http://romanrepublic.ac.uk/rdf/entity/Relationship/9> ;
  a dprr:Relationship ;
  dprr:hasOrderNumber 31 ;
  dprr:hasName "brother of" .
<http://romanrepublic.ac.uk/rdf/entity/Relationship/9> rdfs:label "Relationship: sister of" ;
  a dprr:Relationship ;
  dprr:hasOrderNumber 32 ;
  dprr:hasName "sister of" .
`

const MISC_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/Sex/Male> rdfs:label "Sex: Male" ;
  a dprr:Sex ;
  dprr:hasName "Male" .
<http://romanrepublic.ac.uk/rdf/entity/NoteType/1> rdfs:label "Note Type: Reference Note" ;
  a dprr:NoteType ;
  dprr:hasName "Reference Note" .
<http://romanrepublic.ac.uk/rdf/entity/DateType/1> rdfs:label "Date Type: birth" ;
  a dprr:DateType ;
  dprr:hasName "birth" .
`

const PROVINCE_TTL = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .

<http://romanrepublic.ac.uk/rdf/entity/Province/9> rdfs:label "Province: Hispania Citerior" ;
  a dprr:Province ;
  dprr:hasParent <http://romanrepublic.ac.uk/rdf/entity/Province/18> ;
  dprr:hasName "Hispania Citerior" .
<http://romanrepublic.ac.uk/rdf/entity/Province/99> rdfs:label "Province: Mediterranean" ;
  a dprr:Province ;
  dprr:hasName "Mediterranean" .
<http://romanrepublic.ac.uk/rdf/entity/Province/92> rdfs:label "Province: " ;
  a dprr:Province .
`

const emptyInputs = {
  offices: OFFICE_TTL,
  sources: SOURCE_TTL,
  praenomina: PRAENOMEN_TTL,
  tribes: "",
  relationships: "",
  misc: MISC_TTL,
  provinces: "",
}

describe("parseReferenceTtl", () => {
  test("parses offices", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
      provinces: "",
    })
    const office = refs.offices.get(
      "http://romanrepublic.ac.uk/rdf/entity/Office/3"
    )
    expect(office).toEqual({
      name: "consul",
      abbreviation: "cos.",
      parent: "http://romanrepublic.ac.uk/rdf/entity/Office/2",
    })
  })

  test("parses sources", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
      provinces: "",
    })
    const source = refs.sources.get(
      "http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1"
    )
    expect(source).toEqual({
      name: "Broughton MRR",
      abbreviation: "MRR",
      biblio: "T.R.S. Broughton, Magistrates of the Roman Republic",
    })
  })

  test("parses praenomina", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
      provinces: "",
    })
    // The praenomina file spells this predicate `dprr:Abbreviation`, not
    // `dprr:hasAbbreviation` like every other reference file.
    expect(
      refs.praenomina.get(
        "http://romanrepublic.ac.uk/rdf/entity/Praenomen/Lucius"
      )
    ).toEqual({ name: "Lucius", abbreviation: "L." })
  })

  test("parses relationship inverses", async () => {
    const refs = await parseReferenceTtl({
      ...emptyInputs,
      relationships: RELATIONSHIP_TTL,
    })
    const brother = refs.relationships.get(
      "http://romanrepublic.ac.uk/rdf/entity/Relationship/6"
    )
    expect(brother?.inverseName).toBe("sister of")
    const sister = refs.relationships.get(
      "http://romanrepublic.ac.uk/rdf/entity/Relationship/9"
    )
    expect(sister?.inverseName).toBeNull()
  })

  test("parses sexes from misc", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
      provinces: "",
    })
    expect(
      refs.sexes.get("http://romanrepublic.ac.uk/rdf/entity/Sex/Male")
    ).toBe("Male")
  })

  test("parses note types from misc", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
      provinces: "",
    })
    expect(
      refs.noteTypes.get("http://romanrepublic.ac.uk/rdf/entity/NoteType/1")
    ).toBe("Reference Note")
  })
})

const REL_TTL = `@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Relationship/12> a dprr:Relationship ;
  dprr:hasName "son of" ;
  dprr:hasOrderNumber 3 .
<http://romanrepublic.ac.uk/rdf/entity/Relationship/13> a dprr:Relationship ;
  dprr:hasName "brother of" .
`

describe("relationship reference map", () => {
  test("carries name and orderNumber (null when absent)", async () => {
    const refs = await parseReferenceTtl({
      offices: "",
      sources: "",
      praenomina: "",
      tribes: "",
      relationships: REL_TTL,
      misc: "",
      provinces: "",
    })
    expect(
      refs.relationships.get(
        "http://romanrepublic.ac.uk/rdf/entity/Relationship/12"
      )
    ).toEqual({ name: "son of", orderNumber: 3, inverseName: null })
    expect(
      refs.relationships.get(
        "http://romanrepublic.ac.uk/rdf/entity/Relationship/13"
      )
    ).toEqual({ name: "brother of", orderNumber: null, inverseName: null })
  })
})

describe("provinces", () => {
  test("parses province names and parents, skipping nameless entries", async () => {
    const refs = await parseReferenceTtl({
      ...emptyInputs,
      provinces: PROVINCE_TTL,
    })
    expect(refs.provinces.size).toBe(2)
    expect(
      refs.provinces.get("http://romanrepublic.ac.uk/rdf/entity/Province/9")
    ).toEqual({
      name: "Hispania Citerior",
      parent: "http://romanrepublic.ac.uk/rdf/entity/Province/18",
    })
    expect(
      refs.provinces.get("http://romanrepublic.ac.uk/rdf/entity/Province/99")
    ).toEqual({ name: "Mediterranean", parent: null })
  })
})
