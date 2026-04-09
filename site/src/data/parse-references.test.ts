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
  dprr:hasAbbreviation "L." .
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

describe("parseReferenceTtl", () => {
  test("parses offices", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
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
    })
    expect(
      refs.praenomina.get(
        "http://romanrepublic.ac.uk/rdf/entity/Praenomen/Lucius"
      )
    ).toBe("Lucius")
  })

  test("parses sexes from misc", async () => {
    const refs = await parseReferenceTtl({
      offices: OFFICE_TTL,
      sources: SOURCE_TTL,
      praenomina: PRAENOMEN_TTL,
      tribes: "",
      relationships: "",
      misc: MISC_TTL,
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
    })
    expect(
      refs.noteTypes.get("http://romanrepublic.ac.uk/rdf/entity/NoteType/1")
    ).toBe("Reference Note")
  })
})
