// site/src/lib/sparql.ts
//
// Pure helpers behind the /sparql page: prefix shortening for result
// display, SPARQL Results JSON parsing, and mapping Person IRIs back to
// local person routes. The Oxigraph engine itself lives in
// sparql-worker.ts; nothing here touches WASM so it stays unit-testable.

export const PREFIXES: Record<string, string> = {
  dprr: "http://romanrepublic.ac.uk/rdf/ontology#",
  entity: "http://romanrepublic.ac.uk/rdf/entity/",
  rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  rdfs: "http://www.w3.org/2000/01/rdf-schema#",
  owl: "http://www.w3.org/2002/07/owl#",
  xsd: "http://www.w3.org/2001/XMLSchema#",
  wd: "http://www.wikidata.org/entity/",
}

/** Longest-match prefixed form ("dprr:hasOffice"), or the full IRI. */
export function shortenIri(iri: string): string {
  let best: { prefix: string; ns: string } | null = null
  for (const [prefix, ns] of Object.entries(PREFIXES)) {
    if (iri.startsWith(ns) && (!best || ns.length > best.ns.length)) {
      best = { prefix, ns }
    }
  }
  return best ? `${best.prefix}:${iri.slice(best.ns.length)}` : iri
}

/** SPARQL 1.1 Query Results JSON term. */
export interface ResultTerm {
  type: "uri" | "literal" | "bnode"
  value: string
  datatype?: string
  "xml:lang"?: string
}

export interface SelectResults {
  vars: string[]
  rows: Record<string, ResultTerm | undefined>[]
}

export function parseSelectResults(jsonText: string): SelectResults {
  const parsed = JSON.parse(jsonText) as {
    head: { vars?: string[] }
    results: { bindings: Record<string, ResultTerm>[] }
  }
  return {
    vars: parsed.head.vars ?? [],
    rows: parsed.results.bindings,
  }
}

const PERSON_IRI =
  /^http:\/\/romanrepublic\.ac\.uk\/rdf\/entity\/Person\/(\d+)$/

/**
 * DPRR ids embed a globally unique number (CORN0076 <-> entity/Person/76),
 * so the person-ids list the site already ships maps numbers to routes.
 */
export function buildPersonNumberMap(ids: string[]): Map<number, string> {
  const map = new Map<number, string>()
  for (const id of ids) {
    const digits = /\d+/.exec(id)
    if (digits) map.set(parseInt(digits[0], 10), id)
  }
  return map
}

/** Local route for a Person IRI, or null if it isn't one / isn't known. */
export function personRouteForIri(
  iri: string,
  personIdsByNumber: Map<number, string>
): string | null {
  const match = PERSON_IRI.exec(iri)
  if (!match) return null
  const id = personIdsByNumber.get(parseInt(match[1], 10))
  return id ? `/persons/${id}` : null
}

export const QUERY_PREFIX_BLOCK = `PREFIX dprr: <http://romanrepublic.ac.uk/rdf/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
`

export const EXAMPLE_QUERIES: { label: string; query: string }[] = [
  {
    label: "Consuls, 100–50 BC",
    query: `${QUERY_PREFIX_BLOCK}
# Dates are stored as integers; BC years are negative.
SELECT ?name ?year WHERE {
  ?post a dprr:PostAssertion ;
        dprr:hasOffice ?office ;
        dprr:isAboutPerson ?person ;
        dprr:hasDateStart ?year .
  ?office dprr:hasName "consul" .
  ?person dprr:hasPersonName ?name .
  FILTER(?year >= -100 && ?year <= -50)
}
ORDER BY ?year`,
  },
  {
    label: "Cursus of Scipio Africanus",
    query: `${QUERY_PREFIX_BLOCK}
SELECT ?year ?office WHERE {
  ?post dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/878> ;
        dprr:hasOffice ?o .
  ?o dprr:hasName ?office .
  OPTIONAL { ?post dprr:hasDateStart ?year }
}
ORDER BY ?year`,
  },
  {
    label: "Most-held offices",
    query: `${QUERY_PREFIX_BLOCK}
SELECT ?office (COUNT(DISTINCT ?person) AS ?holders) WHERE {
  ?post dprr:hasOffice ?o ;
        dprr:isAboutPerson ?person .
  ?o dprr:hasName ?office .
}
GROUP BY ?office
ORDER BY DESC(?holders)
LIMIT 20`,
  },
  {
    label: "Any patrician tribunus plebis? (ASK)",
    query: `${QUERY_PREFIX_BLOCK}
ASK {
  ?post dprr:hasOffice ?o ;
        dprr:isAboutPerson ?p .
  ?o dprr:hasName "tribunus plebis" .
  ?p dprr:isPatrician true .
}`,
  },
  {
    label: "Uncertain consulships, with sources",
    query: `${QUERY_PREFIX_BLOCK}
# Post assertions flagged dprr:isUncertain true, with the modern source
# (dprr:hasSecondarySource) each claim rests on.
SELECT ?name ?year ?source ?biblio WHERE {
  ?post dprr:isAboutPerson ?person ;
        dprr:hasOffice ?o ;
        dprr:isUncertain true ;
        dprr:hasSecondarySource ?src .
  ?o dprr:hasName "consul" .
  ?person dprr:hasPersonName ?name .
  ?src dprr:hasAbbreviation ?source ;
       dprr:hasBiblio ?biblio .
  OPTIONAL { ?post dprr:hasDateStart ?year }
}
ORDER BY ?year`,
  },
  {
    label: "Wikidata links (CONSTRUCT)",
    query: `${QUERY_PREFIX_BLOCK}
CONSTRUCT { ?person owl:sameAs ?wd } WHERE {
  ?person owl:sameAs ?wd .
  FILTER(STRSTARTS(STR(?wd), "http://www.wikidata.org/"))
}
LIMIT 10`,
  },
]

export const DEFAULT_QUERY = EXAMPLE_QUERIES[0].query

function csvField(value: string): string {
  return /[",\n\r]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value
}

/** SELECT results as RFC 4180 CSV; unbound cells are empty. */
export function toCsv(results: SelectResults): string {
  const lines = [results.vars.map(csvField).join(",")]
  for (const row of results.rows) {
    lines.push(results.vars.map((v) => csvField(row[v]?.value ?? "")).join(","))
  }
  return lines.join("\r\n")
}
