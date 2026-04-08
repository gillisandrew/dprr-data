# DPRR Static Site Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the DPRR prosopographical dataset as a statically rendered site with faceted search, hostable on GitHub Pages.

**Architecture:** A build-time data loader parses TTL files with N3.js into typed JSON. TanStack Start server functions serve this data at prerender time. Nitro's static preset generates ~4,900 person HTML pages plus a search page with MiniSearch-powered faceted filtering. All search/facet state lives in URL query params.

**Tech Stack:** TanStack Start, TanStack Router, N3.js, MiniSearch, Tailwind CSS 4.2, shadcn/ui, Nitro (static preset), Vite+

**Spec:** `docs/superpowers/specs/2026-04-08-static-site-design.md`

---

## File Structure

```
site/src/
├── data/
│   ├── types.ts               # All data interfaces (Person, PostAssertion, etc.)
│   ├── loader.ts              # Orchestrator: loads all TTL, builds lookup maps, resolves persons
│   ├── parse-references.ts    # Parse reference/*.ttl → lookup maps
│   ├── parse-persons.ts       # Parse persons/**/*.ttl → Person records
│   ├── parse-concordances.ts  # Parse concordances/*.ttl → person ID → links map
│   └── search-index.ts        # Build MiniSearch index + persons-summary from loaded data
├── server/
│   └── data.ts                # createServerFn wrappers for data access
├── lib/
│   ├── utils.ts               # existing cn() utility
│   ├── dates.ts               # formatYear(-509) → "509 BC", formatEra(), etc.
│   └── search.ts              # useSearchState hook: query params ↔ MiniSearch ↔ facets
├── components/
│   ├── ui/                    # shadcn primitives (add: badge, checkbox, input, collapsible)
│   ├── date-display.tsx       # <DateDisplay year={-509} uncertain={false} />
│   ├── source-citation.tsx    # <SourceCitation name="Broughton MRR" abbrev="MRR" />
│   ├── section.tsx            # <Section title="Offices" defaultOpen={true}>{children}</Section>
│   ├── person-card.tsx        # <PersonCard person={summary} /> — used in results + relationships
│   ├── active-filter-chips.tsx # <ActiveFilterChips filters={...} onRemove={...} />
│   ├── search-input.tsx       # <SearchInput value={q} onChange={...} />
│   ├── facet-group.tsx        # <FacetGroup title="Office" items={...} selected={...} onChange={...} />
│   ├── facet-range-group.tsx  # <FacetRangeGroup title="Era" min={-509} max={-31} ... />
│   ├── facet-sidebar.tsx      # Composes FacetGroup + FacetRangeGroup for all dimensions
│   └── results-list.tsx       # Renders PersonCard[] with result count
├── routes/
│   ├── __root.tsx             # Update with site-level meta tags
│   ├── index.tsx              # Search page: composes all search components
│   └── persons.$id.tsx        # Person detail page
└── styles.css                 # existing (may need minor additions)
```

**Test files** live alongside source at `site/src/**/*.test.ts` (Vitest convention via `vp test`).

---

### Task 1: Project Setup & Dependencies

**Files:**
- Modify: `site/package.json`
- Modify: `site/vite.config.ts`

- [ ] **Step 1: Install runtime dependencies**

```bash
cd site && vp add n3 minisearch
```

- [ ] **Step 2: Install dev type definitions**

```bash
cd site && vp add -D @types/n3
```

- [ ] **Step 3: Configure Nitro static preset in vite.config.ts**

In `site/vite.config.ts`, update the `nitro()` plugin call to configure the static preset and prerender options:

```typescript
import { defineConfig } from "vite-plus"
import { devtools } from "@tanstack/devtools-vite"
import { tanstackStart } from "@tanstack/react-start/plugin/vite"
import viteReact from "@vitejs/plugin-react"
import viteTsConfigPaths from "vite-tsconfig-paths"
import tailwindcss from "@tailwindcss/vite"
import { nitro } from "nitro/vite"

const config = defineConfig({
  lint: { options: { typeAware: true, typeCheck: true } },
  fmt: {
    endOfLine: "lf",
    semi: false,
    singleQuote: false,
    tabWidth: 2,
    trailingComma: "es5",
    printWidth: 80,
    sortTailwindcss: {
      stylesheet: "src/styles.css",
      functions: ["cn", "cva"],
    },
    sortPackageJson: false,
    ignorePatterns: ["package-lock.json", "pnpm-lock.yaml", "yarn.lock"],
  },
  plugins: [
    devtools(),
    nitro({
      preset: "static",
    }),
    viteTsConfigPaths({
      projects: ["./tsconfig.json"],
    }),
    tailwindcss(),
    tanstackStart({
      prerender: {
        enabled: true,
        crawlLinks: true,
      },
    }),
    viteReact(),
  ],
})

export default config
```

- [ ] **Step 4: Verify dev server still starts**

```bash
cd site && vp dev --port 3000
```

Expected: Dev server starts without errors. Kill it after confirming.

- [ ] **Step 5: Commit**

```bash
cd site && git add package.json pnpm-lock.yaml vite.config.ts
git commit -m "chore: add n3, minisearch deps and configure static preset"
```

---

### Task 2: Data Types

**Files:**
- Create: `site/src/data/types.ts`

- [ ] **Step 1: Create type definitions**

```typescript
// site/src/data/types.ts

/** Compact person record for search results and faceting. */
export interface PersonSummary {
  id: string
  name: string
  praenomen: string
  nomen: string
  cognomen: string | null
  sex: "Male" | "Female"
  isPatrician: boolean
  isNobilis: boolean
  highestOffice: string | null
  eraFrom: number | null
  eraTo: number | null
  tribe: string | null
  /** Flattened list of office names held (for faceting). */
  offices: string[]
}

/** Full person record with all scholarly data. */
export interface Person extends PersonSummary {
  uri: string
  otherNames: string | null
  filiation: string | null
  reNumber: string | null
  nobilisNotes: string | null
  postAssertions: PostAssertion[]
  relationships: Relationship[]
  dateInformation: DateInfo[]
  personNotes: Note[]
  concordances: Concordance[]
}

export interface PostAssertion {
  id: string
  officeName: string
  officeAbbreviation: string | null
  dateStart: number | null
  dateEnd: number | null
  dateSecondarySource: string | null
  originalText: string | null
  secondarySource: string
  notes: PostAssertionNote[]
  primarySourceRefs: string[]
}

export interface PostAssertionNote {
  type: string
  text: string
  secondarySource: string
  extraInfo: string | null
}

export interface Relationship {
  id: string
  relationshipType: string
  relatedPersonId: string
  relatedPersonName: string
  secondarySource: string
  references: RelationshipReference[]
}

export interface RelationshipReference {
  type: string
  extraInfo: string | null
  secondarySource: string
}

export interface DateInfo {
  type: string
  value: number
  interval: string
  isUncertain: boolean
  notes: string | null
  secondarySource: string
}

export interface Note {
  type: string
  text: string
  secondarySource: string
}

export interface Concordance {
  system: string
  uri: string
  predicate: "owl:sameAs" | "skos:exactMatch"
}

/** Lookup maps built from reference/*.ttl files. */
export interface ReferenceMaps {
  offices: Map<string, { name: string; abbreviation: string | null; parent: string | null }>
  sources: Map<string, { name: string; abbreviation: string | null; biblio: string | null }>
  praenomina: Map<string, string>
  tribes: Map<string, { name: string; abbreviation: string | null }>
  relationships: Map<string, string>
  noteTypes: Map<string, string>
  dateTypes: Map<string, string>
  sexes: Map<string, string>
  statuses: Map<string, { name: string; abbreviation: string | null }>
}

/** A single facet value with its count. */
export interface FacetValue {
  value: string
  count: number
}

/** Active search/filter state derived from URL query params. */
export interface SearchState {
  q: string
  office: string[]
  nomen: string[]
  sex: string[]
  patrician: boolean | null
  nobilis: boolean | null
  tribe: string[]
  eraFrom: number | null
  eraTo: number | null
}
```

- [ ] **Step 2: Commit**

```bash
cd site && git add src/data/types.ts
git commit -m "feat: add data type definitions for persons, references, and search"
```

---

### Task 3: Date Formatting Utilities

**Files:**
- Create: `site/src/lib/dates.ts`
- Create: `site/src/lib/dates.test.ts`

- [ ] **Step 1: Write tests**

```typescript
// site/src/lib/dates.test.ts
import { expect, test, describe } from "vite-plus/test"
import { formatYear, formatEraRange } from "./dates"

describe("formatYear", () => {
  test("negative year displays as BC", () => {
    expect(formatYear(-509)).toBe("509 BC")
  })

  test("positive year displays as AD", () => {
    expect(formatYear(14)).toBe("AD 14")
  })

  test("zero displays as 1 BC", () => {
    expect(formatYear(0)).toBe("1 BC")
  })

  test("with uncertainty marker", () => {
    expect(formatYear(-540, true)).toBe("c. 540 BC")
  })
})

describe("formatEraRange", () => {
  test("both BC dates", () => {
    expect(formatEraRange(-540, -509)).toBe("540\u2013509 BC")
  })

  test("null from", () => {
    expect(formatEraRange(null, -509)).toBe("?\u2013509 BC")
  })

  test("null to", () => {
    expect(formatEraRange(-540, null)).toBe("540 BC\u2013?")
  })

  test("both null", () => {
    expect(formatEraRange(null, null)).toBe(null)
  })

  test("cross BC/AD boundary", () => {
    expect(formatEraRange(-63, 14)).toBe("63 BC\u2013AD 14")
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd site && vp test src/lib/dates.test.ts
```

Expected: FAIL — module `./dates` not found.

- [ ] **Step 3: Implement**

```typescript
// site/src/lib/dates.ts

/**
 * Format a year integer for display.
 * Negative values are BC, positive are AD. Zero = 1 BC (no year zero).
 */
export function formatYear(
  year: number,
  uncertain: boolean = false
): string {
  const prefix = uncertain ? "c. " : ""
  if (year <= 0) {
    return `${prefix}${Math.abs(year) || 1} BC`
  }
  return `${prefix}AD ${year}`
}

/**
 * Format an era range like "540–509 BC" or "63 BC–AD 14".
 * Returns null if both values are null.
 */
export function formatEraRange(
  from: number | null,
  to: number | null
): string | null {
  if (from === null && to === null) return null

  const fromStr = from !== null ? formatYear(from) : "?"
  const toStr = to !== null ? formatYear(to) : "?"

  // Optimize: if both are BC, omit "BC" from the first value
  if (from !== null && from <= 0 && to !== null && to <= 0) {
    const fromNum = Math.abs(from) || 1
    const toNum = Math.abs(to) || 1
    return `${fromNum}\u2013${toNum} BC`
  }

  return `${fromStr}\u2013${toStr}`
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd site && vp test src/lib/dates.test.ts
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
cd site && git add src/lib/dates.ts src/lib/dates.test.ts
git commit -m "feat: add date formatting utilities with tests"
```

---

### Task 4: Reference Data Parsing

**Files:**
- Create: `site/src/data/parse-references.ts`
- Create: `site/src/data/parse-references.test.ts`

- [ ] **Step 1: Write tests**

The test parses a small inline TTL snippet and verifies the lookup map output.

```typescript
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
      refs.noteTypes.get(
        "http://romanrepublic.ac.uk/rdf/entity/NoteType/1"
      )
    ).toBe("Reference Note")
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd site && vp test src/data/parse-references.test.ts
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

```typescript
// site/src/data/parse-references.ts
import { Parser } from "n3"
import type { ReferenceMaps } from "./types"

const DPRR = "http://romanrepublic.ac.uk/rdf/ontology#"

interface RawTtlInputs {
  offices: string
  sources: string
  praenomina: string
  tribes: string
  relationships: string
  misc: string
}

function parseTtl(ttl: string) {
  const parser = new Parser()
  return parser.parse(ttl)
}

function val(term: { value: string } | undefined): string | null {
  return term?.value ?? null
}

export async function parseReferenceTtl(
  inputs: RawTtlInputs
): Promise<ReferenceMaps> {
  const offices = new Map<
    string,
    { name: string; abbreviation: string | null; parent: string | null }
  >()
  const sources = new Map<
    string,
    {
      name: string
      abbreviation: string | null
      biblio: string | null
    }
  >()
  const praenomina = new Map<string, string>()
  const tribes = new Map<
    string,
    { name: string; abbreviation: string | null }
  >()
  const relationships = new Map<string, string>()
  const noteTypes = new Map<string, string>()
  const dateTypes = new Map<string, string>()
  const sexes = new Map<string, string>()
  const statuses = new Map<
    string,
    { name: string; abbreviation: string | null }
  >()

  // Group quads by subject for each file
  function groupBySubject(quads: ReturnType<typeof parseTtl>) {
    const map = new Map<string, Map<string, string>>()
    for (const q of quads) {
      const subj = q.subject.value
      if (!map.has(subj)) map.set(subj, new Map())
      map.get(subj)!.set(q.predicate.value, q.object.value)
    }
    return map
  }

  // Offices
  if (inputs.offices) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.offices))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        offices.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
          parent: props.get(`${DPRR}hasParent`) ?? null,
        })
      }
    }
  }

  // Sources
  if (inputs.sources) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.sources))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        sources.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
          biblio: props.get(`${DPRR}hasBiblio`) ?? null,
        })
      }
    }
  }

  // Praenomina
  if (inputs.praenomina) {
    for (const [uri, props] of groupBySubject(
      parseTtl(inputs.praenomina)
    )) {
      const name = props.get(`${DPRR}hasName`)
      if (name) praenomina.set(uri, name)
    }
  }

  // Tribes
  if (inputs.tribes) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.tribes))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        tribes.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
        })
      }
    }
  }

  // Relationships
  if (inputs.relationships) {
    for (const [uri, props] of groupBySubject(
      parseTtl(inputs.relationships)
    )) {
      const name = props.get(`${DPRR}hasName`)
      if (name) relationships.set(uri, name)
    }
  }

  // Misc: Sex, NoteType, DateType, Status
  if (inputs.misc) {
    const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    const quads = parseTtl(inputs.misc)
    const grouped = new Map<
      string,
      { type: string | null; props: Map<string, string> }
    >()
    for (const q of quads) {
      const subj = q.subject.value
      if (!grouped.has(subj))
        grouped.set(subj, { type: null, props: new Map() })
      const entry = grouped.get(subj)!
      if (q.predicate.value === RDF_TYPE) {
        entry.type = q.object.value
      } else {
        entry.props.set(q.predicate.value, q.object.value)
      }
    }
    for (const [uri, { type, props }] of grouped) {
      const name = props.get(`${DPRR}hasName`)
      if (!name) continue
      if (type === `${DPRR}Sex`) {
        sexes.set(uri, name)
      } else if (type === `${DPRR}NoteType`) {
        noteTypes.set(uri, name)
      } else if (type === `${DPRR}DateType`) {
        dateTypes.set(uri, name)
      } else if (type === `${DPRR}Status`) {
        statuses.set(uri, {
          name,
          abbreviation: props.get(`${DPRR}hasAbbreviation`) ?? null,
        })
      }
    }
  }

  return {
    offices,
    sources,
    praenomina,
    tribes,
    relationships,
    noteTypes,
    dateTypes,
    sexes,
    statuses,
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd site && vp test src/data/parse-references.test.ts
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
cd site && git add src/data/parse-references.ts src/data/parse-references.test.ts
git commit -m "feat: add reference data TTL parser with tests"
```

---

### Task 5: Concordance Parsing

**Files:**
- Create: `site/src/data/parse-concordances.ts`
- Create: `site/src/data/parse-concordances.test.ts`

- [ ] **Step 1: Write tests**

```typescript
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd site && vp test src/data/parse-concordances.test.ts
```

- [ ] **Step 3: Implement**

```typescript
// site/src/data/parse-concordances.ts
import { Parser } from "n3"
import type { Concordance } from "./types"

const OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
const SKOS_EXACT_MATCH = "http://www.w3.org/2004/02/skos/core#exactMatch"
const PERSON_PREFIX = "http://romanrepublic.ac.uk/rdf/entity/Person/"

/**
 * Parse a concordance TTL file and return a map from person numeric ID
 * to concordance entries.
 */
export function parseConcordanceTtl(
  system: string,
  ttl: string
): Map<string, Concordance[]> {
  const parser = new Parser()
  const quads = parser.parse(ttl)
  const result = new Map<string, Concordance[]>()

  for (const q of quads) {
    const subjectUri = q.subject.value
    if (!subjectUri.startsWith(PERSON_PREFIX)) continue

    const personNumericId = subjectUri.slice(PERSON_PREFIX.length)
    let predicate: Concordance["predicate"]

    if (q.predicate.value === OWL_SAME_AS) {
      predicate = "owl:sameAs"
    } else if (q.predicate.value === SKOS_EXACT_MATCH) {
      predicate = "skos:exactMatch"
    } else {
      continue
    }

    const entry: Concordance = {
      system,
      uri: q.object.value,
      predicate,
    }

    const existing = result.get(personNumericId) ?? []
    existing.push(entry)
    result.set(personNumericId, existing)
  }

  return result
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd site && vp test src/data/parse-concordances.test.ts
```

- [ ] **Step 5: Commit**

```bash
cd site && git add src/data/parse-concordances.ts src/data/parse-concordances.test.ts
git commit -m "feat: add concordance TTL parser with tests"
```

---

### Task 6: Person Data Parsing

**Files:**
- Create: `site/src/data/parse-persons.ts`
- Create: `site/src/data/parse-persons.test.ts`

- [ ] **Step 1: Write tests**

Use a stripped-down version of a real person TTL file (ABUR1375 — simple case with one office).

```typescript
// site/src/data/parse-persons.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parsePersonTtl } from "./parse-persons"
import type { ReferenceMaps, Concordance } from "./types"

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
      [
        "http://romanrepublic.ac.uk/rdf/entity/Praenomen/Gaius",
        "Gaius",
      ],
    ]),
    tribes: new Map(),
    relationships: new Map(),
    noteTypes: new Map(),
    dateTypes: new Map(),
    sexes: new Map([
      ["http://romanrepublic.ac.uk/rdf/entity/Sex/Male", "Male"],
    ]),
    statuses: new Map(),
  }
}

describe("parsePersonTtl", () => {
  test("extracts basic person fields", () => {
    const persons = parsePersonTtl(
      SIMPLE_PERSON_TTL,
      makeRefs(),
      new Map()
    )
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
    const persons = parsePersonTtl(
      SIMPLE_PERSON_TTL,
      makeRefs(),
      new Map()
    )
    const p = persons[0]
    expect(p.postAssertions).toHaveLength(1)
    expect(p.postAssertions[0].officeName).toBe("legatus")
    expect(p.postAssertions[0].dateStart).toBe(-171)
    expect(p.postAssertions[0].secondarySource).toBe("Broughton MRR")
  })

  test("builds offices facet array", () => {
    const persons = parsePersonTtl(
      SIMPLE_PERSON_TTL,
      makeRefs(),
      new Map()
    )
    expect(persons[0].offices).toEqual(["legatus"])
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd site && vp test src/data/parse-persons.test.ts
```

- [ ] **Step 3: Implement**

```typescript
// site/src/data/parse-persons.ts
import { Parser } from "n3"
import type {
  Person,
  PostAssertion,
  PostAssertionNote,
  Relationship,
  RelationshipReference,
  DateInfo,
  Note,
  Concordance,
  ReferenceMaps,
} from "./types"

const DPRR = "http://romanrepublic.ac.uk/rdf/ontology#"
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
const PERSON_TYPE = `${DPRR}Person`
const POST_ASSERTION_TYPE = `${DPRR}PostAssertion`
const POST_ASSERTION_NOTE_TYPE = `${DPRR}PostAssertionNote`
const RELATIONSHIP_ASSERTION_TYPE = `${DPRR}RelationshipAssertion`
const RELATIONSHIP_REF_TYPE = `${DPRR}RelationshipAssertionReference`
const DATE_INFO_TYPE = `${DPRR}DateInformation`
const PERSON_NOTE_TYPE = `${DPRR}PersonNote`
const PRIMARY_SOURCE_REF_TYPE = `${DPRR}PrimarySourceReference`
const PERSON_PREFIX = "http://romanrepublic.ac.uk/rdf/entity/Person/"

interface QuadGroup {
  type: string | null
  props: Map<string, string[]>
}

function groupBySubject(
  quads: { subject: { value: string }; predicate: { value: string }; object: { value: string } }[]
): Map<string, QuadGroup> {
  const map = new Map<string, QuadGroup>()
  for (const q of quads) {
    const subj = q.subject.value
    if (!map.has(subj)) map.set(subj, { type: null, props: new Map() })
    const entry = map.get(subj)!
    if (q.predicate.value === RDF_TYPE) {
      entry.type = q.object.value
    } else {
      const pred = q.predicate.value
      if (!entry.props.has(pred)) entry.props.set(pred, [])
      entry.props.get(pred)!.push(q.object.value)
    }
  }
  return map
}

function first(group: QuadGroup, pred: string): string | null {
  return group.props.get(`${DPRR}${pred}`)?.[0] ?? null
}

function firstNum(group: QuadGroup, pred: string): number | null {
  const v = first(group, pred)
  if (v === null) return null
  const n = Number(v)
  return Number.isNaN(n) ? null : n
}

function all(group: QuadGroup, pred: string): string[] {
  return group.props.get(`${DPRR}${pred}`) ?? []
}

/**
 * Parse a person TTL file and return fully resolved Person records.
 * A single TTL file may contain multiple Person entities (e.g., a related
 * person whose data is co-located).
 */
export function parsePersonTtl(
  ttl: string,
  refs: ReferenceMaps,
  concordanceMap: Map<string, Concordance[]>
): Person[] {
  const parser = new Parser()
  const quads = parser.parse(ttl)
  const grouped = groupBySubject(quads)

  // Collect auxiliary entities by type
  const postAssertionGroups = new Map<string, QuadGroup>()
  const postAssertionNoteGroups = new Map<string, QuadGroup>()
  const relationshipGroups = new Map<string, QuadGroup>()
  const relationshipRefGroups = new Map<string, QuadGroup>()
  const dateInfoGroups = new Map<string, QuadGroup>()
  const personNoteGroups = new Map<string, QuadGroup>()
  const primarySourceRefGroups = new Map<string, QuadGroup>()
  const personGroups = new Map<string, QuadGroup>()

  for (const [uri, group] of grouped) {
    switch (group.type) {
      case PERSON_TYPE:
        personGroups.set(uri, group)
        break
      case POST_ASSERTION_TYPE:
        postAssertionGroups.set(uri, group)
        break
      case POST_ASSERTION_NOTE_TYPE:
        postAssertionNoteGroups.set(uri, group)
        break
      case RELATIONSHIP_ASSERTION_TYPE:
        relationshipGroups.set(uri, group)
        break
      case RELATIONSHIP_REF_TYPE:
        relationshipRefGroups.set(uri, group)
        break
      case DATE_INFO_TYPE:
        dateInfoGroups.set(uri, group)
        break
      case PERSON_NOTE_TYPE:
        personNoteGroups.set(uri, group)
        break
      case PRIMARY_SOURCE_REF_TYPE:
        primarySourceRefGroups.set(uri, group)
        break
    }
  }

  // Resolve a source URI to a display name
  function resolveSource(uri: string | null): string {
    if (!uri) return ""
    return refs.sources.get(uri)?.name ?? uri
  }

  // Build PostAssertionNote from a note URI
  function buildPANote(noteUri: string): PostAssertionNote | null {
    const g = postAssertionNoteGroups.get(noteUri)
    if (!g) return null
    const noteTypeUri = first(g, "hasNoteType")
    return {
      type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
      text: first(g, "hasNoteText") ?? "",
      secondarySource: resolveSource(
        first(g, "hasSecondarySourceForNote")
      ),
      extraInfo: first(g, "hasExtraInfo"),
    }
  }

  // Build PostAssertions for a person URI
  function buildPostAssertions(personUri: string): PostAssertion[] {
    const results: PostAssertion[] = []
    for (const [paUri, g] of postAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const officeUri = first(g, "hasOffice")
      const office = officeUri ? refs.offices.get(officeUri) : null

      // Collect notes
      const noteUris = all(g, "hasPostAssertionNote")
      const notes = noteUris
        .map(buildPANote)
        .filter((n): n is PostAssertionNote => n !== null)

      // Collect primary source refs
      const primaryRefs: string[] = []
      for (const [psrUri, psrGroup] of primarySourceRefGroups) {
        if (first(psrGroup, "forAssertion") === paUri) {
          const text = first(psrGroup, "hasNoteText")
          if (text) primaryRefs.push(text)
        }
      }

      results.push({
        id: paUri,
        officeName: office?.name ?? "",
        officeAbbreviation: office?.abbreviation ?? null,
        dateStart: firstNum(g, "hasDateStart"),
        dateEnd: firstNum(g, "hasDateEnd"),
        dateSecondarySource: resolveSource(
          first(g, "hasDateSecondarySource")
        ),
        originalText: first(g, "hasOriginalText"),
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
        notes,
        primarySourceRefs: primaryRefs,
      })
    }
    return results
  }

  // Build RelationshipReference from a ref URI
  function buildRelRef(
    refUri: string
  ): RelationshipReference | null {
    const g = relationshipRefGroups.get(refUri)
    if (!g) return null
    const noteTypeUri = first(g, "hasNoteType")
    return {
      type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
      extraInfo: first(g, "hasExtraInfo"),
      secondarySource: resolveSource(
        first(g, "hasSecondarySourceForNote")
      ),
    }
  }

  // Build Relationships for a person URI
  function buildRelationships(personUri: string): Relationship[] {
    const results: Relationship[] = []
    for (const [raUri, g] of relationshipGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const relTypeUri = first(g, "hasRelationship")
      const relatedUri = first(g, "hasRelatedPerson")

      // Resolve related person name
      let relatedPersonId = ""
      let relatedPersonName = ""
      if (relatedUri) {
        const relatedGroup = personGroups.get(relatedUri)
        relatedPersonName =
          (relatedGroup && first(relatedGroup, "hasPersonName")) ?? ""
        // Extract DPRR ID
        relatedPersonId =
          (relatedGroup && first(relatedGroup, "hasDprrID")) ?? ""
      }

      const refUris = all(g, "hasRelationshipReference")
      const references = refUris
        .map(buildRelRef)
        .filter((r): r is RelationshipReference => r !== null)

      results.push({
        id: raUri,
        relationshipType:
          (relTypeUri && refs.relationships.get(relTypeUri)) ?? "",
        relatedPersonId,
        relatedPersonName,
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
        references,
      })
    }
    return results
  }

  // Build DateInformation for a person URI
  function buildDateInfo(personUri: string): DateInfo[] {
    const results: DateInfo[] = []
    for (const [, g] of dateInfoGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const dateTypeUri = first(g, "hasDateType")
      results.push({
        type: (dateTypeUri && refs.dateTypes.get(dateTypeUri)) ?? "",
        value: firstNum(g, "hasValue") ?? 0,
        interval: first(g, "hasDateInterval") ?? "",
        isUncertain: first(g, "isUncertain") === "true",
        notes: first(g, "hasNotes"),
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
      })
    }
    return results
  }

  // Build PersonNotes for a person URI
  function buildPersonNotes(personUri: string): Note[] {
    const noteUris = personGroups.get(personUri)
      ? all(personGroups.get(personUri)!, "hasPersonNote")
      : []
    return noteUris
      .map((uri) => {
        const g = personNoteGroups.get(uri)
        if (!g) return null
        const noteTypeUri = first(g, "hasNoteType")
        return {
          type: (noteTypeUri && refs.noteTypes.get(noteTypeUri)) ?? "",
          text: first(g, "hasNoteText") ?? "",
          secondarySource: resolveSource(
            first(g, "hasSecondarySourceForNote")
          ),
        }
      })
      .filter((n): n is Note => n !== null)
  }

  // Build Person records
  const persons: Person[] = []
  for (const [personUri, g] of personGroups) {
    const dprrId = first(g, "hasDprrID")
    if (!dprrId) continue

    const sexUri = first(g, "isSex")
    const praenomenUri = first(g, "hasPraenomen")
    const tribeUri = first(g, "hasTribe")
    const personNumericId = personUri.startsWith(PERSON_PREFIX)
      ? personUri.slice(PERSON_PREFIX.length)
      : ""

    const postAssertions = buildPostAssertions(personUri)
    const officeNames = [
      ...new Set(
        postAssertions.map((pa) => pa.officeName).filter(Boolean)
      ),
    ]

    const filiation = first(g, "hasFiliation")

    persons.push({
      id: dprrId,
      uri: personUri,
      name: first(g, "hasPersonName") ?? dprrId,
      praenomen:
        (praenomenUri && refs.praenomina.get(praenomenUri)) ?? "",
      nomen: first(g, "hasNomen") ?? "",
      cognomen: first(g, "hasCognomen"),
      otherNames: first(g, "hasOtherNames"),
      filiation: filiation || null,
      reNumber: first(g, "hasReNumber"),
      sex: ((sexUri && refs.sexes.get(sexUri)) ?? "Male") as
        | "Male"
        | "Female",
      isPatrician: first(g, "isPatrician") === "true",
      isNobilis: first(g, "isNobilis") === "true",
      nobilisNotes: first(g, "hasNobilisNotes"),
      highestOffice: first(g, "hasHighestOffice"),
      eraFrom: firstNum(g, "hasEraFrom"),
      eraTo: firstNum(g, "hasEraTo"),
      tribe:
        (tribeUri && refs.tribes.get(tribeUri)?.name) ?? null,
      offices: officeNames,
      postAssertions,
      relationships: buildRelationships(personUri),
      dateInformation: buildDateInfo(personUri),
      personNotes: buildPersonNotes(personUri),
      concordances: concordanceMap.get(personNumericId) ?? [],
    })
  }

  return persons
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd site && vp test src/data/parse-persons.test.ts
```

- [ ] **Step 5: Commit**

```bash
cd site && git add src/data/parse-persons.ts src/data/parse-persons.test.ts
git commit -m "feat: add person TTL parser with reference resolution"
```

---

### Task 7: Data Loader Orchestrator

**Files:**
- Create: `site/src/data/loader.ts`

This module reads TTL files from disk, runs all parsers, and caches the result in a module-level singleton so it only runs once per build.

- [ ] **Step 1: Implement the loader**

```typescript
// site/src/data/loader.ts
import { readFile } from "node:fs/promises"
import { readdir } from "node:fs/promises"
import { join, basename } from "node:path"
import { parseReferenceTtl } from "./parse-references"
import { parseConcordanceTtl } from "./parse-concordances"
import { parsePersonTtl } from "./parse-persons"
import type { Person, PersonSummary, ReferenceMaps, Concordance } from "./types"

// Path from site/ to repo root
const REPO_ROOT = join(import.meta.dirname, "../..")

let _cache: { persons: Person[]; refs: ReferenceMaps } | null = null

async function readTtl(path: string): Promise<string> {
  return readFile(join(REPO_ROOT, path), "utf-8")
}

async function loadAllPersonFiles(): Promise<string[]> {
  const personsDir = join(REPO_ROOT, "persons")
  const gensDirs = await readdir(personsDir)
  const ttlContents: string[] = []

  for (const gens of gensDirs) {
    const gensPath = join(personsDir, gens)
    const files = await readdir(gensPath)
    for (const file of files) {
      if (file.endsWith(".ttl")) {
        const content = await readFile(join(gensPath, file), "utf-8")
        ttlContents.push(content)
      }
    }
  }

  return ttlContents
}

async function loadConcordances(): Promise<Map<string, Concordance[]>> {
  const concordDir = join(REPO_ROOT, "concordances")
  const files = await readdir(concordDir)
  const merged = new Map<string, Concordance[]>()

  for (const file of files) {
    if (!file.endsWith(".ttl")) continue
    const system = basename(file, ".ttl")
    const content = await readFile(join(concordDir, file), "utf-8")
    const parsed = parseConcordanceTtl(system, content)
    for (const [personId, links] of parsed) {
      const existing = merged.get(personId) ?? []
      existing.push(...links)
      merged.set(personId, existing)
    }
  }

  return merged
}

export async function loadAllData(): Promise<{
  persons: Person[]
  refs: ReferenceMaps
}> {
  if (_cache) return _cache

  // 1. Parse reference files
  const [offices, sources, praenomina, tribes, relationships, misc] =
    await Promise.all([
      readTtl("reference/offices.ttl"),
      readTtl("reference/sources.ttl"),
      readTtl("reference/praenomina.ttl"),
      readTtl("reference/tribes.ttl"),
      readTtl("reference/relationships.ttl"),
      readTtl("reference/misc.ttl"),
    ])

  const refs = await parseReferenceTtl({
    offices,
    sources,
    praenomina,
    tribes,
    relationships,
    misc,
  })

  // 2. Parse concordances
  const concordanceMap = await loadConcordances()

  // 3. Parse all person files
  const personTtls = await loadAllPersonFiles()
  const allPersons: Person[] = []

  for (const ttl of personTtls) {
    const persons = parsePersonTtl(ttl, refs, concordanceMap)
    allPersons.push(...persons)
  }

  // Deduplicate by DPRR ID (a person can appear in multiple files as
  // a related person stub — keep the one with the matching filename/most data)
  const byId = new Map<string, Person>()
  for (const p of allPersons) {
    const existing = byId.get(p.id)
    if (
      !existing ||
      p.postAssertions.length > existing.postAssertions.length
    ) {
      byId.set(p.id, p)
    }
  }

  const persons = [...byId.values()].sort((a, b) =>
    a.id.localeCompare(b.id)
  )

  _cache = { persons, refs }
  return _cache
}

/** Extract compact summaries for search/faceting. */
export function toSummaries(persons: Person[]): PersonSummary[] {
  return persons.map((p) => ({
    id: p.id,
    name: p.name,
    praenomen: p.praenomen,
    nomen: p.nomen,
    cognomen: p.cognomen,
    sex: p.sex,
    isPatrician: p.isPatrician,
    isNobilis: p.isNobilis,
    highestOffice: p.highestOffice,
    eraFrom: p.eraFrom,
    eraTo: p.eraTo,
    tribe: p.tribe,
    offices: p.offices,
  }))
}
```

- [ ] **Step 2: Verify the module compiles**

```bash
cd site && vp typecheck
```

Expected: No type errors related to `src/data/loader.ts`.

- [ ] **Step 3: Commit**

```bash
cd site && git add src/data/loader.ts
git commit -m "feat: add data loader orchestrator for TTL-to-JSON pipeline"
```

---

### Task 8: Search Index Builder

**Files:**
- Create: `site/src/data/search-index.ts`
- Create: `site/src/data/search-index.test.ts`

- [ ] **Step 1: Write tests**

```typescript
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
    sex: "Male",
    isPatrician: true,
    isNobilis: true,
    highestOffice: "cos. 509",
    eraFrom: -540,
    eraTo: -509,
    tribe: null,
    offices: ["consul"],
  },
  {
    id: "CORN0123",
    name: "CORN0123 P. Cornelius Scipio Africanus",
    praenomen: "Publius",
    nomen: "Cornelius",
    cognomen: "Scipio Africanus",
    sex: "Male",
    isPatrician: true,
    isNobilis: true,
    highestOffice: "cos. 205",
    eraFrom: -236,
    eraTo: -183,
    tribe: null,
    offices: ["consul", "proconsul"],
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
})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd site && vp test src/data/search-index.test.ts
```

- [ ] **Step 3: Implement**

```typescript
// site/src/data/search-index.ts
import MiniSearch, { type Options } from "minisearch"
import type { PersonSummary } from "./types"

export const MINISEARCH_OPTIONS: Options<PersonSummary> = {
  fields: ["name", "nomen", "cognomen", "highestOffice"],
  storeFields: [
    "id",
    "name",
    "nomen",
    "cognomen",
    "sex",
    "isPatrician",
    "isNobilis",
    "highestOffice",
    "eraFrom",
    "eraTo",
    "tribe",
    "offices",
  ],
  idField: "id",
  searchOptions: {
    prefix: true,
    fuzzy: 0.2,
    boost: { name: 2, cognomen: 1.5, nomen: 1.5 },
  },
}

/**
 * Build a MiniSearch index and return its JSON-serializable form.
 * The returned object can be passed to MiniSearch.loadJSON() on the client.
 */
export function buildSearchIndex(
  summaries: PersonSummary[]
): ReturnType<MiniSearch<PersonSummary>["toJSON"]> {
  const ms = new MiniSearch<PersonSummary>(MINISEARCH_OPTIONS)
  ms.addAll(summaries)
  return ms.toJSON()
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd site && vp test src/data/search-index.test.ts
```

- [ ] **Step 5: Commit**

```bash
cd site && git add src/data/search-index.ts src/data/search-index.test.ts
git commit -m "feat: add MiniSearch index builder with tests"
```

---

### Task 9: Server Functions

**Files:**
- Create: `site/src/server/data.ts`

Server functions wrap the data loader and expose data to routes. They run at prerender time for static builds.

- [ ] **Step 1: Implement server functions**

```typescript
// site/src/server/data.ts
import { createServerFn } from "@tanstack/react-start"
import { loadAllData, toSummaries } from "../data/loader"
import { buildSearchIndex, MINISEARCH_OPTIONS } from "../data/search-index"
import type { Person, PersonSummary } from "../data/types"

export const getPersonById = createServerFn({ method: "GET" })
  .validator((id: string) => id)
  .handler(async ({ data: id }) => {
    const { persons } = await loadAllData()
    const person = persons.find((p) => p.id === id)
    if (!person) {
      throw new Error(`Person not found: ${id}`)
    }
    return person
  })

export const getAllPersonIds = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return persons.map((p) => p.id)
  }
)

export const getSearchData = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    const summaries = toSummaries(persons)
    const searchIndex = buildSearchIndex(summaries)
    return {
      summaries,
      searchIndex,
      options: MINISEARCH_OPTIONS,
    }
  }
)
```

- [ ] **Step 2: Verify types compile**

```bash
cd site && vp typecheck
```

- [ ] **Step 3: Commit**

```bash
cd site && git add src/server/data.ts
git commit -m "feat: add server functions for person data and search index"
```

---

### Task 10: Install shadcn/ui Components

**Files:**
- Create: `site/src/components/ui/badge.tsx`
- Create: `site/src/components/ui/checkbox.tsx`
- Create: `site/src/components/ui/input.tsx`
- Create: `site/src/components/ui/collapsible.tsx`

- [ ] **Step 1: Add shadcn components**

```bash
cd site && vp dlx shadcn@latest add badge checkbox input collapsible
```

- [ ] **Step 2: Verify components were added**

```bash
ls site/src/components/ui/
```

Expected: `badge.tsx`, `button.tsx`, `checkbox.tsx`, `collapsible.tsx`, `input.tsx`

- [ ] **Step 3: Commit**

```bash
cd site && git add src/components/ui/
git commit -m "chore: add shadcn badge, checkbox, input, collapsible components"
```

---

### Task 11: Reusable Display Components

**Files:**
- Create: `site/src/components/date-display.tsx`
- Create: `site/src/components/source-citation.tsx`
- Create: `site/src/components/section.tsx`
- Create: `site/src/components/person-card.tsx`

- [ ] **Step 1: Create DateDisplay component**

```tsx
// site/src/components/date-display.tsx
import { formatYear, formatEraRange } from "@/lib/dates"

export function DateDisplay({
  year,
  uncertain = false,
}: {
  year: number
  uncertain?: boolean
}) {
  return <span>{formatYear(year, uncertain)}</span>
}

export function EraRange({
  from,
  to,
}: {
  from: number | null
  to: number | null
}) {
  const text = formatEraRange(from, to)
  if (!text) return null
  return <span>{text}</span>
}
```

- [ ] **Step 2: Create SourceCitation component**

```tsx
// site/src/components/source-citation.tsx
export function SourceCitation({
  name,
  className,
}: {
  name: string
  className?: string
}) {
  if (!name) return null
  return <cite className={className}>{name}</cite>
}
```

- [ ] **Step 3: Create Section component**

```tsx
// site/src/components/section.tsx
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { ChevronRight } from "lucide-react"
import { useState } from "react"
import { cn } from "@/lib/utils"

export function Section({
  title,
  children,
  defaultOpen = true,
  count,
}: {
  title: string
  children: React.ReactNode
  defaultOpen?: boolean
  count?: number
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex w-full items-center gap-2 py-3">
        <ChevronRight
          className={cn(
            "h-4 w-4 shrink-0 transition-transform",
            open && "rotate-90"
          )}
        />
        <h2 className="font-heading text-lg font-semibold">{title}</h2>
        {count !== undefined && (
          <span className="text-muted-foreground text-sm">({count})</span>
        )}
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-4 pl-6">
        {children}
      </CollapsibleContent>
    </Collapsible>
  )
}
```

- [ ] **Step 4: Create PersonCard component**

```tsx
// site/src/components/person-card.tsx
import { Link } from "@tanstack/react-router"
import { Badge } from "@/components/ui/badge"
import { EraRange } from "@/components/date-display"
import type { PersonSummary } from "@/data/types"

export function PersonCard({ person }: { person: PersonSummary }) {
  // Strip the DPRR ID prefix from the display name
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")

  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="hover:bg-accent block rounded-md border p-3 transition-colors"
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="font-heading truncate font-medium">{displayName}</p>
          <p className="text-muted-foreground text-sm">
            {person.highestOffice && (
              <span>{person.highestOffice}</span>
            )}
            {person.highestOffice && (person.eraFrom || person.eraTo) && (
              <span> · </span>
            )}
            <EraRange from={person.eraFrom} to={person.eraTo} />
          </p>
        </div>
        <div className="flex shrink-0 gap-1">
          {person.isPatrician && (
            <Badge variant="secondary">Patrician</Badge>
          )}
          {person.isNobilis && (
            <Badge variant="secondary">Nobilis</Badge>
          )}
        </div>
      </div>
    </Link>
  )
}
```

- [ ] **Step 5: Verify types compile**

```bash
cd site && vp typecheck
```

- [ ] **Step 6: Commit**

```bash
cd site && git add src/components/date-display.tsx src/components/source-citation.tsx src/components/section.tsx src/components/person-card.tsx
git commit -m "feat: add reusable display components (DateDisplay, Section, SourceCitation, PersonCard)"
```

---

### Task 12: Person Detail Page

**Files:**
- Create: `site/src/routes/persons.$id.tsx`
- Modify: `site/src/routes/__root.tsx` (update site title)

- [ ] **Step 1: Create the person detail route**

```tsx
// site/src/routes/persons.$id.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getPersonById } from "@/server/data"
import { Badge } from "@/components/ui/badge"
import { Section } from "@/components/section"
import { DateDisplay, EraRange } from "@/components/date-display"
import { SourceCitation } from "@/components/source-citation"
import { PersonCard } from "@/components/person-card"
import { formatYear } from "@/lib/dates"
import type {
  Person,
  PostAssertion,
  Relationship,
  DateInfo,
  Note,
  Concordance,
} from "@/data/types"

export const Route = createFileRoute("/persons/$id")({
  loader: ({ params }) => getPersonById({ data: params.id }),
  head: ({ loaderData: person }) => {
    const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
    const desc = [person.highestOffice, person.isPatrician ? "Patrician" : null]
      .filter(Boolean)
      .join(" · ")
    return {
      meta: [
        { title: `${displayName} (${person.id}) — DPRR` },
        { name: "description", content: desc },
        { property: "og:title", content: `${displayName} — DPRR` },
        { property: "og:description", content: desc },
        { property: "og:type", content: "profile" },
      ],
    }
  },
  component: PersonPage,
})

function PersonPage() {
  const person = Route.useLoaderData()
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <PersonHeader person={person} displayName={displayName} />

      {person.postAssertions.length > 0 && (
        <Section title="Offices" count={person.postAssertions.length}>
          <div className="space-y-4">
            {person.postAssertions.map((pa) => (
              <OfficeEntry key={pa.id} assertion={pa} />
            ))}
          </div>
        </Section>
      )}

      {person.relationships.length > 0 && (
        <Section title="Relationships" count={person.relationships.length}>
          <div className="space-y-3">
            {person.relationships.map((rel) => (
              <RelationshipEntry key={rel.id} relationship={rel} />
            ))}
          </div>
        </Section>
      )}

      {person.dateInformation.length > 0 && (
        <Section title="Dates" count={person.dateInformation.length}>
          <div className="space-y-2">
            {person.dateInformation.map((d, i) => (
              <DateEntry key={i} dateInfo={d} />
            ))}
          </div>
        </Section>
      )}

      {person.personNotes.length > 0 && (
        <Section title="Notes" count={person.personNotes.length}>
          <div className="space-y-4">
            {person.personNotes.map((note, i) => (
              <NoteEntry key={i} note={note} />
            ))}
          </div>
        </Section>
      )}

      {person.concordances.length > 0 && (
        <Section title="External Links" count={person.concordances.length}>
          <ConcordanceList concordances={person.concordances} />
        </Section>
      )}
    </div>
  )
}

function PersonHeader({
  person,
  displayName,
}: {
  person: Person
  displayName: string
}) {
  return (
    <header className="mb-8">
      <h1 className="font-heading text-3xl font-bold">{displayName}</h1>
      <p className="text-muted-foreground mt-1 text-lg">
        <EraRange from={person.eraFrom} to={person.eraTo} />
      </p>
      <div className="mt-3 flex flex-wrap gap-2">
        {person.sex && <Badge variant="outline">{person.sex}</Badge>}
        {person.isPatrician && <Badge variant="secondary">Patrician</Badge>}
        {person.isNobilis && <Badge variant="secondary">Nobilis</Badge>}
      </div>
      <dl className="text-muted-foreground mt-4 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
        {person.praenomen && (
          <>
            <dt className="font-medium">Praenomen</dt>
            <dd>{person.praenomen}</dd>
          </>
        )}
        {person.nomen && (
          <>
            <dt className="font-medium">Nomen</dt>
            <dd>{person.nomen}</dd>
          </>
        )}
        {person.cognomen && (
          <>
            <dt className="font-medium">Cognomen</dt>
            <dd>{person.cognomen}</dd>
          </>
        )}
        {person.filiation && (
          <>
            <dt className="font-medium">Filiation</dt>
            <dd>{person.filiation}</dd>
          </>
        )}
        {person.reNumber && (
          <>
            <dt className="font-medium">RE</dt>
            <dd>{person.reNumber}</dd>
          </>
        )}
        {person.tribe && (
          <>
            <dt className="font-medium">Tribe</dt>
            <dd>{person.tribe}</dd>
          </>
        )}
        {person.highestOffice && (
          <>
            <dt className="font-medium">Highest Office</dt>
            <dd>{person.highestOffice}</dd>
          </>
        )}
        <dt className="font-medium">DPRR ID</dt>
        <dd className="font-mono text-xs">{person.id}</dd>
      </dl>
      {person.nobilisNotes && (
        <p className="mt-3 text-sm italic">{person.nobilisNotes}</p>
      )}
    </header>
  )
}

function OfficeEntry({ assertion }: { assertion: PostAssertion }) {
  return (
    <div className="border-l-2 pl-4">
      <p className="font-medium">
        {assertion.officeName}
        {assertion.officeAbbreviation && (
          <span className="text-muted-foreground ml-1 text-sm">
            ({assertion.officeAbbreviation})
          </span>
        )}
      </p>
      {(assertion.dateStart || assertion.dateEnd) && (
        <p className="text-muted-foreground text-sm">
          {assertion.dateStart !== null && assertion.dateEnd !== null ? (
            assertion.dateStart === assertion.dateEnd ? (
              <DateDisplay year={assertion.dateStart} />
            ) : (
              <EraRange from={assertion.dateStart} to={assertion.dateEnd} />
            )
          ) : (
            <DateDisplay year={assertion.dateStart ?? assertion.dateEnd!} />
          )}
        </p>
      )}
      {assertion.originalText && (
        <p className="mt-1 text-sm">{assertion.originalText}</p>
      )}
      <SourceCitation
        name={assertion.secondarySource}
        className="text-muted-foreground mt-1 block text-xs"
      />
      {assertion.primarySourceRefs.length > 0 && (
        <div className="mt-1">
          {assertion.primarySourceRefs.map((ref, i) => (
            <p key={i} className="text-muted-foreground text-xs">
              {ref}
            </p>
          ))}
        </div>
      )}
      {assertion.notes.map((note, i) => (
        <div key={i} className="mt-2 rounded bg-muted/50 p-3 text-sm">
          <p className="text-muted-foreground mb-1 text-xs font-medium">
            {note.type}
            {note.secondarySource && ` — ${note.secondarySource}`}
          </p>
          <p className="whitespace-pre-wrap leading-relaxed">{note.text}</p>
        </div>
      ))}
    </div>
  )
}

function RelationshipEntry({
  relationship,
}: {
  relationship: Relationship
}) {
  const relatedDisplayName = relationship.relatedPersonName.replace(
    /^[A-Z]{4}\d+ /,
    ""
  )
  return (
    <div className="flex items-baseline gap-2">
      <span className="text-muted-foreground text-sm capitalize">
        {relationship.relationshipType}:
      </span>
      {relationship.relatedPersonId ? (
        <PersonCard
          person={{
            id: relationship.relatedPersonId,
            name: relationship.relatedPersonName,
            praenomen: "",
            nomen: "",
            cognomen: null,
            sex: "Male",
            isPatrician: false,
            isNobilis: false,
            highestOffice: null,
            eraFrom: null,
            eraTo: null,
            tribe: null,
            offices: [],
          }}
        />
      ) : (
        <span>{relatedDisplayName}</span>
      )}
    </div>
  )
}

function DateEntry({ dateInfo }: { dateInfo: DateInfo }) {
  return (
    <div className="flex items-baseline gap-2 text-sm">
      <span className="text-muted-foreground font-medium capitalize">
        {dateInfo.type}:
      </span>
      <DateDisplay year={dateInfo.value} uncertain={dateInfo.isUncertain} />
      {dateInfo.notes && (
        <span className="text-muted-foreground">— {dateInfo.notes}</span>
      )}
      <SourceCitation
        name={dateInfo.secondarySource}
        className="text-muted-foreground text-xs"
      />
    </div>
  )
}

function NoteEntry({ note }: { note: Note }) {
  return (
    <div className="rounded bg-muted/50 p-3">
      <p className="text-muted-foreground mb-1 text-xs font-medium">
        {note.type}
        {note.secondarySource && ` — ${note.secondarySource}`}
      </p>
      <p className="whitespace-pre-wrap text-sm leading-relaxed">
        {note.text}
      </p>
    </div>
  )
}

function ConcordanceList({
  concordances,
}: {
  concordances: Concordance[]
}) {
  // Group by system
  const grouped = new Map<string, Concordance[]>()
  for (const c of concordances) {
    const existing = grouped.get(c.system) ?? []
    existing.push(c)
    grouped.set(c.system, existing)
  }

  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
      {[...grouped].map(([system, links]) => (
        <div key={system} className="contents">
          <dt className="font-medium capitalize">{system}</dt>
          <dd>
            {links.map((link, i) => (
              <a
                key={i}
                href={link.uri}
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary hover:underline"
              >
                {link.uri}
              </a>
            ))}
          </dd>
        </div>
      ))}
    </dl>
  )
}
```

- [ ] **Step 2: Update root route with site-level defaults**

In `site/src/routes/__root.tsx`, update the meta title:

```tsx
// Replace the existing meta array in the head() function:
meta: [
  {
    charSet: "utf-8",
  },
  {
    name: "viewport",
    content: "width=device-width, initial-scale=1",
  },
  {
    title: "DPRR — Digital Prosopography of the Roman Republic",
  },
  {
    name: "description",
    content:
      "Search and browse 4,876 persons from the Roman Republic (509–31 BC)",
  },
],
```

- [ ] **Step 3: Verify types compile**

```bash
cd site && vp typecheck
```

- [ ] **Step 4: Commit**

```bash
cd site && git add src/routes/persons.\$id.tsx src/routes/__root.tsx
git commit -m "feat: add person detail page with full scholarly view"
```

---

### Task 13: Search State Hook

**Files:**
- Create: `site/src/lib/search.ts`

This hook manages the bidirectional sync between URL query params, MiniSearch, and facet filtering.

- [ ] **Step 1: Implement the search state hook**

```typescript
// site/src/lib/search.ts
import { useCallback, useMemo } from "react"
import { useNavigate, useSearch } from "@tanstack/react-router"
import MiniSearch from "minisearch"
import type { PersonSummary, SearchState, FacetValue } from "@/data/types"
import { MINISEARCH_OPTIONS } from "@/data/search-index"

function parseSearchParams(params: Record<string, string>): SearchState {
  return {
    q: params.q ?? "",
    office: params.office ? params.office.split(",") : [],
    nomen: params.nomen ? params.nomen.split(",") : [],
    sex: params.sex ? params.sex.split(",") : [],
    patrician:
      params.patrician === "true"
        ? true
        : params.patrician === "false"
          ? false
          : null,
    nobilis:
      params.nobilis === "true"
        ? true
        : params.nobilis === "false"
          ? false
          : null,
    tribe: params.tribe ? params.tribe.split(",") : [],
    eraFrom: params.eraFrom ? Number(params.eraFrom) : null,
    eraTo: params.eraTo ? Number(params.eraTo) : null,
  }
}

function toSearchParams(state: SearchState): Record<string, string> {
  const params: Record<string, string> = {}
  if (state.q) params.q = state.q
  if (state.office.length) params.office = state.office.join(",")
  if (state.nomen.length) params.nomen = state.nomen.join(",")
  if (state.sex.length) params.sex = state.sex.join(",")
  if (state.patrician !== null)
    params.patrician = String(state.patrician)
  if (state.nobilis !== null) params.nobilis = String(state.nobilis)
  if (state.tribe.length) params.tribe = state.tribe.join(",")
  if (state.eraFrom !== null) params.eraFrom = String(state.eraFrom)
  if (state.eraTo !== null) params.eraTo = String(state.eraTo)
  return params
}

function matchesFacets(person: PersonSummary, state: SearchState): boolean {
  if (
    state.office.length > 0 &&
    !state.office.some((o) => person.offices.includes(o))
  )
    return false
  if (state.nomen.length > 0 && !state.nomen.includes(person.nomen))
    return false
  if (state.sex.length > 0 && !state.sex.includes(person.sex))
    return false
  if (state.patrician !== null && person.isPatrician !== state.patrician)
    return false
  if (state.nobilis !== null && person.isNobilis !== state.nobilis)
    return false
  if (
    state.tribe.length > 0 &&
    (!person.tribe || !state.tribe.includes(person.tribe))
  )
    return false
  if (
    state.eraFrom !== null &&
    (person.eraTo === null || person.eraTo < state.eraFrom)
  )
    return false
  if (
    state.eraTo !== null &&
    (person.eraFrom === null || person.eraFrom > state.eraTo)
  )
    return false
  return true
}

function computeFacetValues(
  persons: PersonSummary[],
  field: keyof PersonSummary
): FacetValue[] {
  const counts = new Map<string, number>()
  for (const p of persons) {
    const val = p[field]
    if (Array.isArray(val)) {
      for (const v of val) {
        counts.set(v, (counts.get(v) ?? 0) + 1)
      }
    } else if (typeof val === "string" && val) {
      counts.set(val, (counts.get(val) ?? 0) + 1)
    }
  }
  return [...counts]
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count)
}

export function useSearchState(
  summaries: PersonSummary[],
  searchIndexJson: object
) {
  const rawParams = useSearch({ strict: false }) as Record<string, string>
  const navigate = useNavigate()
  const state = useMemo(() => parseSearchParams(rawParams), [rawParams])

  const miniSearch = useMemo(() => {
    return MiniSearch.loadJSON<PersonSummary>(
      JSON.stringify(searchIndexJson),
      MINISEARCH_OPTIONS
    )
  }, [searchIndexJson])

  const results = useMemo(() => {
    let candidates: PersonSummary[]

    if (state.q.trim()) {
      const searchResults = miniSearch.search(state.q)
      const idSet = new Set(searchResults.map((r) => r.id))
      candidates = summaries.filter((p) => idSet.has(p.id))
    } else {
      candidates = summaries
    }

    return candidates.filter((p) => matchesFacets(p, state))
  }, [state, summaries, miniSearch])

  const facets = useMemo(
    () => ({
      office: computeFacetValues(results, "offices"),
      nomen: computeFacetValues(results, "nomen"),
      sex: computeFacetValues(results, "sex"),
      tribe: computeFacetValues(results, "tribe"),
    }),
    [results]
  )

  const updateState = useCallback(
    (updates: Partial<SearchState>) => {
      const newState = { ...state, ...updates }
      void navigate({
        to: "/",
        search: toSearchParams(newState),
        replace: true,
      })
    },
    [state, navigate]
  )

  const clearAll = useCallback(() => {
    void navigate({ to: "/", search: {}, replace: true })
  }, [navigate])

  return { state, results, facets, updateState, clearAll }
}
```

- [ ] **Step 2: Verify types compile**

```bash
cd site && vp typecheck
```

- [ ] **Step 3: Commit**

```bash
cd site && git add src/lib/search.ts
git commit -m "feat: add search state hook with MiniSearch + facet filtering"
```

---

### Task 14: Search UI Components

**Files:**
- Create: `site/src/components/search-input.tsx`
- Create: `site/src/components/active-filter-chips.tsx`
- Create: `site/src/components/facet-group.tsx`
- Create: `site/src/components/facet-range-group.tsx`
- Create: `site/src/components/facet-sidebar.tsx`
- Create: `site/src/components/results-list.tsx`

- [ ] **Step 1: Create SearchInput**

```tsx
// site/src/components/search-input.tsx
import { useEffect, useRef, useState } from "react"
import { Input } from "@/components/ui/input"
import { Search } from "lucide-react"

export function SearchInput({
  value,
  onChange,
}: {
  value: string
  onChange: (value: string) => void
}) {
  const [local, setLocal] = useState(value)
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>()

  useEffect(() => {
    setLocal(value)
  }, [value])

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    setLocal(v)
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => onChange(v), 200)
  }

  return (
    <div className="relative">
      <Search className="text-muted-foreground absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2" />
      <Input
        type="search"
        placeholder="Search persons..."
        value={local}
        onChange={handleChange}
        className="pl-9"
      />
    </div>
  )
}
```

- [ ] **Step 2: Create ActiveFilterChips**

```tsx
// site/src/components/active-filter-chips.tsx
import { Badge } from "@/components/ui/badge"
import { X } from "lucide-react"
import type { SearchState } from "@/data/types"

interface ActiveFilterChipsProps {
  state: SearchState
  onRemove: (updates: Partial<SearchState>) => void
  onClearAll: () => void
}

export function ActiveFilterChips({
  state,
  onRemove,
  onClearAll,
}: ActiveFilterChipsProps) {
  const chips: { label: string; onRemove: () => void }[] = []

  for (const office of state.office) {
    chips.push({
      label: `Office: ${office}`,
      onRemove: () =>
        onRemove({ office: state.office.filter((o) => o !== office) }),
    })
  }
  for (const nomen of state.nomen) {
    chips.push({
      label: `Gens: ${nomen}`,
      onRemove: () =>
        onRemove({ nomen: state.nomen.filter((n) => n !== nomen) }),
    })
  }
  for (const sex of state.sex) {
    chips.push({
      label: sex,
      onRemove: () =>
        onRemove({ sex: state.sex.filter((s) => s !== sex) }),
    })
  }
  if (state.patrician !== null) {
    chips.push({
      label: state.patrician ? "Patrician" : "Non-Patrician",
      onRemove: () => onRemove({ patrician: null }),
    })
  }
  if (state.nobilis !== null) {
    chips.push({
      label: state.nobilis ? "Nobilis" : "Non-Nobilis",
      onRemove: () => onRemove({ nobilis: null }),
    })
  }
  for (const tribe of state.tribe) {
    chips.push({
      label: `Tribe: ${tribe}`,
      onRemove: () =>
        onRemove({ tribe: state.tribe.filter((t) => t !== tribe) }),
    })
  }

  if (chips.length === 0) return null

  return (
    <div className="flex flex-wrap items-center gap-2">
      {chips.map((chip) => (
        <Badge
          key={chip.label}
          variant="secondary"
          className="cursor-pointer gap-1"
          onClick={chip.onRemove}
        >
          {chip.label}
          <X className="h-3 w-3" />
        </Badge>
      ))}
      {chips.length > 1 && (
        <button
          onClick={onClearAll}
          className="text-muted-foreground hover:text-foreground text-xs underline"
        >
          Clear all
        </button>
      )}
    </div>
  )
}
```

- [ ] **Step 3: Create FacetGroup**

```tsx
// site/src/components/facet-group.tsx
import { useState } from "react"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"
import type { FacetValue } from "@/data/types"

const DEFAULT_VISIBLE = 10

export function FacetGroup({
  title,
  items,
  selected,
  onChange,
  defaultOpen = true,
  searchable = false,
}: {
  title: string
  items: FacetValue[]
  selected: string[]
  onChange: (selected: string[]) => void
  defaultOpen?: boolean
  searchable?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  const [filter, setFilter] = useState("")
  const [showAll, setShowAll] = useState(false)

  const filtered = filter
    ? items.filter((item) =>
        item.value.toLowerCase().includes(filter.toLowerCase())
      )
    : items

  const visible = showAll ? filtered : filtered.slice(0, DEFAULT_VISIBLE)
  const hasMore = filtered.length > DEFAULT_VISIBLE

  function toggle(value: string) {
    if (selected.includes(value)) {
      onChange(selected.filter((v) => v !== value))
    } else {
      onChange([...selected, value])
    }
  }

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex w-full items-center gap-1.5 py-2 text-sm font-semibold">
        <ChevronRight
          className={cn(
            "h-3.5 w-3.5 shrink-0 transition-transform",
            open && "rotate-90"
          )}
        />
        {title}
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-3 pl-5">
        {searchable && (
          <Input
            type="search"
            placeholder={`Filter ${title.toLowerCase()}...`}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="mb-2 h-7 text-xs"
          />
        )}
        <div className="space-y-1">
          {visible.map((item) => (
            <label
              key={item.value}
              className="flex cursor-pointer items-center gap-2 text-sm"
            >
              <Checkbox
                checked={selected.includes(item.value)}
                onCheckedChange={() => toggle(item.value)}
              />
              <span className="min-w-0 truncate">{item.value}</span>
              <span className="text-muted-foreground ml-auto text-xs">
                {item.count}
              </span>
            </label>
          ))}
        </div>
        {hasMore && !showAll && (
          <button
            onClick={() => setShowAll(true)}
            className="text-muted-foreground mt-1 text-xs hover:underline"
          >
            + {filtered.length - DEFAULT_VISIBLE} more...
          </button>
        )}
        {showAll && hasMore && (
          <button
            onClick={() => setShowAll(false)}
            className="text-muted-foreground mt-1 text-xs hover:underline"
          >
            Show less
          </button>
        )}
      </CollapsibleContent>
    </Collapsible>
  )
}
```

- [ ] **Step 4: Create FacetRangeGroup**

```tsx
// site/src/components/facet-range-group.tsx
import { useState } from "react"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Input } from "@/components/ui/input"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"

export function FacetRangeGroup({
  title,
  fromValue,
  toValue,
  onFromChange,
  onToChange,
  fromPlaceholder = "From",
  toPlaceholder = "To",
  defaultOpen = true,
}: {
  title: string
  fromValue: number | null
  toValue: number | null
  onFromChange: (value: number | null) => void
  onToChange: (value: number | null) => void
  fromPlaceholder?: string
  toPlaceholder?: string
  defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)

  function handleFrom(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    onFromChange(v === "" ? null : Number(v))
  }

  function handleTo(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    onToChange(v === "" ? null : Number(v))
  }

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex w-full items-center gap-1.5 py-2 text-sm font-semibold">
        <ChevronRight
          className={cn(
            "h-3.5 w-3.5 shrink-0 transition-transform",
            open && "rotate-90"
          )}
        />
        {title}
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-3 pl-5">
        <p className="text-muted-foreground mb-2 text-xs">
          Use negative numbers for BC (e.g., -509)
        </p>
        <div className="flex items-center gap-2">
          <Input
            type="number"
            placeholder={fromPlaceholder}
            value={fromValue ?? ""}
            onChange={handleFrom}
            className="h-7 text-xs"
          />
          <span className="text-muted-foreground text-xs">to</span>
          <Input
            type="number"
            placeholder={toPlaceholder}
            value={toValue ?? ""}
            onChange={handleTo}
            className="h-7 text-xs"
          />
        </div>
      </CollapsibleContent>
    </Collapsible>
  )
}
```

- [ ] **Step 5: Create FacetSidebar**

```tsx
// site/src/components/facet-sidebar.tsx
import { FacetGroup } from "./facet-group"
import { FacetRangeGroup } from "./facet-range-group"
import type { SearchState, FacetValue } from "@/data/types"

interface FacetSidebarProps {
  facets: {
    office: FacetValue[]
    nomen: FacetValue[]
    sex: FacetValue[]
    tribe: FacetValue[]
  }
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
}

export function FacetSidebar({
  facets,
  state,
  onUpdate,
}: FacetSidebarProps) {
  return (
    <aside className="w-56 shrink-0 space-y-1">
      {/* Primary facets — expanded by default */}
      <FacetGroup
        title="Office"
        items={facets.office}
        selected={state.office}
        onChange={(office) => onUpdate({ office })}
        searchable
      />
      <FacetGroup
        title="Gens"
        items={facets.nomen}
        selected={state.nomen}
        onChange={(nomen) => onUpdate({ nomen })}
        searchable
      />
      <FacetRangeGroup
        title="Era"
        fromValue={state.eraFrom}
        toValue={state.eraTo}
        onFromChange={(eraFrom) => onUpdate({ eraFrom })}
        onToChange={(eraTo) => onUpdate({ eraTo })}
        fromPlaceholder="-509"
        toPlaceholder="-31"
      />

      {/* Secondary facets — collapsed by default */}
      <FacetGroup
        title="Sex"
        items={facets.sex}
        selected={state.sex}
        onChange={(sex) => onUpdate({ sex })}
        defaultOpen={false}
      />
      <FacetGroup
        title="Tribe"
        items={facets.tribe}
        selected={state.tribe}
        onChange={(tribe) => onUpdate({ tribe })}
        defaultOpen={false}
        searchable
      />
    </aside>
  )
}
```

- [ ] **Step 6: Create ResultsList**

```tsx
// site/src/components/results-list.tsx
import { useState } from "react"
import { PersonCard } from "./person-card"
import type { PersonSummary } from "@/data/types"

const PAGE_SIZE = 50

export function ResultsList({ results }: { results: PersonSummary[] }) {
  const [page, setPage] = useState(0)
  const totalPages = Math.ceil(results.length / PAGE_SIZE)
  const visible = results.slice(0, (page + 1) * PAGE_SIZE)

  return (
    <div>
      <p className="text-muted-foreground mb-3 text-sm">
        {results.length.toLocaleString()} result
        {results.length !== 1 && "s"}
      </p>
      <div className="space-y-2">
        {visible.map((person) => (
          <PersonCard key={person.id} person={person} />
        ))}
      </div>
      {page + 1 < totalPages && (
        <button
          onClick={() => setPage((p) => p + 1)}
          className="text-primary mt-4 text-sm hover:underline"
        >
          Show more ({results.length - visible.length} remaining)
        </button>
      )}
    </div>
  )
}
```

- [ ] **Step 7: Verify types compile**

```bash
cd site && vp typecheck
```

- [ ] **Step 8: Commit**

```bash
cd site && git add src/components/search-input.tsx src/components/active-filter-chips.tsx src/components/facet-group.tsx src/components/facet-range-group.tsx src/components/facet-sidebar.tsx src/components/results-list.tsx
git commit -m "feat: add search UI components (facets, chips, results)"
```

---

### Task 15: Search Page

**Files:**
- Modify: `site/src/routes/index.tsx`

- [ ] **Step 1: Replace the index route with the search page**

```tsx
// site/src/routes/index.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getSearchData } from "@/server/data"
import { useSearchState } from "@/lib/search"
import { SearchInput } from "@/components/search-input"
import { ActiveFilterChips } from "@/components/active-filter-chips"
import { FacetSidebar } from "@/components/facet-sidebar"
import { ResultsList } from "@/components/results-list"

export const Route = createFileRoute("/")({
  validateSearch: (search: Record<string, unknown>) => search,
  loader: () => getSearchData(),
  component: SearchPage,
})

function SearchPage() {
  const { summaries, searchIndex } = Route.useLoaderData()
  const { state, results, facets, updateState, clearAll } = useSearchState(
    summaries,
    searchIndex
  )

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <header className="mb-6">
        <h1 className="font-heading text-2xl font-bold">
          Digital Prosopography of the Roman Republic
        </h1>
        <p className="text-muted-foreground text-sm">
          Search and browse {summaries.length.toLocaleString()} persons
          (509–31 BC)
        </p>
      </header>

      <SearchInput
        value={state.q}
        onChange={(q) => updateState({ q })}
      />

      <div className="mt-3">
        <ActiveFilterChips
          state={state}
          onRemove={updateState}
          onClearAll={clearAll}
        />
      </div>

      <div className="mt-4 flex gap-6">
        <FacetSidebar
          facets={facets}
          state={state}
          onUpdate={updateState}
        />
        <main className="min-w-0 flex-1">
          <ResultsList results={results} />
        </main>
      </div>
    </div>
  )
}
```

- [ ] **Step 2: Verify types compile**

```bash
cd site && vp typecheck
```

- [ ] **Step 3: Start the dev server and verify**

```bash
cd site && vp dev --port 3000
```

Open http://localhost:3000. Verify:
- Search page loads with person list
- Text search filters results
- Facet checkboxes filter results
- Filter chips appear and are removable
- Clicking a person card navigates to the detail page

- [ ] **Step 4: Commit**

```bash
cd site && git add src/routes/index.tsx
git commit -m "feat: add search page with faceted filtering"
```

---

### Task 16: Prerender All Person Routes

**Files:**
- Modify: `site/src/routes/index.tsx` (add links for crawling)
- Potentially modify: `site/vite.config.ts`

The static build needs to know about all ~4,900 person routes. TanStack Start's `crawlLinks` option will discover them automatically if the search page links to each person. The search page already renders `PersonCard` with `<Link to="/persons/$id">`, so `crawlLinks: true` (set in Task 1) should handle this.

- [ ] **Step 1: Add a hidden link list for the crawler**

The search page only shows 50 results at a time via pagination, so the crawler won't find all person pages. Add a hidden link list that renders all person IDs for the prerender crawler to discover:

At the bottom of `SearchPage` in `site/src/routes/index.tsx`, inside the root `<div>`, add:

```tsx
{/* Hidden links for static prerender crawler */}
<div className="hidden" aria-hidden="true">
  {summaries.map((p) => (
    <a key={p.id} href={`/persons/${p.id}`}>
      {p.id}
    </a>
  ))}
</div>
```

- [ ] **Step 2: Test the static build**

```bash
cd site && vp build
```

Expected: Build completes and generates `dist/` with:
- `dist/index.html`
- `dist/persons/IUNI0001/index.html` (and ~4,900 others)

Verify:
```bash
ls site/dist/persons/ | head -20
find site/dist/persons -name 'index.html' | wc -l
```

Expected: ~4,900 HTML files.

- [ ] **Step 3: Preview the static build**

```bash
cd site && vp preview
```

Open http://localhost:4173. Verify search and person pages work as static HTML.

- [ ] **Step 4: Commit**

```bash
cd site && git add src/routes/index.tsx
git commit -m "feat: add hidden link list for static prerender crawling"
```

---

### Task 17: GitHub Pages Deployment Workflow

**Files:**
- Create: `.github/workflows/deploy-site.yml`

- [ ] **Step 1: Create the deployment workflow**

```yaml
# .github/workflows/deploy-site.yml
name: Deploy Site to GitHub Pages

on:
  push:
    branches: [main]
    paths:
      - 'site/**'
      - 'persons/**'
      - 'reference/**'
      - 'concordances/**'
      - 'ontology.ttl'

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: true

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: voidzero-dev/setup-vp@v1
        with:
          cache: true

      - name: Install dependencies
        run: cd site && vp install

      - name: Build static site
        run: cd site && vp build

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: site/dist

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/deploy-site.yml
git commit -m "ci: add GitHub Pages deployment workflow"
```

---

### Task 18: Smoke Test & Polish

**Files:**
- Various (fixes found during testing)

- [ ] **Step 1: Run all tests**

```bash
cd site && vp test
```

Expected: All tests pass.

- [ ] **Step 2: Run type checking**

```bash
cd site && vp typecheck
```

Expected: No errors.

- [ ] **Step 3: Run the full static build**

```bash
cd site && vp build
```

Expected: Completes without errors. Check the output size:

```bash
du -sh site/dist/
find site/dist/persons -name 'index.html' | wc -l
```

- [ ] **Step 4: Preview and manually verify**

```bash
cd site && vp preview
```

Verify:
- [ ] Search page loads and shows all persons
- [ ] Text search filters results (try "Brutus", "Cornelius")
- [ ] Facet filtering works (select an office, a gens)
- [ ] Filter chips appear and remove correctly
- [ ] URL updates when filters change
- [ ] Navigating to a person page works
- [ ] Person detail shows: header, offices, relationships, dates, notes, concordances
- [ ] Related person links navigate to the correct page
- [ ] Browser back/forward works with search state
- [ ] Meta tags are correct (view source on a person page)

- [ ] **Step 5: Fix any issues found, commit**

```bash
git add -A
git commit -m "fix: polish and bug fixes from smoke testing"
```
