# Advanced Search Gap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the query-capability gap against the legacy DPRR person search (status facet, father/grandfather filters, relationship-context result lines) and replace the tall facet sidebar with a progressive-disclosure filter band.

**Architecture:** Data flows one way: TTL → `parsePersonTtl` → `Person[]` → `toSummaries` → static JSON payload → client filtering (`matchesFacets`) driven by URL params. New capabilities enter at the parser (StatusAssertions, isNovus, filiation ancestors), get denormalized onto `PersonSummary` at build time (combined `statuses`, `father`, `grandfather`, `contextLine`), and surface through new URL params + a new `FilterBand` UI that replaces `FacetSidebar`.

**Tech Stack:** TypeScript, TanStack Start (prerendered static site), Vite+ (`vp` CLI — `vp test`, `vp check`, `vp build`), Vitest via `vp test`, radix-ui (single `radix-ui` package, already a dependency), Tailwind v4 with the site's "Editorial Ledger" utility classes (`micro-label`, `micro-label-muted`, `rule-hair`, `ledger-row`, `small-caps`).

Spec: `docs/superpowers/specs/2026-08-09-advanced-search-gap-design.md`

## Global Constraints

- All commands run from `site/` (`cd site`). Use `vp test`, `vp check`, `vp build` — never npm/pnpm directly, never `npx`.
- Formatting is enforced: run `vp check` before each commit; `vp check --fix` to fix.
- Status facet semantics are **AND within the facet** (`every`), unlike other facets — statuses are attributes of one person, and this preserves the old "Patrician ∧ Nobilis" queries. Other facets stay OR.
- Old URL params `patrician=true` / `nobilis=true` must keep working as aliases parsed into `status`; serialization emits only `status`.
- Status display values are exactly: `Patrician`, `Nobilis`, `Novus`, `Eques Romanus`, `Senator`.
- The prerendered page count must stay 6,131 (build regression check).
- UI text/markup follows the ledger idiom; no new card/box chrome.

---

### Task 1: Filiation ancestor parser

**Files:**
- Create: `src/data/parse-filiation.ts`
- Test: `src/data/parse-filiation.test.ts`

**Interfaces:**
- Consumes: nothing (pure module; hardcoded abbreviation table).
- Produces: `parseFiliation(filiation: string | null): { father: string | null; grandfather: string | null }` — used by Task 2 inside `parsePersonTtl`.

Filiation grammar observed in the data: tokens before `f.` name the father's praenomen (abbreviated), tokens before `n.` the grandfather's. Real examples: `"M. f. M. n."`, `"L. f."`, `"- f. - n."` (unknown), `"L.? f. C. n."` (uncertainty stripped), `"Sex. f. (Sex. n.)"` (parens stripped), `"Q. f. Q. or L.? n."` (ambiguous → null), `"Ser. ? f. - n."` (detached `?`).

- [ ] **Step 1: Write the failing test**

```ts
// src/data/parse-filiation.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parseFiliation } from "./parse-filiation"

describe("parseFiliation", () => {
  test("expands father and grandfather praenomina", () => {
    expect(parseFiliation("M. f. M. n.")).toEqual({
      father: "Marcus",
      grandfather: "Marcus",
    })
    expect(parseFiliation("Q. f. Ser. n.")).toEqual({
      father: "Quintus",
      grandfather: "Servius",
    })
  })

  test("father only", () => {
    expect(parseFiliation("L. f.")).toEqual({
      father: "Lucius",
      grandfather: null,
    })
  })

  test("unknown slots yield null", () => {
    expect(parseFiliation("- f. - n.")).toEqual({
      father: null,
      grandfather: null,
    })
    expect(parseFiliation(null)).toEqual({ father: null, grandfather: null })
    expect(parseFiliation("")).toEqual({ father: null, grandfather: null })
  })

  test("strips uncertainty markers and parentheses", () => {
    expect(parseFiliation("L.? f. C. n.")).toEqual({
      father: "Lucius",
      grandfather: "Gaius",
    })
    expect(parseFiliation("Sex. f. (Sex. n.)")).toEqual({
      father: "Sextus",
      grandfather: "Sextus",
    })
    expect(parseFiliation("Ser. ? f. - n.")).toEqual({
      father: "Servius",
      grandfather: null,
    })
  })

  test("ambiguous 'or' slots yield null", () => {
    expect(parseFiliation("Q. f. Q. or L.? n.")).toEqual({
      father: "Quintus",
      grandfather: null,
    })
  })

  test("longer abbreviations", () => {
    expect(parseFiliation("Volus. f. Volus. n.")).toEqual({
      father: "Volusus",
      grandfather: "Volusus",
    })
    expect(parseFiliation("Mam. f.")).toEqual({
      father: "Mamercus",
      grandfather: null,
    })
    expect(parseFiliation("M'. f.")).toEqual({
      father: "Manius",
      grandfather: null,
    })
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `vp test src/data/parse-filiation.test.ts`
Expected: FAIL — cannot resolve `./parse-filiation`.

- [ ] **Step 3: Write the implementation**

```ts
// src/data/parse-filiation.ts

// Canonical praenomen abbreviations as they appear in DPRR filiation
// strings ("Q. f. Ser. n." = son of Quintus, grandson of Servius).
// The praenomina reference file carries no abbreviation property, so the
// standard epigraphic table is encoded here. Abbreviations observed in
// the export but with no unambiguous expansion (e.g. "S.", "Stat.",
// "Ann.", "V.") are deliberately absent — unknown slots resolve to null.
const PRAENOMEN_ABBREVIATIONS: Record<string, string> = {
  "A.": "Aulus",
  "Agripp.": "Agrippa",
  "Ap.": "Appius",
  "C.": "Gaius",
  "Cn.": "Gnaeus",
  "D.": "Decimus",
  "K.": "Caeso",
  "L.": "Lucius",
  "M.": "Marcus",
  "M'.": "Manius",
  "Mam.": "Mamercus",
  "Minat.": "Minatius",
  "N.": "Numerius",
  "Opet.": "Opiter",
  "Opit.": "Opiter",
  "P.": "Publius",
  "Post.": "Postumus",
  "Q.": "Quintus",
  "Ser.": "Servius",
  "Sex.": "Sextus",
  "Sp.": "Spurius",
  "T.": "Titus",
  "Ti.": "Tiberius",
  "Voler.": "Volero",
  "Volus.": "Volusus",
  "Vop.": "Vopiscus",
}

export interface FiliationAncestors {
  father: string | null
  grandfather: string | null
}

/**
 * Extract the father's and grandfather's praenomina from a filiation
 * string ("Q. f. Ser. n."). Unknown ("-"), ambiguous ("Q. or L.?"), or
 * unrecognized slots yield null — the search filter simply never matches
 * them.
 */
export function parseFiliation(filiation: string | null): FiliationAncestors {
  if (!filiation) return { father: null, grandfather: null }
  // Strip parentheses so "(Sex. n.)" tokenizes like "Sex. n."
  const tokens = filiation.replace(/[()]/g, "").trim().split(/\s+/)
  return {
    father: slotBefore(tokens, "f."),
    grandfather: slotBefore(tokens, "n."),
  }
}

function slotBefore(tokens: string[], marker: "f." | "n."): string | null {
  const idx = tokens.indexOf(marker)
  if (idx <= 0) return null
  // Walk back past detached uncertainty markers ("Ser. ? f.")
  let i = idx - 1
  while (i >= 0 && tokens[i].replace(/\?/g, "") === "") i--
  if (i < 0) return null
  // "Q. or L.?" — an ambiguous slot; refuse to guess
  if (i >= 1 && tokens[i - 1] === "or") return null
  const abbrev = tokens[i].replace(/\?/g, "")
  return PRAENOMEN_ABBREVIATIONS[abbrev] ?? null
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `vp test src/data/parse-filiation.test.ts`
Expected: PASS (6 tests).

- [ ] **Step 5: Check and commit**

```bash
vp check && git add src/data/parse-filiation.ts src/data/parse-filiation.test.ts && git commit -m "feat: filiation ancestor parser (father/grandfather praenomina)"
```

---

### Task 2: Parse statuses, novus, and ancestors onto Person

**Files:**
- Modify: `src/data/types.ts` (PersonSummary + Person interfaces)
- Modify: `src/data/parse-persons.ts`
- Test: `src/data/parse-persons.test.ts` (add cases; existing tests must keep passing)

**Interfaces:**
- Consumes: `parseFiliation` from Task 1.
- Produces, on `PersonSummary`: `statuses: string[]` (display values from the Global Constraints list), `father: string | null`, `grandfather: string | null`, `contextLine: string | null` (parser always sets `null`; Task 3 fills it). **Removes** `isPatrician`/`isNobilis` from `PersonSummary` and moves them to `Person`.
- Produces, on `Person`: `isPatrician: boolean`, `isNobilis: boolean`, `isNovus: boolean`, `statusAssertions: string[]` (raw reference names, e.g. `"eques Romanus"`, `"senator"`).

- [ ] **Step 1: Update the type definitions**

In `src/data/types.ts`, `PersonSummary`: delete the `isPatrician: boolean` and `isNobilis: boolean` lines and add:

```ts
  /** Display statuses for faceting: Patrician, Nobilis, Novus, Eques Romanus, Senator. */
  statuses: string[]
  /** Father's / grandfather's praenomen parsed from the filiation string. */
  father: string | null
  grandfather: string | null
  /** "father of Ap. Claudius (321), cos. 495" for career-less persons; null otherwise. */
  contextLine: string | null
```

In `Person` add:

```ts
  isPatrician: boolean
  isNobilis: boolean
  isNovus: boolean
  /** Raw StatusAssertion names ("eques Romanus", "senator"). */
  statusAssertions: string[]
```

- [ ] **Step 2: Add failing parser test cases**

Open `src/data/parse-persons.test.ts`, look at how existing tests build TTL fixture strings and call `parsePersonTtl(ttl, refs, concordanceMap)` — follow that exact fixture pattern (the test file already constructs a `ReferenceMaps` fixture; extend its `statuses` map with `["http://romanrepublic.ac.uk/rdf/entity/Status/1", { name: "eques Romanus", abbreviation: null }]`). Add:

```ts
test("parses StatusAssertions and isNovus into statuses", () => {
  const ttl = `
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertion/1> a dprr:StatusAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasStatus <http://romanrepublic.ac.uk/rdf/entity/Status/1> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" ;
  dprr:isPatrician "true" ;
  dprr:isNovus "true" .
`
  const [person] = parsePersonTtl(ttl, refs, new Map())
  expect(person.statusAssertions).toEqual(["eques Romanus"])
  expect(person.isNovus).toBe(true)
  expect(person.statuses).toEqual(["Patrician", "Novus", "Eques Romanus"])
})

test("parses father and grandfather from filiation", () => {
  const ttl = `
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" ;
  dprr:hasFiliation "Q. f. Ser. n." .
`
  const [person] = parsePersonTtl(ttl, refs, new Map())
  expect(person.father).toBe("Quintus")
  expect(person.grandfather).toBe("Servius")
  expect(person.contextLine).toBeNull()
})
```

Run: `vp test src/data/parse-persons.test.ts` — expected: FAIL (type errors / missing fields).

- [ ] **Step 3: Implement in parse-persons.ts**

Add to the type-constant block:

```ts
const STATUS_ASSERTION_TYPE = `${DPRR}StatusAssertion`
```

Add a `statusAssertionGroups` map beside the other auxiliary maps, and a `case STATUS_ASSERTION_TYPE:` in the type switch that fills it.

Add import: `import { parseFiliation } from "./parse-filiation"`.

Add a builder beside `buildTribes`:

```ts
  // Build raw status names for a person URI from StatusAssertion entities
  function buildStatusAssertions(personUri: string): string[] {
    const names: string[] = []
    for (const [, g] of statusAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const statusUri = first(g, "hasStatus")
      const name = statusUri ? refs.statuses.get(statusUri)?.name : null
      if (name && !names.includes(name)) names.push(name)
    }
    return names
  }
```

In the person-construction loop, before `persons.push`, compute:

```ts
    const isPatrician = first(g, "isPatrician") === "true"
    const isNobilis = first(g, "isNobilis") === "true"
    const isNovus = first(g, "isNovus") === "true"
    const statusAssertions = buildStatusAssertions(personUri)
    const statuses = [
      ...(isPatrician ? ["Patrician"] : []),
      ...(isNobilis ? ["Nobilis"] : []),
      ...(isNovus ? ["Novus"] : []),
      // "eques Romanus" → "Eques Romanus"
      ...statusAssertions.map((s) => s.charAt(0).toUpperCase() + s.slice(1)),
    ]
    const { father, grandfather } = parseFiliation(filiation || null)
```

and in the pushed object replace `isPatrician: first(g, "isPatrician") === "true"` / the `isNobilis` line with the computed `isPatrician, isNobilis,` and add `isNovus, statusAssertions, statuses, father, grandfather, contextLine: null,`.

- [ ] **Step 4: Fix the summary-shaped test helpers**

`src/lib/filter.test.ts` and `src/lib/search.test.ts` both have a `makeSummary` helper constructing a `PersonSummary` literal. In each: delete `isPatrician`/`isNobilis` entries and add `statuses: [], father: null, grandfather: null, contextLine: null`. Any test that set `isPatrician: true` should set `statuses: ["Patrician"]` instead. `src/data/loader.ts` `toSummaries` will now have type errors — fix minimally in this task by mapping the new fields verbatim (`statuses: p.statuses, father: p.father, grandfather: p.grandfather, contextLine: null`) and deleting the two flag lines; Task 3 revisits `toSummaries` for context lines. Also update `src/data/search-index.ts` `storeFields`: replace `"isPatrician", "isNobilis"` with `"statuses"`. Fix any other compile errors `vp check` reveals (e.g. `src/routes/persons.$id.tsx` uses `person.isPatrician` — still valid, it lives on `Person` now).

- [ ] **Step 5: Run the full suite**

Run: `vp test` and `vp check`
Expected: all green, including the two new parser tests.

- [ ] **Step 6: Commit**

```bash
git add -A src/ && git commit -m "feat: parse status assertions, novus flag, and filiation ancestors"
```

---

### Task 3: Relationship-context lines

**Files:**
- Create: `src/data/context-line.ts`
- Test: `src/data/context-line.test.ts`
- Modify: `src/data/loader.ts` (`toSummaries`)

**Interfaces:**
- Consumes: `Person` (Task 2 shape), `displayName(name: string): string` from `src/lib/order` (strips the "CLAU4781 " ID prefix from stored names).
- Produces: `buildContextLine(person: Person, byId: Map<string, Person>): string | null`; `toSummaries` gains real `contextLine` values.

- [ ] **Step 1: Write the failing test**

```ts
// src/data/context-line.test.ts
import { expect, test, describe } from "vite-plus/test"
import { buildContextLine } from "./context-line"
import type { Person, Relationship } from "./types"

function makePerson(over: Partial<Person>): Person {
  return {
    id: "TEST0001",
    uri: "urn:test:1",
    name: "TEST0001 T. Testius",
    praenomen: "Titus",
    nomen: "Testius",
    cognomen: null,
    otherNames: null,
    sex: "Male",
    isPatrician: false,
    isNobilis: false,
    isNovus: false,
    statusAssertions: [],
    statuses: [],
    father: null,
    grandfather: null,
    contextLine: null,
    highestOffice: null,
    eraFrom: null,
    eraTo: null,
    tribes: [],
    offices: [],
    provinces: [],
    reNumber: null,
    filiation: null,
    lifeEvents: [],
    nobilisNotes: null,
    postAssertions: [],
    relationships: [],
    dateInformation: [],
    personNotes: [],
    concordances: [],
    ...over,
  }
}

function makeRel(over: Partial<Relationship>): Relationship {
  return {
    id: "rel1",
    relationshipType: "father of",
    relatedPersonId: "TEST0002",
    relatedPersonName: "TEST0002 A. Testius",
    secondarySource: "",
    references: [],
    ...over,
  }
}

describe("buildContextLine", () => {
  test("names the most notable relative with their highest office", () => {
    const relative = makePerson({
      id: "TEST0002",
      name: "TEST0002 A. Testius",
      highestOffice: "cos. 495",
    })
    const person = makePerson({ relationships: [makeRel({})] })
    const byId = new Map([[relative.id, relative]])
    expect(buildContextLine(person, byId)).toBe(
      "father of A. Testius, cos. 495"
    )
  })

  test("null for persons with a career of their own", () => {
    const person = makePerson({
      relationships: [makeRel({})],
      postAssertions: [{} as never],
    })
    expect(buildContextLine(person, new Map())).toBeNull()
  })

  test("null when no relative has an office", () => {
    const relative = makePerson({ id: "TEST0002", highestOffice: null })
    const person = makePerson({ relationships: [makeRel({})] })
    expect(buildContextLine(person, new Map([[relative.id, relative]]))).
      toBeNull()
  })

  test("prefers the earlier-era relative when several qualify", () => {
    const early = makePerson({
      id: "TEST0002",
      name: "TEST0002 A. Testius",
      highestOffice: "cos. 495",
      eraFrom: -500,
    })
    const late = makePerson({
      id: "TEST0003",
      name: "TEST0003 B. Testius",
      highestOffice: "pr. 100",
      eraFrom: -120,
    })
    const person = makePerson({
      relationships: [
        makeRel({ relatedPersonId: "TEST0003", relationshipType: "son of" }),
        makeRel({ relatedPersonId: "TEST0002" }),
      ],
    })
    const byId = new Map([
      [early.id, early],
      [late.id, late],
    ])
    expect(buildContextLine(person, byId)).toBe(
      "father of A. Testius, cos. 495"
    )
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `vp test src/data/context-line.test.ts`
Expected: FAIL — cannot resolve `./context-line`.

- [ ] **Step 3: Write the implementation**

```ts
// site/src/data/context-line.ts
import { displayName } from "../lib/order"
import type { Person } from "./types"

/**
 * A one-line relationship anchor for persons with no career of their own,
 * mirroring the original DPRR results ("father of Ap. Claudius (321),
 * cos. 495"). Chooses the relative with a recorded highest office,
 * earliest era first (nulls last), so the anchor is the most historically
 * locatable member of the family.
 */
export function buildContextLine(
  person: Person,
  byId: Map<string, Person>
): string | null {
  if (person.postAssertions.length > 0) return null
  const candidates = person.relationships
    .map((rel) => ({ rel, related: byId.get(rel.relatedPersonId) }))
    .filter(
      (c): c is { rel: (typeof person.relationships)[number]; related: Person } =>
        c.related !== undefined && c.related.highestOffice !== null
    )
  if (candidates.length === 0) return null
  candidates.sort(
    (a, b) =>
      (a.related.eraFrom ?? Number.MAX_SAFE_INTEGER) -
      (b.related.eraFrom ?? Number.MAX_SAFE_INTEGER)
  )
  const { rel, related } = candidates[0]
  // Relationship names already carry the preposition ("father of",
  // "married to"), so the line is "<type> <name>, <office>".
  return `${rel.relationshipType} ${displayName(related.name)}, ${related.highestOffice}`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `vp test src/data/context-line.test.ts`
Expected: PASS (4 tests).

- [ ] **Step 5: Wire into toSummaries**

In `src/data/loader.ts`, change `toSummaries` to build an ID map first:

```ts
/** Extract compact summaries for search/faceting. */
export function toSummaries(persons: Person[]): PersonSummary[] {
  const byId = new Map(persons.map((p) => [p.id, p]))
  return persons.map((p) => ({
    // ...existing field mapping stays as-is, except:
    contextLine: buildContextLine(p, byId),
  }))
}
```

(Add `import { buildContextLine } from "./context-line"`.) Note `toSummaries` is also called by `buildTribeDetail`/`buildGensDetail` in aggregate-references with filtered subsets — the map built from the subset means context lines may be null there; that is acceptable (members' context lines aren't rendered on those pages).

- [ ] **Step 6: Full suite, check, commit**

Run: `vp test && vp check`
Expected: green.

```bash
git add src/data/context-line.ts src/data/context-line.test.ts src/data/loader.ts && git commit -m "feat: relationship-context lines for career-less persons"
```

---

### Task 4: Search state and URL params

**Files:**
- Modify: `src/data/types.ts` (`SearchState`)
- Modify: `src/lib/search-params.ts`
- Test: `src/lib/search.test.ts` (round-trip + alias cases)

**Interfaces:**
- Consumes: nothing new.
- Produces: `SearchState` gains `status: string[]`, `father: string[]`, `grandfather: string[]`; **loses** `patrician`/`nobilis`. `parseSearchParams` accepts legacy `patrician=true`/`nobilis=true` as aliases appending `"Patrician"`/`"Nobilis"` to `status`. `toSearchParams` emits `status`, `father`, `grandfather` facet params only.

- [ ] **Step 1: Add failing tests**

In `src/lib/search.test.ts`:

```ts
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
```

Run: `vp test src/lib/search.test.ts` — expected: FAIL (fields missing).

- [ ] **Step 2: Update SearchState in types.ts**

Delete `patrician: boolean | null` and `nobilis: boolean | null`; add:

```ts
  /** AND-semantics status facet (Patrician, Nobilis, Novus, Eques Romanus, Senator). */
  status: string[]
  father: string[]
  grandfather: string[]
```

- [ ] **Step 3: Update search-params.ts**

In `parseSearchParams`, replace the `patrician:`/`nobilis:` ternaries with:

```ts
    status: [
      ...splitFacetParam(params.status),
      // Pre-status-facet URLs used boolean patrician/nobilis params;
      // keep shipped links working by folding them in.
      ...(params.patrician === "true" ? ["Patrician"] : []),
      ...(params.nobilis === "true" ? ["Nobilis"] : []),
    ],
    father: splitFacetParam(params.father),
    grandfather: splitFacetParam(params.grandfather),
```

In `toSearchParams`, replace the two `state.patrician`/`state.nobilis` lines with:

```ts
  if (state.status.length) params.status = joinFacetParam(state.status)
  if (state.father.length) params.father = joinFacetParam(state.father)
  if (state.grandfather.length)
    params.grandfather = joinFacetParam(state.grandfather)
```

- [ ] **Step 4: Fix compile fallout in this task only where mechanical**

`vp check` will flag `filter.ts`, `search.ts` (countWith on removed keys is not present yet — fine), `facet-sidebar.tsx`, `active-filter-chips.tsx`. Fix **filter.ts minimally** so the build compiles: replace the two `matchesFlag(...)` calls with `state.status.every((s) => person.statuses.includes(s))` inline for now (Task 5 shapes this properly), delete `matchesFlag`. In `facet-sidebar.tsx`, change the two Status checkboxes to drive the array (`checked={state.status.includes("Patrician")}`, `onUpdate({ status: [...without or with value...] })`) — this component is deleted in Task 7, so keep it merely compiling:

```tsx
                <Checkbox
                  checked={state.status.includes("Patrician")}
                  onCheckedChange={(checked) =>
                    onUpdate({
                      status: checked
                        ? [...state.status, "Patrician"]
                        : state.status.filter((s) => s !== "Patrician"),
                    })
                  }
                />
```

(and the same for "Nobilis"; update the `tier2Active` expression to use `state.status.length > 0`). In `active-filter-chips.tsx`, replace the two `if (state.patrician !== null)` / nobilis blocks with:

```ts
  for (const status of state.status) {
    chips.push({
      label: status,
      onRemove: () =>
        onRemove({ status: state.status.filter((s) => s !== status) }),
    })
  }
  for (const father of state.father) {
    chips.push({
      label: `Father: ${father}`,
      onRemove: () =>
        onRemove({ father: state.father.filter((f) => f !== father) }),
    })
  }
  for (const grandfather of state.grandfather) {
    chips.push({
      label: `Grandfather: ${grandfather}`,
      onRemove: () =>
        onRemove({
          grandfather: state.grandfather.filter((g) => g !== grandfather),
        }),
    })
  }
```

Update `makeState`-style helpers or literal `SearchState` objects in `filter.test.ts`/`search.test.ts` to the new shape (`status: [], father: [], grandfather: []`).

- [ ] **Step 5: Run the suite and commit**

Run: `vp test && vp check` — expected green.

```bash
git add -A src/ && git commit -m "feat: status/father/grandfather search params with legacy aliases"
```

---

### Task 5: Filtering and facet counts

**Files:**
- Modify: `src/lib/filter.ts`
- Modify: `src/lib/search.ts` (`useSearchState` facets)
- Test: `src/lib/filter.test.ts`

**Interfaces:**
- Consumes: `SearchState` (Task 4), `PersonSummary` (Task 2).
- Produces: `matchesFacets` honoring `status` (AND), `father`, `grandfather` (OR-membership like praenomen); `useSearchState().facets` gains `status`, `father`, `grandfather` `FacetValue[]` lists (disjunctive counts, own facet relaxed).

- [ ] **Step 1: Add failing filter tests**

In `src/lib/filter.test.ts` (using its existing `makeSummary`/state helpers):

```ts
test("status facet requires every selected status", () => {
  const patricianSenator = makeSummary({
    statuses: ["Patrician", "Senator"],
  })
  const plainSenator = makeSummary({ id: "TEST0002", statuses: ["Senator"] })
  const state = makeState({ status: ["Patrician", "Senator"] })
  expect(matchesFacets(patricianSenator, state, ctx)).toBe(true)
  expect(matchesFacets(plainSenator, state, ctx)).toBe(false)
})

test("father and grandfather facets match parsed ancestors", () => {
  const person = makeSummary({ father: "Quintus", grandfather: "Servius" })
  expect(
    matchesFacets(person, makeState({ father: ["Quintus"] }), ctx)
  ).toBe(true)
  expect(
    matchesFacets(person, makeState({ father: ["Lucius"] }), ctx)
  ).toBe(false)
  expect(
    matchesFacets(
      makeSummary({ father: null }),
      makeState({ father: ["Quintus"] }),
      ctx
    )
  ).toBe(false)
  expect(
    matchesFacets(person, makeState({ grandfather: ["Servius"] }), ctx)
  ).toBe(true)
})
```

Run: `vp test src/lib/filter.test.ts` — expected: FAIL.

- [ ] **Step 2: Implement in filter.ts**

Add beside the other predicates (and replace the Task-4 inline expression):

```ts
/** Statuses are attributes, not alternatives: every selected one must hold. */
function matchesAllStatuses(selected: string[], statuses: string[]): boolean {
  return selected.every((s) => statuses.includes(s))
}
```

In the `matchesFacets` conjunction use:

```ts
    matchesAllStatuses(state.status, person.statuses) &&
    matchesCognomen(state.father, person.father) &&
    matchesCognomen(state.grandfather, person.grandfather) &&
```

(`matchesCognomen` already implements "empty selection matches everyone; a null value never matches" — exactly the ancestor semantics. If the overload reads poorly, rename it `matchesNullableSelection` and update the cognomen call site too.)

- [ ] **Step 3: Add facet counts in search.ts**

In `useSearchState`'s `facets` memo add to the returned object:

```ts
      status: countWith("status", "statuses"),
      father: countWith("father", "father"),
      grandfather: countWith("grandfather", "grandfather"),
```

(`computeFacetValues` already handles both `string[]` fields and nullable `string` fields.)

- [ ] **Step 4: Run the suite and commit**

Run: `vp test && vp check` — expected green.

```bash
git add src/lib/filter.ts src/lib/filter.test.ts src/lib/search.ts && git commit -m "feat: filter and count status/father/grandfather facets"
```

---

### Task 6: Popover primitive (responsive FilterPopover)

**Files:**
- Create: `src/components/ui/popover.tsx`
- Create: `src/components/filter-popover.tsx`

**Interfaces:**
- Consumes: `Popover` and `Dialog` from the `radix-ui` package (already a dependency; import as `import { Popover as PopoverPrimitive, Dialog as DialogPrimitive } from "radix-ui"`).
- Produces: `<FilterPopover label activeCount open onOpenChange>{children}</FilterPopover>` — trigger pill + one open surface at a time, popover ≥ md, bottom sheet < md. Task 7 consumes it.

There are no component tests in this repo (pure logic only) — verification for UI tasks is `vp check` + the Task 9 build/dev checks.

- [ ] **Step 1: Create the radix wrapper**

```tsx
// site/src/components/ui/popover.tsx
import { Popover as PopoverPrimitive } from "radix-ui"
import { cn } from "@/lib/utils"

export const Popover = PopoverPrimitive.Root
export const PopoverTrigger = PopoverPrimitive.Trigger

export function PopoverContent({
  className,
  align = "start",
  sideOffset = 6,
  ...props
}: React.ComponentProps<typeof PopoverPrimitive.Content>) {
  return (
    <PopoverPrimitive.Portal>
      <PopoverPrimitive.Content
        align={align}
        sideOffset={sideOffset}
        className={cn(
          "bg-background z-50 w-72 border border-border p-3 shadow-md outline-none",
          className
        )}
        {...props}
      />
    </PopoverPrimitive.Portal>
  )
}
```

(If `@/lib/utils` doesn't export `cn`, check where existing ui components like `src/components/ui/input.tsx` import it from and match that.)

- [ ] **Step 2: Create FilterPopover**

```tsx
// site/src/components/filter-popover.tsx
import { useEffect, useState } from "react"
import { Dialog as DialogPrimitive } from "radix-ui"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"

function useIsDesktop(): boolean {
  const [isDesktop, setIsDesktop] = useState(true)
  useEffect(() => {
    const mq = window.matchMedia("(min-width: 768px)")
    setIsDesktop(mq.matches)
    const onChange = (e: MediaQueryListEvent) => setIsDesktop(e.matches)
    mq.addEventListener("change", onChange)
    return () => mq.removeEventListener("change", onChange)
  }, [])
  return isDesktop
}

interface FilterPopoverProps {
  label: string
  activeCount: number
  open: boolean
  onOpenChange: (open: boolean) => void
  children: React.ReactNode
}

/**
 * One facet group behind a pill trigger: Radix Popover on desktop, a
 * bottom-sheet Dialog under the md breakpoint. The parent owns open
 * state so only one group is open at a time.
 */
export function FilterPopover({
  label,
  activeCount,
  open,
  onOpenChange,
  children,
}: FilterPopoverProps) {
  const isDesktop = useIsDesktop()

  const trigger = (
    <button
      type="button"
      className={
        activeCount > 0
          ? "micro-label rule-hair pb-0.5"
          : "micro-label-muted rule-hair pb-0.5 hover:text-foreground"
      }
    >
      {label}
      {activeCount > 0 && ` (${activeCount})`}
      <span aria-hidden="true"> ▾</span>
    </button>
  )

  if (isDesktop) {
    return (
      <Popover open={open} onOpenChange={onOpenChange}>
        <PopoverTrigger asChild>{trigger}</PopoverTrigger>
        <PopoverContent className="max-h-[70vh] overflow-y-auto">
          {children}
        </PopoverContent>
      </Popover>
    )
  }

  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Trigger asChild>{trigger}</DialogPrimitive.Trigger>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-black/30" />
        <DialogPrimitive.Content className="bg-background fixed inset-x-0 bottom-0 z-50 max-h-[75vh] overflow-y-auto border-t border-border p-4">
          <DialogPrimitive.Title className="micro-label pb-2">
            {label}
          </DialogPrimitive.Title>
          {children}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  )
}
```

- [ ] **Step 3: Check and commit**

Run: `vp check` — expected green (unused-export warnings are acceptable only if the linter allows; if it flags, Task 7 consumes them — you may combine the commit with a `// oxlint-disable` only if genuinely blocked, otherwise proceed to Task 7 before committing lint-clean).

```bash
git add src/components/ui/popover.tsx src/components/filter-popover.tsx && git commit -m "feat: responsive FilterPopover primitive"
```

---

### Task 7: FilterBand and layout swap

**Files:**
- Create: `src/components/filter-band.tsx`
- Modify: `src/routes/index.tsx`
- Delete: `src/components/facet-sidebar.tsx`, `src/components/advanced-search.tsx`

**Interfaces:**
- Consumes: `FilterPopover` (Task 6), `FacetGroup`, `FacetHierarchyGroup`, `FacetCombobox` (existing — props: `FacetGroup {title, items, selected, onChange, defaultOpen?, searchable?}`; `FacetHierarchyGroup {title, items, parentOf, selected, onChange, defaultOpen?, hideCounts?}`; `FacetCombobox {label, values, selected, onChange}`), `useSearchState` facets incl. Task 5 additions, `Checkbox`, `Input`.
- Produces: `<FilterBand facets officeHierarchy provinceHierarchy state onUpdate initialFocus?>` — same prop surface as the old `FacetSidebar` plus the extended facets object `{office, nomen, sex, tribe, province, event, praenomen, cognomen, status, father, grandfather}`.

- [ ] **Step 1: Create FilterBand**

```tsx
// site/src/components/filter-band.tsx
import { useState } from "react"
import { FilterPopover } from "./filter-popover"
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { FacetCombobox } from "./facet-combobox"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import type { SearchState, FacetValue } from "@/data/types"

type BandKey =
  | "office"
  | "name"
  | "status"
  | "tribe"
  | "location"
  | "events"

interface FilterBandProps {
  facets: {
    office: FacetValue[]
    nomen: FacetValue[]
    sex: FacetValue[]
    tribe: FacetValue[]
    province: FacetValue[]
    event: FacetValue[]
    praenomen: FacetValue[]
    cognomen: FacetValue[]
    status: FacetValue[]
    father: FacetValue[]
    grandfather: FacetValue[]
  }
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
  /** Popover to open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "gens"
}

export function FilterBand({
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
  initialFocus,
}: FilterBandProps) {
  // One open popover at a time; the band owns which.
  const [openKey, setOpenKey] = useState<BandKey | null>(() =>
    initialFocus === "office" ? "office" : initialFocus === "gens" ? "name" : null
  )
  const openFor = (key: BandKey) => ({
    open: openKey === key,
    onOpenChange: (open: boolean) => setOpenKey(open ? key : null),
  })

  const nameCount =
    state.praenomen.length +
    state.nomen.length +
    state.cognomen.length +
    state.father.length +
    state.grandfather.length +
    (state.re ? 1 : 0)
  const officeCount =
    state.office.length +
    (state.officeMode === "all" ? 1 : 0) +
    (state.officeInRange ? 1 : 0)
  const statusCount = state.status.length + state.sex.length

  return (
    <div className="rule-lead flex flex-wrap items-baseline gap-x-5 gap-y-1 pt-2 pb-1">
      <FilterPopover
        label="Office"
        activeCount={officeCount}
        {...openFor("office")}
      >
        <FacetHierarchyGroup
          title="Office"
          items={facets.office}
          parentOf={officeHierarchy}
          selected={state.office}
          onChange={(office) => onUpdate({ office })}
          defaultOpen={true}
          hideCounts={state.officeMode === "all" || state.officeInRange}
        />
        <div className="mt-3 space-y-2">
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <Checkbox
              checked={state.officeMode === "all"}
              onCheckedChange={(c) =>
                onUpdate({ officeMode: c ? "all" : "any" })
              }
            />
            <span>Require every selected office (AND)</span>
          </label>
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <Checkbox
              checked={state.officeInRange}
              onCheckedChange={(c) =>
                onUpdate({ officeInRange: c === true })
              }
            />
            <span>Apply time period to offices (held in range)</span>
          </label>
        </div>
      </FilterPopover>

      <FilterPopover label="Name" activeCount={nameCount} {...openFor("name")}>
        <div className="space-y-3">
          <FacetCombobox
            label="Praenomen"
            values={facets.praenomen}
            selected={state.praenomen}
            onChange={(praenomen) => onUpdate({ praenomen })}
          />
          <FacetCombobox
            label="Gens (nomen)"
            values={facets.nomen}
            selected={state.nomen}
            onChange={(nomen) => onUpdate({ nomen })}
          />
          <FacetCombobox
            label="Cognomen"
            values={facets.cognomen}
            selected={state.cognomen}
            onChange={(cognomen) => onUpdate({ cognomen })}
          />
          <FacetCombobox
            label="Father (praenomen)"
            values={facets.father}
            selected={state.father}
            onChange={(father) => onUpdate({ father })}
          />
          <FacetCombobox
            label="Grandfather (praenomen)"
            values={facets.grandfather}
            selected={state.grandfather}
            onChange={(grandfather) => onUpdate({ grandfather })}
          />
          <div className="space-y-1">
            <p className="micro-label-muted">RE number</p>
            <Input
              value={state.re}
              onChange={(e) => onUpdate({ re: e.target.value })}
              placeholder="e.g. 46a"
              className="h-7 text-xs"
            />
          </div>
        </div>
      </FilterPopover>

      <FilterPopover
        label="Status"
        activeCount={statusCount}
        {...openFor("status")}
      >
        <FacetGroup
          title="Status"
          items={facets.status}
          selected={state.status}
          onChange={(status) => onUpdate({ status })}
          defaultOpen={true}
        />
        <FacetGroup
          title="Sex"
          items={facets.sex}
          selected={state.sex}
          onChange={(sex) => onUpdate({ sex })}
          defaultOpen={true}
        />
      </FilterPopover>

      <FilterPopover
        label="Tribe"
        activeCount={state.tribe.length}
        {...openFor("tribe")}
      >
        <FacetGroup
          title="Tribe"
          items={facets.tribe}
          selected={state.tribe}
          onChange={(tribe) => onUpdate({ tribe })}
          defaultOpen={true}
          searchable
        />
      </FilterPopover>

      <FilterPopover
        label="Location"
        activeCount={state.province.length}
        {...openFor("location")}
      >
        <FacetHierarchyGroup
          title="Location"
          items={facets.province}
          parentOf={provinceHierarchy}
          selected={state.province}
          onChange={(province) => onUpdate({ province })}
          defaultOpen={true}
        />
      </FilterPopover>

      <FilterPopover
        label="Events"
        activeCount={state.event.length}
        {...openFor("events")}
      >
        <FacetGroup
          title="Life events"
          items={facets.event}
          selected={state.event}
          onChange={(event) => onUpdate({ event })}
          defaultOpen={true}
        />
      </FilterPopover>
    </div>
  )
}
```

- [ ] **Step 2: Swap the layout in index.tsx**

In `src/routes/index.tsx`: replace the `FacetSidebar` import with `import { FilterBand } from "@/components/filter-band"`. Replace the sidebar layout block

```tsx
      <div className="mt-4 flex gap-6">
        <FacetSidebar ... />
        <main className="min-w-0 flex-1">
          <ResultsList results={results} />
        </main>
      </div>
```

with

```tsx
      <div className="mt-3">
        <FilterBand
          facets={facets}
          state={state}
          onUpdate={updateState}
          officeHierarchy={bundle.payload.officeHierarchy}
          provinceHierarchy={bundle.payload.provinceHierarchy}
          initialFocus={
            initialFocus === "office" || initialFocus === "gens"
              ? initialFocus
              : undefined
          }
        />
      </div>
      <main className="mt-2 min-w-0">
        <ResultsList results={results} />
      </main>
```

The `Focus` type in index.tsx includes `"time"` (landing card that focuses the timeline) — the timeline is always visible above the band, so `"time"` needs no popover; the conditional above drops it.

- [ ] **Step 3: Delete the superseded components**

```bash
git rm src/components/facet-sidebar.tsx src/components/advanced-search.tsx
```

Fix any dangling imports `vp check` finds (only index.tsx should reference them).

- [ ] **Step 4: Verify against the dev server**

Run: `vp test && vp check`, then `vp dev --port 3000` and load `http://localhost:3000/dprr-data/`:
- band shows Office · Name · Status · Tribe · Location · Events; one popover open at a time
- Status popover lists Patrician / Nobilis / Novus / Eques Romanus / Senator with counts and Sex below
- Name popover has Father/Grandfather comboboxes that filter results
- selecting facets updates chips and URL; narrow the window below 768px and confirm popovers become bottom sheets

- [ ] **Step 5: Commit**

```bash
git add -A src/ && git commit -m "feat: filter band with popovers replaces facet sidebar"
```

---

### Task 8: Person-page status markers

**Files:**
- Modify: `src/routes/persons.$id.tsx` (the header marker row around lines 110-120)

**Interfaces:**
- Consumes: `person.statuses` (Task 2; `Person` extends `PersonSummary`, so the combined display list is available on the full person record).

- [ ] **Step 1: Replace the two hardcoded markers**

The header currently renders:

```tsx
          {person.isPatrician && (
            <span className="small-caps ml-2 text-muted-foreground">
              Patrician
            </span>
          )}
          {person.isNobilis && (
            <span className="small-caps ml-1 text-muted-foreground">
              Nobilis
            </span>
          )}
```

Replace both blocks with one loop over the combined list:

```tsx
          {person.statuses.map((status, i) => (
            <span
              key={status}
              className={`small-caps ${i === 0 ? "ml-2" : "ml-1"} text-muted-foreground`}
            >
              {status}
            </span>
          ))}
```

Also update the meta-description builder at ~line 59 (`[person.highestOffice, person.isPatrician ? "Patrician" : null]`) to use `person.statuses[0] ?? null` in place of the patrician ternary — or, more faithfully, spread `...person.statuses` into the array being joined; match the existing join semantics when you read the line.

- [ ] **Step 2: Verify, check, commit**

Run: `vp check && vp test`, then in the dev server open `/dprr-data/persons/CORN0985` (has an eques Romanus StatusAssertion) and confirm the marker row shows the new statuses.

```bash
git add "src/routes/persons.\$id.tsx" && git commit -m "feat: render all status markers on person page header"
```

---

### Task 9: Context lines in result rows + final verification

**Files:**
- Modify: `src/components/fasti-row.tsx`

**Interfaces:**
- Consumes: `person.contextLine` (Task 3).

- [ ] **Step 1: Render the context line**

In `src/components/fasti-row.tsx`, the secondary `<p>` starts with the filiation span. Insert the context line before it, only when present (career-less persons — those never have `highestOffice`, so the primary line has room):

```tsx
      <p className="text-xs leading-snug text-muted-foreground">
        {person.contextLine && <span>{person.contextLine} · </span>}
        {person.filiation && <span>{person.filiation} · </span>}
```

- [ ] **Step 2: Full verification**

Run: `vp test && vp check` — all green.
Run: `vp build` — expected: `[prerender] Prerendered 6131 pages` (the page count is the regression check), sitemap normalization output present.
Dev-server spot checks:
- `/dprr-data/?status=Eques%20Romanus` filters and shows the chip
- `/dprr-data/?patrician=true` (legacy alias) still filters, chip reads "Patrician", and interacting with any filter rewrites the URL to `status=Patrician`
- a career-less person (search "Inregillensis" relatives — e.g. CLAU4781 M. Claudius) shows "father of …, cos. …" in the row
- landing page renders without JS-fetched data (view-source shows content)

- [ ] **Step 3: Commit**

```bash
git add src/components/fasti-row.tsx && git commit -m "feat: relationship-context lines in fasti result rows"
```
