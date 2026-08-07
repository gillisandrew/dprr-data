# UX Pass (Plan 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the search experience around progressive disclosure (two-state landing, tiered sidebar, advanced office queries, attestation-density timeline), put the person page on a career-main/identity-rail layout, move the search data out of the page HTML into lazy static JSON, and give every list a stated deterministic order.

**Architecture:** All new query logic lands as pure, unit-tested modules under `site/src/lib/` (params, filter, order, histogram, dates). Search data is assembled by a pure builder in `site/src/data/search-payload.ts` and written to `site/public/data/*.json` by a small Vite plugin at `buildStart`, so dev and build serve identical static assets; the search page fetches them lazily and the landing state renders from HTML alone. UI restructures (sidebar tiers, timeline, landing, fasti rows, person rail) are separate components consuming those modules.

**Tech Stack:** TanStack Start/Router, Vite+ (`vp`), MiniSearch, Tailwind 4 + shadcn/ui, tests via `vp test` (vite-plus/Vitest). **No new runtime dependencies** — the timeline is hand-rolled SVG, comboboxes are built from existing Input/Badge primitives.

## Global Constraints

- All site commands run from `site/` via `vp` (`vp install`, `vp check`, `vp test`, `vp build`). NEVER pnpm/npm/npx/vitest directly.
- Test imports from `vite-plus/test`: `import { expect, test, describe } from "vite-plus/test"`.
- Style: no semicolons, double quotes, 2-space indent. `vp check` must be fully green before every commit (the tsconfig baseUrl error was fixed in Plan 1 — there is no "known acceptable" error anymore).
- Do NOT merge to `main`. Work on `feature/static-site`.
- Base path `/dprr-data/`; client fetches use `import.meta.env.BASE_URL` prefixes. `SITE_URL` stays `https://gillisandrew.github.io/dprr-data`.
- **Year-zero policy: 0 = 1 BC.** Data convention: negative = BC (−509 = 509 BC), positive = AD, 0 is a degenerate value treated as 1 BC for display and clamped to −1 for binning/conversion. User-facing year inputs are unsigned + BC/AD selector (default BC); URLs keep signed integers.
- Sort labels are "Earliest first" / "Latest first" — never "Date ↑/↓".
- The province facet/pages are relabeled **"Location(s)"** in all UI text; URLs stay `/provinces/…`.
- URL multi-value facets stay comma-separated with per-value `encodeURIComponent` (the `joinFacetParam`/`splitFacetParam` helpers in `site/src/lib/search.ts`).
- Existing scholarly conventions must survive every restructure: uncertainty italic+"?", chronological careers, `name.replace(/^[A-Z]{4}\d+ /, "")` display names.

---

### Task 1: Summary enrichment (reNumber, filiation, lifeEvents)

**Files:**
- Modify: `site/src/data/types.ts` (`PersonSummary`)
- Modify: `site/src/data/parse-persons.ts`
- Modify: `site/src/data/loader.ts` (`toSummaries`)
- Test: `site/src/data/parse-persons.test.ts`
- Modify: `site/src/data/aggregate-references.test.ts`, `site/src/data/search-index.test.ts` (fixtures gain the new required fields)

**Interfaces:**
- Produces on `PersonSummary`: `reNumber: string | null`, `filiation: string | null`, `lifeEvents: string[]` (distinct DateInformation type names, excluding `"attested"`, in first-seen order). `Person` already carries `reNumber`/`filiation`; only the summary projection and lifeEvents derivation are new.

- [ ] **Step 1: Write the failing test**

Add to `site/src/data/parse-persons.test.ts` (reuse the file's existing `emptyRefs()`/`makeRefs()` helper; extend it so `refs.dateTypes` can be seeded):

```ts
describe("life events", () => {
  test("derives distinct life events from date information, excluding 'attested'", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/DateInformation/1> a dprr:DateInformation ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateType <http://romanrepublic.ac.uk/rdf/entity/DateType/4> ;
  dprr:hasValue "-42"^^xsd:integer .
<http://romanrepublic.ac.uk/rdf/entity/DateInformation/2> a dprr:DateInformation ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateType <http://romanrepublic.ac.uk/rdf/entity/DateType/4> ;
  dprr:hasValue "-42"^^xsd:integer .
<http://romanrepublic.ac.uk/rdf/entity/DateInformation/3> a dprr:DateInformation ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateType <http://romanrepublic.ac.uk/rdf/entity/DateType/1> ;
  dprr:hasValue "-60"^^xsd:integer .
`
    const refs = emptyRefs()
    refs.dateTypes.set(
      "http://romanrepublic.ac.uk/rdf/entity/DateType/4",
      "death - violent"
    )
    refs.dateTypes.set(
      "http://romanrepublic.ac.uk/rdf/entity/DateType/1",
      "attested"
    )
    const persons = parsePersonTtl(ttl, refs, new Map())
    expect(persons[0].lifeEvents).toEqual(["death - violent"])
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/data/parse-persons.test.ts`
Expected: FAIL — `lifeEvents` does not exist.

- [ ] **Step 3: Implement**

`site/src/data/types.ts` — add to `PersonSummary`:

```ts
  reNumber: string | null
  filiation: string | null
  /** Distinct DateInformation type names (e.g. "death - violent"), excluding "attested". */
  lifeEvents: string[]
```

(`Person` already declares `reNumber`/`filiation` — they move UP into `PersonSummary`; delete the duplicate declarations from `Person`.)

`site/src/data/parse-persons.ts` — in the person-building loop, after `dateInformation` is built:

```ts
    const lifeEvents = [
      ...new Set(
        dateInformation
          .map((d) => d.type)
          .filter((t) => t && t !== "attested")
      ),
    ]
```

(compute `const dateInformation = buildDateInfo(personUri)` once into a local instead of calling it inline in the object literal) and add `lifeEvents,` to the pushed person.

`site/src/data/loader.ts` — `toSummaries` adds:

```ts
    reNumber: p.reNumber,
    filiation: p.filiation,
    lifeEvents: p.lifeEvents,
```

Fixture updates: every `PersonSummary`/`Person` literal in `aggregate-references.test.ts` and `search-index.test.ts` gains `reNumber: null, filiation: null, lifeEvents: []` (or test-relevant values).

- [ ] **Step 4: Run tests and check**

Run: `cd site && vp test src && vp check`
Expected: all PASS, check green.

- [ ] **Step 5: Commit**

```bash
git add site/src/data
git commit -m "feat: add reNumber, filiation, and lifeEvents to person summaries"
```

---

### Task 2: Histogram module (attestation binning)

**Files:**
- Create: `site/src/lib/histogram.ts`
- Test: `site/src/lib/histogram.test.ts`

**Interfaces:**
- Produces:

```ts
export interface Histogram {
  /** First bin's starting year (signed, multiple of binSize). */
  start: number
  binSize: number
  counts: number[]
}
/** Bin dated ranges by representative year (start ?? end). Undated dropped.
    Year 0 clamps to -1. Years after -31 fold into the final bin. */
export function buildHistogram(
  ranges: ReadonlyArray<readonly [number | null, number | null]>,
  binSize?: number
): Histogram
/** Year → bin index for an existing histogram (same clamp rules). */
export function binIndexFor(year: number, h: Histogram): number
```

- [ ] **Step 1: Write the failing test**

`site/src/lib/histogram.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import { buildHistogram, binIndexFor } from "./histogram"

describe("buildHistogram", () => {
  test("bins representative years into 5-year bins", () => {
    const h = buildHistogram([
      [-509, -509],
      [-508, null],
      [null, -507],
      [-31, -31],
    ])
    expect(h.binSize).toBe(5)
    expect(h.start).toBe(-510)
    // bins: [-510..-506), [-505..-501), ... final bin contains -31
    expect(h.counts[0]).toBe(3)
    expect(h.counts[h.counts.length - 1]).toBe(1)
    expect(h.counts.reduce((a, b) => a + b, 0)).toBe(4)
  })

  test("drops undated, clamps year 0 to 1 BC, folds AD into the final bin", () => {
    const h = buildHistogram([
      [null, null],
      [-100, -99],
      [0, null],
      [14, null],
    ])
    expect(h.counts.reduce((a, b) => a + b, 0)).toBe(3)
    // 0 → -1 and 14 (AD) both land in the final bin
    expect(h.counts[h.counts.length - 1]).toBe(2)
  })

  test("binIndexFor matches the builder's placement", () => {
    const h = buildHistogram([[-509, null], [-31, null]])
    expect(binIndexFor(-509, h)).toBe(0)
    expect(binIndexFor(-31, h)).toBe(h.counts.length - 1)
    expect(binIndexFor(100, h)).toBe(h.counts.length - 1)
    expect(binIndexFor(-9999, h)).toBe(0)
  })

  test("empty input yields a single empty bin", () => {
    const h = buildHistogram([])
    expect(h.counts).toEqual([0])
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/lib/histogram.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

`site/src/lib/histogram.ts`:

```ts
// site/src/lib/histogram.ts

export interface Histogram {
  start: number
  binSize: number
  counts: number[]
}

const DEFAULT_BIN_SIZE = 5
/** Last year covered by its own bin; later years fold into the final bin. */
const AXIS_END = -31

function representativeYear(
  range: readonly [number | null, number | null]
): number | null {
  const year = range[0] ?? range[1]
  if (year === null) return null
  return year === 0 ? -1 : year
}

export function buildHistogram(
  ranges: ReadonlyArray<readonly [number | null, number | null]>,
  binSize: number = DEFAULT_BIN_SIZE
): Histogram {
  const years = ranges
    .map(representativeYear)
    .filter((y): y is number => y !== null)

  if (years.length === 0) {
    return { start: AXIS_END - binSize + 1, binSize, counts: [0] }
  }

  const minYear = Math.min(...years, AXIS_END)
  const start = Math.floor(minYear / binSize) * binSize
  const binCount = Math.max(1, Math.ceil((AXIS_END + 1 - start) / binSize))
  const counts = new Array<number>(binCount).fill(0)
  const h = { start, binSize, counts }
  for (const y of years) {
    counts[binIndexFor(y, h)] += 1
  }
  return h
}

export function binIndexFor(year: number, h: Histogram): number {
  const y = year === 0 ? -1 : year
  const idx = Math.floor((y - h.start) / h.binSize)
  return Math.min(Math.max(idx, 0), h.counts.length - 1)
}
```

- [ ] **Step 4: Run tests and check**

Run: `cd site && vp test src/lib/histogram.test.ts && vp check`
Expected: PASS, green.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/histogram.ts site/src/lib/histogram.test.ts
git commit -m "feat: add attestation histogram binning module"
```

---

### Task 3: Search payload builder

**Files:**
- Create: `site/src/data/search-payload.ts`
- Test: `site/src/data/search-payload.test.ts`

**Interfaces:**
- Consumes: `toSummaries` (loader), `buildSearchIndex`/`MINISEARCH_OPTIONS` (search-index), `buildNameHierarchy` (aggregate-references), `buildHistogram` (Task 2).
- Produces:

```ts
export interface SearchPayload {
  summaries: PersonSummary[]
  /** Office name table; career tuples index into it. */
  officeNames: string[]
  /** personId → [officeNameIndex, dateStart, dateEnd][] (dated + undated assertions). */
  careers: Record<string, [number, number | null, number | null][]>
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  histogram: Histogram
}
export interface SerializableOptions {
  fields: string[]
  storeFields: string[]
  idField: string
  searchOptions: { prefix: boolean; fuzzy: number; boost: Record<string, number> }
}
export interface SearchIndexPayload {
  index: object
  options: SerializableOptions
}
export function buildSearchPayload(persons: Person[], refs: ReferenceMaps): SearchPayload
export function buildSearchIndexPayload(summaries: PersonSummary[]): SearchIndexPayload
```

- [ ] **Step 1: Write the failing test**

`site/src/data/search-payload.test.ts` (reuse the `makePerson`/`makeAssertion` fixture style from `aggregate-references.test.ts` — copy the helpers or import them if exported; copying is fine):

```ts
import { expect, test, describe } from "vite-plus/test"
import { buildSearchPayload, buildSearchIndexPayload } from "./search-payload"
import type { ReferenceMaps } from "./types"

function emptyRefMaps(): ReferenceMaps {
  return {
    offices: new Map(),
    sources: new Map(),
    praenomina: new Map(),
    tribes: new Map(),
    relationships: new Map(),
    noteTypes: new Map(),
    dateTypes: new Map(),
    sexes: new Map(),
    statuses: new Map(),
    provinces: new Map(),
  }
}

// makePerson / makeAssertion: same fixture helpers as aggregate-references.test.ts
// (copy them here, including all current required PersonSummary/PostAssertion fields)

describe("buildSearchPayload", () => {
  test("builds office table and career tuples per person", () => {
    const consul = makeAssertion({
      id: "pa1",
      officeName: "consul",
      dateStart: -100,
      dateEnd: -100,
    })
    const praetor = makeAssertion({
      id: "pa2",
      officeName: "praetor",
      dateStart: -104,
      dateEnd: null,
    })
    const p = makePerson({
      id: "AAAA0001",
      postAssertions: [praetor, consul],
      offices: ["praetor", "consul"],
    })
    const payload = buildSearchPayload([p], emptyRefMaps())
    expect(payload.officeNames.sort()).toEqual(["consul", "praetor"])
    const tuples = payload.careers["AAAA0001"]
    expect(tuples).toHaveLength(2)
    const byOffice = Object.fromEntries(
      tuples.map(([idx, s, e]) => [payload.officeNames[idx], [s, e]])
    )
    expect(byOffice["consul"]).toEqual([-100, -100])
    expect(byOffice["praetor"]).toEqual([-104, null])
    expect(payload.summaries[0].id).toBe("AAAA0001")
    expect(payload.histogram.counts.reduce((a, b) => a + b, 0)).toBe(2)
  })

  test("assertions with empty office names are excluded from tuples", () => {
    const anon = makeAssertion({ id: "pa3", officeName: "", dateStart: -50 })
    const p = makePerson({ id: "BBBB0001", postAssertions: [anon] })
    const payload = buildSearchPayload([p], emptyRefMaps())
    expect(payload.careers["BBBB0001"]).toBeUndefined()
  })
})

describe("buildSearchIndexPayload", () => {
  test("emits a loadable index and serializable options", () => {
    const p = makePerson({ id: "CCCC0001", name: "CCCC0001 C. Cornelius" })
    const { index, options } = buildSearchIndexPayload([p])
    expect(options.idField).toBe("id")
    expect(typeof index).toBe("object")
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/data/search-payload.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

`site/src/data/search-payload.ts`:

```ts
// site/src/data/search-payload.ts
import { toSummaries } from "./loader"
import { buildSearchIndex, MINISEARCH_OPTIONS } from "./search-index"
import { buildNameHierarchy } from "./aggregate-references"
import { buildHistogram, type Histogram } from "../lib/histogram"
import type { Person, PersonSummary, ReferenceMaps } from "./types"

export interface SearchPayload {
  summaries: PersonSummary[]
  officeNames: string[]
  careers: Record<string, [number, number | null, number | null][]>
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  histogram: Histogram
}

export interface SerializableOptions {
  fields: string[]
  storeFields: string[]
  idField: string
  searchOptions: {
    prefix: boolean
    fuzzy: number
    boost: Record<string, number>
  }
}

export interface SearchIndexPayload {
  index: object
  options: SerializableOptions
}

export function buildSearchPayload(
  persons: Person[],
  refs: ReferenceMaps
): SearchPayload {
  const officeIndex = new Map<string, number>()
  const officeNames: string[] = []
  const careers: SearchPayload["careers"] = {}
  const allRanges: [number | null, number | null][] = []

  for (const p of persons) {
    const tuples: [number, number | null, number | null][] = []
    for (const pa of p.postAssertions) {
      if (!pa.officeName) continue
      let idx = officeIndex.get(pa.officeName)
      if (idx === undefined) {
        idx = officeNames.length
        officeNames.push(pa.officeName)
        officeIndex.set(pa.officeName, idx)
      }
      tuples.push([idx, pa.dateStart, pa.dateEnd])
      allRanges.push([pa.dateStart, pa.dateEnd])
    }
    if (tuples.length > 0) careers[p.id] = tuples
  }

  return {
    summaries: toSummaries(persons),
    officeNames,
    careers,
    officeHierarchy: buildNameHierarchy(refs.offices),
    provinceHierarchy: buildNameHierarchy(refs.provinces),
    histogram: buildHistogram(allRanges),
  }
}

export function buildSearchIndexPayload(
  summaries: PersonSummary[]
): SearchIndexPayload {
  return {
    index: buildSearchIndex(summaries),
    options: {
      fields: MINISEARCH_OPTIONS.fields as string[],
      storeFields: MINISEARCH_OPTIONS.storeFields as string[],
      idField: MINISEARCH_OPTIONS.idField as string,
      searchOptions: {
        prefix: MINISEARCH_OPTIONS.searchOptions?.prefix as boolean,
        fuzzy: MINISEARCH_OPTIONS.searchOptions?.fuzzy as number,
        boost: MINISEARCH_OPTIONS.searchOptions?.boost as Record<string, number>,
      },
    },
  }
}
```

- [ ] **Step 4: Run tests and check**

Run: `cd site && vp test src/data && vp check`
Expected: PASS, green.

- [ ] **Step 5: Commit**

```bash
git add site/src/data/search-payload.ts site/src/data/search-payload.test.ts
git commit -m "feat: add search payload builder with career tuples and histogram"
```

---

### Task 4: Static data emission (Vite plugin)

**Files:**
- Create: `site/src/build/search-data-plugin.ts`
- Modify: `site/vite.config.ts`
- Modify: `.gitignore` (repo root — add `site/public/data/`)

**Interfaces:**
- Produces: at dev-server start AND at build start, `site/public/data/search-data.json` (the `SearchPayload`) and `site/public/data/search-index.json` (the `SearchIndexPayload`) are (re)generated. Vite's `publicDir` handling then serves them in dev and copies them into `dist/client/data/` at build. Client URL: `${import.meta.env.BASE_URL}data/search-data.json`.

- [ ] **Step 1: Implement the plugin**

`site/src/build/search-data-plugin.ts`:

```ts
// site/src/build/search-data-plugin.ts
import { mkdir, writeFile } from "node:fs/promises"
import { join } from "node:path"
// If vite-plus doesn't re-export the Plugin type, fall back to a structural
// type: `type Plugin = { name: string; configResolved?: (c: { root: string }) => void; buildStart?: () => Promise<void> | void }`
import type { Plugin } from "vite-plus"

/**
 * Generates the static search-data JSON assets into public/data/ so both
 * the dev server and the production build serve identical files. Runs the
 * TTL pipeline once per vite startup (buildStart), guarded so parallel
 * environments (client/ssr) don't regenerate concurrently.
 */
export function searchDataPlugin(): Plugin {
  let generated: Promise<void> | null = null

  async function generate(root: string): Promise<void> {
    const { loadAllData } = await import("../data/loader")
    const { buildSearchPayload, buildSearchIndexPayload } = await import(
      "../data/search-payload"
    )
    const { persons, refs } = await loadAllData()
    const payload = buildSearchPayload(persons, refs)
    const indexPayload = buildSearchIndexPayload(payload.summaries)
    const outDir = join(root, "public", "data")
    await mkdir(outDir, { recursive: true })
    await writeFile(
      join(outDir, "search-data.json"),
      JSON.stringify(payload)
    )
    await writeFile(
      join(outDir, "search-index.json"),
      JSON.stringify(indexPayload)
    )
    console.log(
      `[search-data] wrote public/data/*.json (${payload.summaries.length} summaries)`
    )
  }

  let root = process.cwd()
  return {
    name: "dprr-search-data",
    configResolved(config) {
      root = config.root
    },
    buildStart() {
      generated ??= generate(root)
      return generated
    },
  }
}
```

`site/vite.config.ts` — import and register first in `plugins`:

```ts
import { searchDataPlugin } from "./src/build/search-data-plugin"
// …
  plugins: [
    searchDataPlugin(),
    devtools(),
    // …
```

`.gitignore` (repo root) — add a line: `site/public/data/`

- [ ] **Step 2: Verify in dev and build**

```bash
cd site && vp dev --port 3000 &
sleep 20
curl -s -o /dev/null -w '%{http_code} %{size_download}\n' http://localhost:3000/dprr-data/data/search-data.json || curl -s -o /dev/null -w '%{http_code} %{size_download}\n' http://localhost:3000/data/search-data.json
kill %1
ls -la public/data/
```

Expected: HTTP 200 with a multi-hundred-KB size; both JSON files on disk. Then `vp build 2>&1 | tail -5` and `ls dist/client/data/` — both files present in build output.

- [ ] **Step 3: Check and commit**

Run: `cd site && vp check`

```bash
git add site/src/build/search-data-plugin.ts site/vite.config.ts ../.gitignore
git commit -m "feat: emit static search-data JSON assets via vite plugin"
```

(If `.gitignore` at repo root is untracked, `git add .gitignore` from the repo root — it already exists untracked; add ONLY the `site/public/data/` line to it, keep its other content unchanged.)

---

### Task 5: Client search-data hook and page swap

**Files:**
- Create: `site/src/lib/use-search-data.ts`
- Modify: `site/src/routes/index.tsx` (drop server-fn loader; fetch lazily)
- Modify: `site/src/server/data.ts` (delete `getSearchData` and its `SerializableOptions` block — now unused)

**Interfaces:**
- Produces:

```ts
export interface SearchDataBundle {
  payload: SearchPayload
  miniSearch: MiniSearch<PersonSummary>
}
/** Fetch-once (module-cached) search data. enabled=false renders nothing. */
export function useSearchData(enabled: boolean): {
  bundle: SearchDataBundle | null
  error: string | null
}
```

- The search page keeps rendering the full current UI, but sources data from the hook (temporary loading state: "Loading search data…"). The two-state landing arrives in Task 14 — after this task the page just always enables the fetch.

- [ ] **Step 1: Implement the hook**

`site/src/lib/use-search-data.ts`:

```ts
// site/src/lib/use-search-data.ts
import { useEffect, useState } from "react"
import MiniSearch from "minisearch"
import type { PersonSummary } from "@/data/types"
import type {
  SearchPayload,
  SearchIndexPayload,
} from "@/data/search-payload"

export interface SearchDataBundle {
  payload: SearchPayload
  miniSearch: MiniSearch<PersonSummary>
}

let cache: Promise<SearchDataBundle> | null = null

function load(): Promise<SearchDataBundle> {
  cache ??= (async () => {
    const base = import.meta.env.BASE_URL
    const [payloadRes, indexRes] = await Promise.all([
      fetch(`${base}data/search-data.json`),
      fetch(`${base}data/search-index.json`),
    ])
    if (!payloadRes.ok || !indexRes.ok) {
      throw new Error(
        `search data fetch failed: ${payloadRes.status}/${indexRes.status}`
      )
    }
    const payload = (await payloadRes.json()) as SearchPayload
    const indexPayload = (await indexRes.json()) as SearchIndexPayload
    const miniSearch = MiniSearch.loadJSON<PersonSummary>(
      JSON.stringify(indexPayload.index),
      indexPayload.options
    )
    return { payload, miniSearch }
  })()
  return cache
}

export function useSearchData(enabled: boolean): {
  bundle: SearchDataBundle | null
  error: string | null
} {
  const [bundle, setBundle] = useState<SearchDataBundle | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!enabled) return
    let alive = true
    load().then(
      (b) => alive && setBundle(b),
      (e: unknown) => {
        if (alive) {
          setError(e instanceof Error ? e.message : String(e))
          cache = null
        }
      }
    )
    return () => {
      alive = false
    }
  }, [enabled])

  return { bundle, error }
}
```

- [ ] **Step 2: Swap the search page's data source**

`site/src/routes/index.tsx`:
- Delete the `loader: () => getSearchData(),` line and the `getSearchData` import.
- In `SearchPage`, replace `const { summaries, searchIndex } = Route.useLoaderData()` with:

```tsx
  const { bundle, error } = useSearchData(true)
  if (error) {
    return (
      <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
        Failed to load search data — please reload. ({error})
      </div>
    )
  }
  if (!bundle) {
    return (
      <div className="mx-auto max-w-6xl px-4 py-12 text-center text-muted-foreground">
        Loading search data…
      </div>
    )
  }
```

- `useSearchState(summaries, searchIndex)` becomes `useSearchState(bundle.payload.summaries, bundle.miniSearch)` — and change `useSearchState`'s second parameter in `site/src/lib/search.ts` from `searchIndexJson: object` to `miniSearch: MiniSearch<PersonSummary>`, deleting its internal `useMemo`/`loadJSON` (the hook now receives the instance). Hierarchy props for the sidebar come from `bundle.payload.officeHierarchy` / `bundle.payload.provinceHierarchy`.
- The hidden crawl links block STAYS for now (removed in Task 15). It uses `summaries` — source it from `bundle.payload.summaries`. NOTE: the hidden links now only render client-side after fetch, which would break the prerender crawl — so for THIS task only, hardcode the crawl block to render from a new tiny loader: `loader: () => getAllPersonIds()` (existing server fn, an array of ~4,876 strings, ~90 KB — a stopgap until Task 15's `/directory/` page). Render `<a key={id} href={`${import.meta.env.BASE_URL}persons/${id}`}>{id}</a>` from `Route.useLoaderData()`.

`site/src/server/data.ts` — delete `getSearchData`, its `SerializableOptions` interface and `serializableOptions` constant, and now-unused imports (`buildSearchIndex`, `MINISEARCH_OPTIONS`, `toSummaries` if unused elsewhere in the file — `buildTribeDetail` consumes `toSummaries` via aggregate module, not here).

- [ ] **Step 3: Verify**

`cd site && vp check && vp test src` then dev server: search page shows "Loading search data…" briefly, then works exactly as before (facets, timeline inputs, results). `vp build 2>&1 | tail -3` succeeds; person page count unchanged (`find dist/client/persons -name index.html | wc -l` ≈ 4876) — the stopgap ID loader keeps the crawl alive.

- [ ] **Step 4: Commit**

```bash
git add site/src/lib/use-search-data.ts site/src/routes/index.tsx site/src/server/data.ts site/src/lib/search.ts
git commit -m "feat: fetch search data lazily from static JSON assets"
```

---

### Task 6: Search params — new state fields

**Files:**
- Modify: `site/src/data/types.ts` (`SearchState`)
- Modify: `site/src/lib/search.ts` (`parseSearchParams`, `toSearchParams`)
- Test: `site/src/lib/search.test.ts`

**Interfaces:**
- Produces on `SearchState`:

```ts
  event: string[]
  praenomen: string[]
  cognomen: string[]
  /** Case-insensitive substring match against reNumber. */
  re: string
  officeMode: "any" | "all"
  officeInRange: boolean
  sort: "earliest" | "latest" | "name" | "relevance" | null
```

- URL params: `event`, `praenomen`, `cognomen` (comma-separated, encoded like `office`); `re` (plain string); `officeMode=all` (omitted when "any"); `officeInRange=true` (omitted when false); `sort` (omitted when null). Defaults: `officeMode: "any"`, `officeInRange: false`, `sort: null`.

- [ ] **Step 1: Write the failing tests**

Add to `site/src/lib/search.test.ts`:

```ts
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd site && vp test src/lib/search.test.ts`
Expected: FAIL.

- [ ] **Step 3: Implement**

`types.ts`: add the fields above to `SearchState`.

`search.ts` `parseSearchParams` additions:

```ts
    event: splitFacetParam(params.event),
    praenomen: splitFacetParam(params.praenomen),
    cognomen: splitFacetParam(params.cognomen),
    re: params.re ?? "",
    officeMode: params.officeMode === "all" ? "all" : "any",
    officeInRange: params.officeInRange === "true",
    sort:
      params.sort === "earliest" ||
      params.sort === "latest" ||
      params.sort === "name" ||
      params.sort === "relevance"
        ? params.sort
        : null,
```

`toSearchParams` additions:

```ts
  if (state.event.length) params.event = joinFacetParam(state.event)
  if (state.praenomen.length) params.praenomen = joinFacetParam(state.praenomen)
  if (state.cognomen.length) params.cognomen = joinFacetParam(state.cognomen)
  if (state.re) params.re = state.re
  if (state.officeMode !== "any") params.officeMode = state.officeMode
  if (state.officeInRange) params.officeInRange = "true"
  if (state.sort) params.sort = state.sort
```

(Adapt to the file's actual helper names — `splitFacetParam`/`joinFacetParam` exist from the Plan-1 comma-encoding fix; if `splitFacetParam` doesn't accept `undefined`, keep the existing `params.x ? split : []` pattern.)

- [ ] **Step 4: Run tests and check** — `cd site && vp test src/lib && vp check` → PASS.

- [ ] **Step 5: Commit**

```bash
git add site/src/data/types.ts site/src/lib/search.ts site/src/lib/search.test.ts
git commit -m "feat: add advanced search params (events, name parts, office mode, sort)"
```

---

### Task 7: Filter module (hierarchy expansion, AND mode, office-in-range, events, name parts)

**Files:**
- Create: `site/src/lib/filter.ts` (move `matchesFacets` here and extend)
- Test: `site/src/lib/filter.test.ts`
- Modify: `site/src/lib/search.ts` (import from filter module; delete local copy)

**Interfaces:**
- Produces:

```ts
export interface FilterContext {
  /** office child name → parent name (from payload.officeHierarchy). */
  parentOf: Record<string, string | null>
  careers: Record<string, [number, number | null, number | null][]>
  officeNames: string[]
}
/** Selected value → the set of it plus all hierarchy descendants. */
export function descendantSet(
  value: string,
  parentOf: Record<string, string | null>
): Set<string>
export function matchesFacets(
  person: PersonSummary,
  state: SearchState,
  ctx: FilterContext
): boolean
```

- Semantics (binding):
  - Each selected office value expands to `descendantSet` (subtree selection).
  - `officeMode "any"`: at least one selection's set intersects the person's offices; `"all"`: every selection's set intersects.
  - `officeInRange` active (true AND offices selected AND at least one of eraFrom/eraTo set): office matching switches to assertion level — a career tuple whose office name is in the selection's set and whose dated span overlaps `[eraFrom ?? -Infinity, eraTo ?? +Infinity]` (span = `[dateStart ?? dateEnd, dateEnd ?? dateStart]`; tuples with both null never match). The person-level era-overlap check is skipped in this mode (the assertion check subsumes it). `officeMode` applies per selection as above.
  - `event`: `person.lifeEvents` contains any selected value.
  - `praenomen`/`cognomen`: exact membership. `re`: case-insensitive substring of `person.reNumber ?? ""` (empty `re` = no filter).
  - All existing checks (nomen, sex, patrician, nobilis, tribe, province, plain era) keep today's semantics.

- [ ] **Step 1: Write the failing tests**

`site/src/lib/filter.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import { descendantSet, matchesFacets, type FilterContext } from "./filter"
import { parseSearchParams } from "./search"
import type { PersonSummary } from "@/data/types"

const parentOf = {
  "Magisterial Posts": null,
  consul: "Magisterial Posts",
  "consul suffectus": "consul",
  praetor: "Magisterial Posts",
}

function makeSummary(over: Partial<PersonSummary>): PersonSummary {
  return {
    id: "TEST0001",
    name: "TEST0001 T. Testius",
    praenomen: "Titus",
    nomen: "Testius",
    cognomen: null,
    otherNames: null,
    sex: "Male",
    isPatrician: false,
    isNobilis: false,
    highestOffice: null,
    eraFrom: -120,
    eraTo: -80,
    tribes: [],
    offices: [],
    provinces: [],
    reNumber: null,
    filiation: null,
    lifeEvents: [],
    ...over,
  }
}

function ctx(over: Partial<FilterContext> = {}): FilterContext {
  return { parentOf, careers: {}, officeNames: [], ...over }
}

function state(params: Record<string, string>) {
  return parseSearchParams(params)
}

describe("descendantSet", () => {
  test("includes the value and all descendants", () => {
    expect(descendantSet("consul", parentOf)).toEqual(
      new Set(["consul", "consul suffectus"])
    )
    expect(descendantSet("Magisterial Posts", parentOf)).toEqual(
      new Set(["Magisterial Posts", "consul", "consul suffectus", "praetor"])
    )
    expect(descendantSet("unknown", parentOf)).toEqual(new Set(["unknown"]))
  })
})

describe("office matching", () => {
  const suffect = makeSummary({ offices: ["consul suffectus"] })

  test("subtree: selecting a parent matches descendant holders", () => {
    expect(matchesFacets(suffect, state({ office: "consul" }), ctx())).toBe(true)
    expect(
      matchesFacets(suffect, state({ office: "Magisterial%20Posts" }), ctx())
    ).toBe(true)
    expect(matchesFacets(suffect, state({ office: "praetor" }), ctx())).toBe(false)
  })

  test("officeMode all requires every selection", () => {
    const both = makeSummary({ offices: ["consul", "praetor"] })
    const one = makeSummary({ offices: ["consul"] })
    const s = state({ office: "consul,praetor", officeMode: "all" })
    expect(matchesFacets(both, s, ctx())).toBe(true)
    expect(matchesFacets(one, s, ctx())).toBe(false)
    // default OR still matches
    expect(matchesFacets(one, state({ office: "consul,praetor" }), ctx())).toBe(true)
  })

  test("officeInRange matches at the assertion level", () => {
    const p = makeSummary({ offices: ["consul"], eraFrom: -150, eraTo: -80 })
    const c = ctx({
      officeNames: ["consul"],
      careers: { TEST0001: [[0, -140, -140]] },
    })
    const inRange = state({
      office: "consul",
      eraFrom: "-145",
      eraTo: "-135",
      officeInRange: "true",
    })
    const outOfRange = state({
      office: "consul",
      eraFrom: "-100",
      eraTo: "-90",
      officeInRange: "true",
    })
    const personLevel = state({ office: "consul", eraFrom: "-100", eraTo: "-90" })
    expect(matchesFacets(p, inRange, c)).toBe(true)
    expect(matchesFacets(p, outOfRange, c)).toBe(false)
    // without the toggle, person-level era overlap still matches
    expect(matchesFacets(p, personLevel, c)).toBe(true)
  })

  test("officeInRange ignores undated assertions", () => {
    const p = makeSummary({ offices: ["consul"] })
    const c = ctx({
      officeNames: ["consul"],
      careers: { TEST0001: [[0, null, null]] },
    })
    const s = state({
      office: "consul",
      eraFrom: "-145",
      eraTo: "-135",
      officeInRange: "true",
    })
    expect(matchesFacets(p, s, c)).toBe(false)
  })
})

describe("events and name parts", () => {
  test("life events filter disjunctively", () => {
    const p = makeSummary({ lifeEvents: ["exiled"] })
    expect(matchesFacets(p, state({ event: "exiled,proscribed" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ event: "proscribed" }), ctx())).toBe(false)
  })

  test("praenomen, cognomen, and RE substring", () => {
    const p = makeSummary({
      praenomen: "Lucius",
      cognomen: "Brutus",
      reNumber: "RE 46a",
    })
    expect(matchesFacets(p, state({ praenomen: "Lucius" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ praenomen: "Gaius" }), ctx())).toBe(false)
    expect(matchesFacets(p, state({ cognomen: "Brutus" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ re: "46A" }), ctx())).toBe(true)
    expect(matchesFacets(p, state({ re: "99" }), ctx())).toBe(false)
  })
})
```

- [ ] **Step 2: Run tests to verify they fail** — `cd site && vp test src/lib/filter.test.ts` → FAIL (module not found).

- [ ] **Step 3: Implement**

`site/src/lib/filter.ts` — move the existing `matchesFacets` body from `search.ts` and extend:

```ts
// site/src/lib/filter.ts
import type { PersonSummary, SearchState } from "@/data/types"

export interface FilterContext {
  parentOf: Record<string, string | null>
  careers: Record<string, [number, number | null, number | null][]>
  officeNames: string[]
}

export function descendantSet(
  value: string,
  parentOf: Record<string, string | null>
): Set<string> {
  const result = new Set<string>([value])
  let added = true
  while (added) {
    added = false
    for (const [child, parent] of Object.entries(parentOf)) {
      if (parent !== null && result.has(parent) && !result.has(child)) {
        result.add(child)
        added = true
      }
    }
  }
  return result
}

function officeSelectionSets(
  selected: string[],
  parentOf: Record<string, string | null>
): Set<string>[] {
  return selected.map((s) => descendantSet(s, parentOf))
}

function intersects(set: Set<string>, values: string[]): boolean {
  return values.some((v) => set.has(v))
}

function assertionMatches(
  set: Set<string>,
  tuples: [number, number | null, number | null][],
  officeNames: string[],
  from: number | null,
  to: number | null
): boolean {
  for (const [idx, dateStart, dateEnd] of tuples) {
    if (!set.has(officeNames[idx])) continue
    const start = dateStart ?? dateEnd
    const end = dateEnd ?? dateStart
    if (start === null || end === null) continue
    if (from !== null && end < from) continue
    if (to !== null && start > to) continue
    return true
  }
  return false
}

export function matchesFacets(
  person: PersonSummary,
  state: SearchState,
  ctx: FilterContext
): boolean {
  const inRangeMode =
    state.officeInRange &&
    state.office.length > 0 &&
    (state.eraFrom !== null || state.eraTo !== null)

  if (state.office.length > 0) {
    const sets = officeSelectionSets(state.office, ctx.parentOf)
    if (inRangeMode) {
      const tuples = ctx.careers[person.id] ?? []
      const check = (set: Set<string>) =>
        assertionMatches(set, tuples, ctx.officeNames, state.eraFrom, state.eraTo)
      if (state.officeMode === "all" ? !sets.every(check) : !sets.some(check))
        return false
    } else {
      const check = (set: Set<string>) => intersects(set, person.offices)
      if (state.officeMode === "all" ? !sets.every(check) : !sets.some(check))
        return false
    }
  }

  if (state.nomen.length > 0 && !state.nomen.includes(person.nomen)) return false
  if (state.sex.length > 0 && !state.sex.includes(person.sex)) return false
  if (state.patrician !== null && person.isPatrician !== state.patrician)
    return false
  if (state.nobilis !== null && person.isNobilis !== state.nobilis) return false
  if (
    state.tribe.length > 0 &&
    !state.tribe.some((t) => person.tribes.includes(t))
  )
    return false
  if (
    state.province.length > 0 &&
    !state.province.some((pr) => person.provinces.includes(pr))
  )
    return false
  if (
    state.event.length > 0 &&
    !state.event.some((e) => person.lifeEvents.includes(e))
  )
    return false
  if (
    state.praenomen.length > 0 &&
    !state.praenomen.includes(person.praenomen)
  )
    return false
  if (
    state.cognomen.length > 0 &&
    (!person.cognomen || !state.cognomen.includes(person.cognomen))
  )
    return false
  if (
    state.re &&
    !(person.reNumber ?? "").toLowerCase().includes(state.re.toLowerCase())
  )
    return false

  if (!inRangeMode) {
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
  }
  return true
}
```

`search.ts`: delete its local `matchesFacets`, import from `./filter`, and thread a `FilterContext` through (built in Task 8). For THIS task, `search.ts` may temporarily construct `{ parentOf: {}, careers: {}, officeNames: [] }` so it compiles and behavior is unchanged (empty parentOf = no subtree expansion beyond self, which matches current behavior).

- [ ] **Step 4: Run tests and check** — `cd site && vp test src/lib && vp check` → PASS.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/filter.ts site/src/lib/filter.test.ts site/src/lib/search.ts
git commit -m "feat: add filter module with subtree, AND-mode, in-range, event, and name-part matching"
```

---

### Task 8: Order module + hook wiring (context, counts, sorted results, filtered histogram)

**Files:**
- Create: `site/src/lib/order.ts`
- Test: `site/src/lib/order.test.ts`
- Modify: `site/src/lib/search.ts` (`useSearchState`)
- Modify: `site/src/routes/index.tsx` (pass the bundle through)

**Interfaces:**
- Produces (`@/lib/order`):

```ts
export const UNDATED = Number.MAX_SAFE_INTEGER
/** Earliest-known-year key; undated → UNDATED (sorts last ascending). */
export function eraKey(p: { eraFrom: number | null; eraTo: number | null }): number
export function compareByName(a: { name: string }, b: { name: string }): number
/** sort=null resolves to "relevance" when hasQuery else "earliest". */
export function sortResults(
  results: PersonSummary[],
  sort: SearchState["sort"],
  hasQuery: boolean
): PersonSummary[]
```

  `compareByName` compares display names (ID prefix stripped). `sortResults` returns a NEW array; `"relevance"` preserves input order (MiniSearch order upstream).
- Produces (`useSearchState`): signature becomes `useSearchState(bundle: SearchDataBundle)`; returns `{ state, results, facets, updateState, clearAll, filteredHistogram }` where `facets` gains `event`, `praenomen`, `cognomen` (via `countWith`), `results` are sorted via `sortResults`, and `filteredHistogram: Histogram` is `payload.histogram` when no query/filters are active, else `buildHistogram` over the filtered persons' career tuples.

- [ ] **Step 1: Write the failing order tests**

`site/src/lib/order.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import { eraKey, compareByName, sortResults, UNDATED } from "./order"

const p = (id: string, name: string, eraFrom: number | null, eraTo: number | null) =>
  ({ id, name, eraFrom, eraTo }) as never

describe("ordering", () => {
  test("eraKey uses eraFrom, falls back to eraTo, undated last", () => {
    expect(eraKey({ eraFrom: -509, eraTo: -31 })).toBe(-509)
    expect(eraKey({ eraFrom: null, eraTo: -100 })).toBe(-100)
    expect(eraKey({ eraFrom: null, eraTo: null })).toBe(UNDATED)
  })

  test("compareByName strips the DPRR ID prefix", () => {
    expect(
      compareByName({ name: "IUNI0001 L. Iunius" }, { name: "AEMI0002 M. Aemilius" })
    ).toBeGreaterThan(0)
  })

  test("sortResults: earliest default, latest, name, relevance passthrough", () => {
    const list = [
      p("B", "BBBB0001 B", -100, -90),
      p("A", "AAAA0001 A", -200, -150),
      p("U", "UUUU0001 U", null, null),
    ]
    expect(sortResults(list, null, false).map((x: { id: string }) => x.id)).toEqual([
      "A",
      "B",
      "U",
    ])
    expect(sortResults(list, "latest", false).map((x: { id: string }) => x.id)).toEqual([
      "B",
      "A",
      "U",
    ])
    expect(sortResults(list, "name", false)[0].id).toBe("A")
    expect(sortResults(list, null, true)).toEqual(list) // relevance = passthrough
    expect(sortResults(list, "earliest", true)[0].id).toBe("A")
  })
})
```

- [ ] **Step 2: Run to verify FAIL**, then **Step 3: Implement**

`site/src/lib/order.ts`:

```ts
// site/src/lib/order.ts
import type { PersonSummary, SearchState } from "@/data/types"

export const UNDATED = Number.MAX_SAFE_INTEGER

export function eraKey(p: {
  eraFrom: number | null
  eraTo: number | null
}): number {
  return p.eraFrom ?? p.eraTo ?? UNDATED
}

const stripId = (name: string) => name.replace(/^[A-Z]{4}\d+ /, "")

export function compareByName(
  a: { name: string },
  b: { name: string }
): number {
  return stripId(a.name).localeCompare(stripId(b.name))
}

export function sortResults(
  results: PersonSummary[],
  sort: SearchState["sort"],
  hasQuery: boolean
): PersonSummary[] {
  const resolved = sort ?? (hasQuery ? "relevance" : "earliest")
  const copy = [...results]
  switch (resolved) {
    case "relevance":
      return copy
    case "name":
      return copy.sort(compareByName)
    case "latest":
      return copy.sort(
        (a, b) =>
          (eraKey(b) === UNDATED ? -UNDATED : eraKey(b)) -
            (eraKey(a) === UNDATED ? -UNDATED : eraKey(a)) || compareByName(a, b)
      )
    case "earliest":
      return copy.sort((a, b) => eraKey(a) - eraKey(b) || compareByName(a, b))
  }
}
```

(Note "latest": undated must STILL sort last — the `-UNDATED` swap achieves that; the test pins it.)

- [ ] **Step 4: Wire the hook**

`site/src/lib/search.ts` — `useSearchState(bundle: SearchDataBundle)`:
- Destructure `const { payload, miniSearch } = bundle`; use `payload.summaries` everywhere `summaries` was used.
- Build the real filter context once: `const ctx = useMemo(() => ({ parentOf: payload.officeHierarchy, careers: payload.careers, officeNames: payload.officeNames }), [payload])`; pass `ctx` to every `matchesFacets` call (including inside `countWith`).
- `results`: after filtering, `return sortResults(filtered, state.sort, state.q.trim().length > 0)`.
- `facets`: add `event: countWith("event", "lifeEvents"), praenomen: countWith("praenomen", "praenomen"), cognomen: countWith("cognomen", "cognomen"),`.
- `filteredHistogram`: 

```ts
  const filteredHistogram = useMemo(() => {
    // A sort-only URL counts as unfiltered.
    const anyFilter =
      Object.keys(toSearchParams({ ...state, sort: null })).length > 0
    if (!anyFilter) return payload.histogram
    const ranges: [number | null, number | null][] = []
    for (const p of results) {
      for (const [, s, e] of payload.careers[p.id] ?? []) ranges.push([s, e])
    }
    return buildHistogram(ranges)
  }, [state, results, payload])
```

  (import `buildHistogram` from `@/lib/histogram`; the `anyFilter` check treats a sort-only URL as unfiltered).
- Return `filteredHistogram` in the hook result.

`index.tsx`: `useSearchState(bundle)`; sidebar receives `histogram={filteredHistogram}` starting Task 12 (until then nothing consumes it — that's fine).

- [ ] **Step 5: Run everything and commit**

`cd site && vp test src && vp check` → PASS.

```bash
git add site/src/lib/order.ts site/src/lib/order.test.ts site/src/lib/search.ts site/src/routes/index.tsx
git commit -m "feat: add ordering module and wire filter context, counts, and filtered histogram"
```

---

### Task 9: BC/AD year conversion + YearInput component

**Files:**
- Modify: `site/src/lib/dates.ts`
- Test: `site/src/lib/dates.test.ts`
- Create: `site/src/components/year-input.tsx`

**Interfaces:**
- Produces (`@/lib/dates`):

```ts
export type EraLabel = "BC" | "AD"
/** "509 BC" → -509; "14 AD" → 14. Years < 1 clamp to 1. */
export function toSignedYear(year: number, era: EraLabel): number
/** -509 → {year: 509, era: "BC"}; 0 → {year: 1, era: "BC"}; 14 → {year: 14, era: "AD"}. */
export function fromSignedYear(signed: number): { year: number; era: EraLabel }
```

- Produces: `<YearInput value={number | null} onChange={(v: number | null) => void} placeholder={string} />` — an unsigned number input + BC/AD select (default BC), emitting signed years; empty input emits null.

- [ ] **Step 1: Failing tests** — add to `site/src/lib/dates.test.ts`:

```ts
describe("signed year conversion", () => {
  test("BC years negate, AD years pass through", () => {
    expect(toSignedYear(509, "BC")).toBe(-509)
    expect(toSignedYear(14, "AD")).toBe(14)
  })
  test("year zero policy: 0 = 1 BC, inputs below 1 clamp", () => {
    expect(fromSignedYear(0)).toEqual({ year: 1, era: "BC" })
    expect(toSignedYear(0, "BC")).toBe(-1)
    expect(toSignedYear(-5, "AD")).toBe(1)
  })
  test("round-trips", () => {
    expect(toSignedYear(fromSignedYear(-509).year, fromSignedYear(-509).era)).toBe(-509)
    expect(fromSignedYear(14)).toEqual({ year: 14, era: "AD" })
  })
})
```

- [ ] **Step 2: Run FAIL, Step 3: Implement**

Append to `site/src/lib/dates.ts`:

```ts
export type EraLabel = "BC" | "AD"

export function toSignedYear(year: number, era: EraLabel): number {
  const y = Math.max(1, Math.floor(Math.abs(year)) || 1)
  return era === "BC" ? -y : y
}

export function fromSignedYear(signed: number): { year: number; era: EraLabel } {
  if (signed <= 0) return { year: Math.abs(signed) || 1, era: "BC" }
  return { year: signed, era: "AD" }
}
```

`site/src/components/year-input.tsx`:

```tsx
// site/src/components/year-input.tsx
import { useEffect, useState } from "react"
import { Input } from "@/components/ui/input"
import { toSignedYear, fromSignedYear, type EraLabel } from "@/lib/dates"

interface YearInputProps {
  value: number | null
  onChange: (value: number | null) => void
  placeholder?: string
  "aria-label"?: string
}

/** Unsigned year + BC/AD selector; emits signed years (BC negative). */
export function YearInput({ value, onChange, placeholder, ...aria }: YearInputProps) {
  const parts = value !== null ? fromSignedYear(value) : null
  const [text, setText] = useState(parts ? String(parts.year) : "")
  const [era, setEra] = useState<EraLabel>(parts?.era ?? "BC")

  useEffect(() => {
    const next = value !== null ? fromSignedYear(value) : null
    setText(next ? String(next.year) : "")
    if (next) setEra(next.era)
  }, [value])

  function emit(nextText: string, nextEra: EraLabel) {
    const year = Number.parseInt(nextText, 10)
    onChange(Number.isNaN(year) ? null : toSignedYear(year, nextEra))
  }

  return (
    <span className="inline-flex items-center gap-1">
      <Input
        type="number"
        min={1}
        inputMode="numeric"
        value={text}
        placeholder={placeholder}
        onChange={(e) => {
          setText(e.target.value)
          emit(e.target.value, era)
        }}
        className="h-7 w-20 text-xs"
        {...aria}
      />
      <select
        value={era}
        onChange={(e) => {
          const next = e.target.value as EraLabel
          setEra(next)
          emit(text, next)
        }}
        className="h-7 rounded-md border bg-transparent px-1 text-xs"
        aria-label="era"
      >
        <option>BC</option>
        <option>AD</option>
      </select>
    </span>
  )
}
```

- [ ] **Step 4: Run tests and check** — `cd site && vp test src/lib/dates.test.ts && vp check` → PASS.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/dates.ts site/src/lib/dates.test.ts site/src/components/year-input.tsx
git commit -m "feat: add BC/AD year conversion and YearInput component"
```

---

### Task 10: EraTimeline component (area curve + drag handles)

**Files:**
- Create: `site/src/components/era-timeline.tsx`

**Interfaces:**
- Consumes: `Histogram`/`binIndexFor` (Task 2), `YearInput` (Task 9), `formatYear` (dates).
- Produces:

```tsx
interface EraTimelineProps {
  histogram: Histogram
  from: number | null
  to: number | null
  onChange: (from: number | null, to: number | null) => void
}
export function EraTimeline(props: EraTimelineProps): JSX.Element
```

  SVG area curve of `histogram.counts`; the `[from,to]` span renders highlighted; two handles draggable via pointer events (drag moves the nearest bound, snapped to years); axis labels in BC ("500 BC · 400 · 300 · 200 · 100 · 31 BC" — computed from the histogram range at ~5 labels, never a minus sign); `YearInput` pair below; a "clear" affordance nulls both.

- [ ] **Step 1: Implement**

`site/src/components/era-timeline.tsx`:

```tsx
// site/src/components/era-timeline.tsx
import { useRef } from "react"
import type { Histogram } from "@/lib/histogram"
import { fromSignedYear } from "@/lib/dates"
import { YearInput } from "@/components/year-input"

interface EraTimelineProps {
  histogram: Histogram
  from: number | null
  to: number | null
  onChange: (from: number | null, to: number | null) => void
}

const W = 280
const H = 56

function axisYear(signed: number): string {
  const { year, era } = fromSignedYear(signed)
  return era === "BC" ? `${year}` : `AD ${year}`
}

export function EraTimeline({ histogram, from, to, onChange }: EraTimelineProps) {
  const svgRef = useRef<SVGSVGElement>(null)
  const { start, binSize, counts } = histogram
  const end = start + counts.length * binSize
  const span = end - start

  const yearToX = (y: number) => ((Math.min(Math.max(y, start), end) - start) / span) * W
  const xToYear = (x: number) => Math.round(start + (Math.min(Math.max(x, 0), W) / W) * span)

  const max = Math.max(...counts, 1)
  const points = counts.map((c, i) => {
    const x = ((i + 0.5) / counts.length) * W
    const y = H - (c / max) * (H - 4)
    return `${x},${y}`
  })
  const areaPath = `M0,${H} L${points.join(" L")} L${W},${H} Z`

  const selFrom = from ?? start
  const selTo = to ?? end
  const dragging = useRef<"from" | "to" | null>(null)

  function pointerYear(e: React.PointerEvent): number {
    const rect = svgRef.current!.getBoundingClientRect()
    return xToYear(((e.clientX - rect.left) / rect.width) * W)
  }

  function onPointerDown(e: React.PointerEvent) {
    const y = pointerYear(e)
    dragging.current =
      Math.abs(y - selFrom) <= Math.abs(y - selTo) ? "from" : "to"
    ;(e.target as Element).setPointerCapture(e.pointerId)
    onPointerMove(e)
  }

  function onPointerMove(e: React.PointerEvent) {
    if (!dragging.current) return
    const y = pointerYear(e)
    if (dragging.current === "from") onChange(Math.min(y, selTo), to ?? null)
    else onChange(from ?? null, Math.max(y, selFrom))
  }

  function onPointerUp() {
    dragging.current = null
  }

  const labelCount = 5
  const labels = Array.from({ length: labelCount + 1 }, (_, i) =>
    Math.round(start + (i / labelCount) * span)
  )

  return (
    <div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        className="h-16 w-full cursor-col-resize touch-none select-none"
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        role="slider"
        aria-label="Time period"
        aria-valuetext={`${axisYear(selFrom)} to ${axisYear(selTo)}`}
      >
        <path d={areaPath} className="fill-muted-foreground/25" />
        <rect
          x={yearToX(selFrom)}
          y={0}
          width={Math.max(yearToX(selTo) - yearToX(selFrom), 0)}
          height={H}
          className="fill-primary/15"
        />
        <line
          x1={yearToX(selFrom)} x2={yearToX(selFrom)} y1={0} y2={H}
          className="stroke-primary" strokeWidth={2}
        />
        <line
          x1={yearToX(selTo)} x2={yearToX(selTo)} y1={0} y2={H}
          className="stroke-primary" strokeWidth={2}
        />
      </svg>
      <div className="flex justify-between text-[10px] text-muted-foreground">
        {labels.map((y, i) => {
          const { year, era } = fromSignedYear(y)
          const withEra = i === 0 || i === labels.length - 1 || era === "AD"
          return <span key={i}>{withEra ? `${year} ${era}` : year}</span>
        })}
      </div>
      <div className="mt-2 flex items-center gap-2 text-xs">
        <YearInput value={from} onChange={(v) => onChange(v, to)} placeholder="509" aria-label="from year" />
        <span className="text-muted-foreground">to</span>
        <YearInput value={to} onChange={(v) => onChange(from, v)} placeholder="31" aria-label="to year" />
        {(from !== null || to !== null) && (
          <button
            onClick={() => onChange(null, null)}
            className="text-xs text-muted-foreground underline hover:text-foreground"
          >
            clear
          </button>
        )}
      </div>
    </div>
  )
}
```

Simplify the label row if the ternary chain reads poorly: first label always gets a " BC" suffix, the rest render bare years, last label "31 BC" — the requirement is: at least the first and last labels carry era text, no minus signs anywhere.

- [ ] **Step 2: Check** — `cd site && vp check` → green (component not yet mounted; that's Task 12).

- [ ] **Step 3: Commit**

```bash
git add site/src/components/era-timeline.tsx
git commit -m "feat: add attestation-density era timeline component"
```

---

### Task 11: Combobox component

**Files:**
- Create: `site/src/components/facet-combobox.tsx`

**Interfaces:**
- Produces:

```tsx
interface FacetComboboxProps {
  label: string
  values: FacetValue[]           // full candidate list with counts
  selected: string[]
  onChange: (selected: string[]) => void
  placeholder?: string
}
export function FacetCombobox(props: FacetComboboxProps): JSX.Element
```

  A text input filtering `values` (case-insensitive substring, top 8 shown with counts); clicking a suggestion adds it to `selected` (chips with × above the input); no external deps.

- [ ] **Step 1: Implement**

`site/src/components/facet-combobox.tsx`:

```tsx
// site/src/components/facet-combobox.tsx
import { useState } from "react"
import { X } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import type { FacetValue } from "@/data/types"

interface FacetComboboxProps {
  label: string
  values: FacetValue[]
  selected: string[]
  onChange: (selected: string[]) => void
  placeholder?: string
}

export function FacetCombobox({
  label,
  values,
  selected,
  onChange,
  placeholder,
}: FacetComboboxProps) {
  const [query, setQuery] = useState("")
  const suggestions = query.trim()
    ? values
        .filter(
          (v) =>
            !selected.includes(v.value) &&
            v.value.toLowerCase().includes(query.trim().toLowerCase())
        )
        .slice(0, 8)
    : []

  return (
    <div className="space-y-1">
      <p className="text-xs font-medium">{label}</p>
      {selected.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {selected.map((s) => (
            <Badge
              key={s}
              variant="secondary"
              className="cursor-pointer gap-1 text-xs"
              onClick={() => onChange(selected.filter((v) => v !== s))}
            >
              {s}
              <X className="h-3 w-3" />
            </Badge>
          ))}
        </div>
      )}
      <Input
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={placeholder ?? `Search ${label.toLowerCase()}…`}
        className="h-7 text-xs"
      />
      {suggestions.length > 0 && (
        <ul className="rounded-md border bg-background text-xs shadow-sm">
          {suggestions.map((v) => (
            <li key={v.value}>
              <button
                className="flex w-full items-baseline justify-between px-2 py-1 text-left hover:bg-accent"
                onClick={() => {
                  onChange([...selected, v.value])
                  setQuery("")
                }}
              >
                <span className="min-w-0 truncate">{v.value}</span>
                <span className="ml-2 text-muted-foreground">{v.count}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
```

- [ ] **Step 2: Check and commit**

```bash
cd site && vp check
git add site/src/components/facet-combobox.tsx
git commit -m "feat: add facet combobox component"
```

---

### Task 12: Tiered sidebar restructure

**Files:**
- Modify: `site/src/components/facet-sidebar.tsx` (full restructure)
- Create: `site/src/components/advanced-search.tsx`
- Modify: `site/src/routes/index.tsx` (new props)

**Interfaces:**
- `FacetSidebar` props become:

```tsx
interface FacetSidebarProps {
  facets: {
    office: FacetValue[]; nomen: FacetValue[]; sex: FacetValue[]
    tribe: FacetValue[]; province: FacetValue[]; event: FacetValue[]
    praenomen: FacetValue[]; cognomen: FacetValue[]
  }
  histogram: Histogram
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
  /** Facet to force-open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "time" | "gens"
}
```

- Structure: **Tier 1**: Office (`FacetHierarchyGroup`, `defaultOpen` true when `initialFocus === "office"`, else false), "Time period" section wrapping `EraTimeline`. **Tier 2** "More filters" (a `Collapsible`, auto-open when any of nomen/tribe/province/sex/patrician/nobilis/event params are active or `initialFocus === "gens"`): Gens, Tribe, **Location** (province facet, relabeled), Sex, Status, Life events (`FacetGroup title="Life events"` items=facets.event). **Tier 3** "Advanced search" (`Collapsible`, auto-open when officeMode!=="any" || officeInRange || praenomen/cognomen/re active): renders `<AdvancedSearch />`.
- `AdvancedSearch` props: `{ facets, state, onUpdate }` — office combobox (`FacetCombobox label="Office" values={facets.office} selected={state.office} onChange={(office) => onUpdate({ office })}`), praenomen/cognomen comboboxes, RE text input (`state.re`), and two toggles:

```tsx
      <label className="flex cursor-pointer items-center gap-2 text-xs">
        <Checkbox
          checked={state.officeMode === "all"}
          onCheckedChange={(c) => onUpdate({ officeMode: c ? "all" : "any" })}
        />
        <span>Require every selected office (AND)</span>
      </label>
      <label className="flex cursor-pointer items-center gap-2 text-xs">
        <Checkbox
          checked={state.officeInRange}
          onCheckedChange={(c) => onUpdate({ officeInRange: c === true })}
        />
        <span>Apply time period to offices (held in range)</span>
      </label>
```

- [ ] **Step 1: Implement** the restructure. Auto-open logic per tier:

```tsx
  const tier2Active =
    state.nomen.length > 0 || state.tribe.length > 0 ||
    state.province.length > 0 || state.sex.length > 0 ||
    state.patrician !== null || state.nobilis !== null ||
    state.event.length > 0
  const tier3Active =
    state.officeMode !== "any" || state.officeInRange ||
    state.praenomen.length > 0 || state.cognomen.length > 0 || state.re !== ""
```

Use `useState(() => tier2Active || initialFocus === "gens")` style initializers (auto-open on load, user-controlled after). Move the existing Gens/Tribe/Sex/Status groups into Tier 2 unchanged apart from the Location retitle (`title="Location"` on the province `FacetHierarchyGroup`). The old `FacetRangeGroup` usage is deleted (the component file stays — `git rm` NOT required; leave it, Task 18's verification confirms no orphan imports; actually DELETE `site/src/components/facet-range-group.tsx` once nothing imports it). Timeline wiring: `<EraTimeline histogram={histogram} from={state.eraFrom} to={state.eraTo} onChange={(eraFrom, eraTo) => onUpdate({ eraFrom, eraTo })} />` under a "Time period" heading.

`index.tsx`: pass `histogram={filteredHistogram}`, `facets`, hierarchies, and `initialFocus` (plumbed properly in Task 14; pass `undefined` for now).

- [ ] **Step 2: Verify in dev** — timeline renders with the curve and drags both handles updating URL + results; Location label shows; Tier 2/3 collapse and auto-open from a deep link like `/?officeMode=all&office=consul,censor`. `vp test src && vp check` green.

- [ ] **Step 3: Commit**

```bash
git add site/src/components site/src/routes/index.tsx
git commit -m "feat: restructure facet sidebar into disclosure tiers with timeline and advanced search"
```

---

### Task 13: Active filter chips for new state

**Files:**
- Modify: `site/src/components/active-filter-chips.tsx`

**Interfaces:** chips added for: `event` ("Event: exiled"), `praenomen` ("Praenomen: Lucius"), `cognomen` ("Cognomen: Brutus"), `re` ("RE: 46a" — removal sets `re: ""`), `officeMode` ("Offices: all of" — removal resets to "any"), `officeInRange` ("Offices in time range" — removal sets false); the province chip label changes from "Province:" to **"Location:"**.

- [ ] **Step 1: Implement** — follow the existing per-facet loop pattern exactly; scalar chips:

```tsx
  if (state.officeMode === "all") {
    chips.push({
      label: "Offices: all of",
      onRemove: () => onRemove({ officeMode: "any" }),
    })
  }
  if (state.officeInRange) {
    chips.push({
      label: "Offices in time range",
      onRemove: () => onRemove({ officeInRange: false }),
    })
  }
  if (state.re) {
    chips.push({ label: `RE: ${state.re}`, onRemove: () => onRemove({ re: "" }) })
  }
```

- [ ] **Step 2: Verify in dev** (chips appear/remove for each), `vp check`, commit:

```bash
git add site/src/components/active-filter-chips.tsx
git commit -m "feat: add filter chips for advanced search state"
```

---

### Task 14: Two-state landing

**Files:**
- Create: `site/src/components/search-landing.tsx`
- Modify: `site/src/routes/index.tsx`

**Interfaces:**
- `SearchLanding` props: `{ onSearch: (q: string) => void; onBrowse: (focus: "office" | "time" | "gens") => void }`.
- `index.tsx` behavior: landing renders when `Object.keys(toSearchParams(state)).length === 0` AND the user hasn't interacted this session (`useState` flag). Landing does NOT call `useSearchData(true)` — the fetch enables only when leaving landing (`useSearchData(!showLanding)`). `onBrowse(focus)` sets the interacted flag and `initialFocus`; `onSearch(q)` sets the flag and `updateState({ q })` — but since `useSearchState` needs the bundle, buffer the first query in local state and apply it once the bundle arrives.

- [ ] **Step 1: Implement the landing component**

`site/src/components/search-landing.tsx`:

```tsx
// site/src/components/search-landing.tsx
import { useState } from "react"
import { Input } from "@/components/ui/input"

interface SearchLandingProps {
  onSearch: (q: string) => void
  onBrowse: (focus: "office" | "time" | "gens") => void
}

const cards = [
  { key: "office" as const, title: "Office", blurb: "Consuls, praetors, priesthoods — browse the hierarchy of Roman offices" },
  { key: "time" as const, title: "Time", blurb: "Sweep across five centuries of attested careers on a timeline" },
  { key: "gens" as const, title: "Gens", blurb: "Explore families — Cornelii, Iunii, Claudii, and 700 more" },
]

export function SearchLanding({ onSearch, onBrowse }: SearchLandingProps) {
  const [q, setQ] = useState("")
  return (
    <div className="mx-auto max-w-2xl px-4 py-16 text-center">
      <h1 className="font-heading text-3xl font-bold">
        Digital Prosopography of the Roman Republic
      </h1>
      <p className="mt-2 text-muted-foreground">
        4,876 persons of the Roman Republic, 509–31 BC — offices, families,
        dates, and sources
      </p>
      <Input
        autoFocus
        value={q}
        onChange={(e) => {
          setQ(e.target.value)
          if (e.target.value.trim()) onSearch(e.target.value)
        }}
        placeholder="Search 4,876 persons…"
        className="mx-auto mt-6 h-11 max-w-md text-base"
      />
      <div className="mt-8 grid grid-cols-1 gap-3 sm:grid-cols-3">
        {cards.map((c) => (
          <button
            key={c.key}
            onClick={() => onBrowse(c.key)}
            className="rounded-lg border p-4 text-left transition-colors hover:bg-accent"
          >
            <p className="font-heading font-semibold">Browse by {c.title}</p>
            <p className="mt-1 text-xs text-muted-foreground">{c.blurb}</p>
          </button>
        ))}
      </div>
    </div>
  )
}
```

- [ ] **Step 2: Wire the two states in `index.tsx`**

```tsx
function SearchPage() {
  const rawParams = Route.useSearch() as Record<string, string>
  const hasParams = Object.keys(toSearchParams(parseSearchParams(rawParams))).length > 0
  const [interacted, setInteracted] = useState(false)
  const [initialFocus, setInitialFocus] = useState<"office" | "time" | "gens" | undefined>()
  const [pendingQuery, setPendingQuery] = useState<string | null>(null)
  const showLanding = !hasParams && !interacted
  const { bundle, error } = useSearchData(!showLanding)

  if (showLanding) {
    return (
      <SearchLanding
        onSearch={(q) => { setPendingQuery(q); setInteracted(true) }}
        onBrowse={(focus) => { setInitialFocus(focus); setInteracted(true) }}
      />
    )
  }
  // …error/loading states from Task 5, then:
  return <SearchResults bundle={bundle} initialFocus={initialFocus} pendingQuery={pendingQuery} onPendingApplied={() => setPendingQuery(null)} />
}
```

`SearchResults` (extracted inner component so hooks order stays legal) runs `useSearchState(bundle)`, applies `pendingQuery` once via `useEffect` (`updateState({ q: pendingQuery })` then `onPendingApplied()`), and renders the full layout, passing `initialFocus` to the sidebar. `initialFocus === "time"` also renders the sidebar with the timeline scrolled into view — acceptable minimum: Tier 1 is always visible, so no special handling beyond office/gens opening their groups.

Keep the hidden-crawl-links block rendering in BOTH states (it's `aria-hidden`; the prerendered page is the landing state and must keep the links until Task 15 removes them).

- [ ] **Step 3: Verify in dev** — clean `/` shows landing (no data fetch in the Network tab until interaction); typing transitions and applies the query; each card opens its facet; deep link `/?office=consul` skips landing. `vp check && vp test src` green. `vp build` and confirm `dist/client/index.html` contains the landing markup.

- [ ] **Step 4: Commit**

```bash
git add site/src/components/search-landing.tsx site/src/routes/index.tsx
git commit -m "feat: add two-state landing with browse-by entry cards"
```

---

### Task 15: /directory/ crawl page + footer; drop hidden links

**Files:**
- Create: `site/src/routes/directory.tsx`
- Create: `site/src/components/site-footer.tsx`
- Modify: `site/src/routes/__root.tsx` (mount footer)
- Modify: `site/src/routes/index.tsx` (delete hidden links + `getAllPersonIds` loader)

**Interfaces:**
- `/directory/` — prerendered page listing links to every person page plus the three reference indexes; loader = existing `getAllPersonIds()`. Title "Directory — DPRR", meta description "Complete index of all person records".
- `SiteFooter` — rendered in the root layout under the outlet on every page: a muted single line with a `Link to="/directory"` labeled "Directory", plus a data-attribution line.

- [ ] **Step 1: Implement**

`site/src/routes/directory.tsx`:

```tsx
// site/src/routes/directory.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getAllPersonIds } from "@/server/data"

export const Route = createFileRoute("/directory")({
  loader: () => getAllPersonIds(),
  head: () => ({
    meta: [
      { title: "Directory — DPRR" },
      { name: "description", content: "Complete index of all person records" },
    ],
  }),
  component: DirectoryPage,
})

function DirectoryPage() {
  const ids = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-6xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">Directory</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        Complete index of {ids.length.toLocaleString()} person records —{" "}
        <Link to="/offices" className="underline">offices</Link>,{" "}
        <Link to="/tribes" className="underline">tribes</Link>, and{" "}
        <Link to="/provinces" className="underline">locations</Link> have their own indexes.
      </p>
      <ul className="mt-6 columns-3 gap-4 text-xs sm:columns-5 lg:columns-8">
        {ids.map((id) => (
          <li key={id}>
            <Link to="/persons/$id" params={{ id }} className="hover:underline">
              {id}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
```

`site/src/components/site-footer.tsx`:

```tsx
// site/src/components/site-footer.tsx
import { Link } from "@tanstack/react-router"

export function SiteFooter() {
  return (
    <footer className="mt-12 border-t">
      <div className="mx-auto flex max-w-6xl items-baseline justify-between px-4 py-4 text-xs text-muted-foreground">
        <span>
          Data: Digital Prosopography of the Roman Republic (King&apos;s College
          London)
        </span>
        <Link to="/directory" className="hover:text-foreground hover:underline">
          Directory
        </Link>
      </div>
    </footer>
  )
}
```

`__root.tsx` `RootLayout`: `<SiteHeader /><Outlet /><SiteFooter />`.

`index.tsx`: delete the hidden-links `<div className="hidden">…` block, the `getAllPersonIds` loader and import.

- [ ] **Step 2: Verify the crawl stays complete**

`cd site && vp build 2>&1 | tail -3` then:

```bash
find dist/client/persons -name index.html | wc -l   # must still be ~4876
ls dist/client/directory/                            # index.html exists
grep -c 'href="/dprr-data/persons/' dist/client/directory/index.html  # ~4876
grep -c 'href="/dprr-data/persons/' dist/client/index.html            # 0 (links gone)
```

- [ ] **Step 3: Check, test, commit**

```bash
cd site && vp check && vp test src
git add site/src/routes/directory.tsx site/src/components/site-footer.tsx site/src/routes/__root.tsx site/src/routes/index.tsx site/src/routeTree.gen.ts
git commit -m "feat: add directory crawl page and site footer, drop hidden search-page links"
```

---

### Task 16: Fasti result rows + sort control

**Files:**
- Create: `site/src/components/fasti-row.tsx`
- Modify: `site/src/components/results-list.tsx`

**Interfaces:**
- `FastiRow` props: `{ person: PersonSummary }`. Line 1: display name, then `({reNumber})` when present, then `— {highestOffice}` when present. Line 2 (muted, smaller): filiation · era range (`EraRange`) · gens (`nomen`). The whole row links to the person page.
- `ResultsList` props become `{ results: PersonSummary[]; sort: SearchState["sort"]; hasQuery: boolean; onSortChange: (s: SearchState["sort"]) => void }` — renders the count, a sort `<select>` (Earliest first / Latest first / Name A–Z, plus Relevance option only when `hasQuery`), and `FastiRow`s. (`PersonCard` remains for tribe pages — do not modify it.)

- [ ] **Step 1: Implement**

`site/src/components/fasti-row.tsx`:

```tsx
// site/src/components/fasti-row.tsx
import { Link } from "@tanstack/react-router"
import { EraRange } from "@/components/date-display"
import type { PersonSummary } from "@/data/types"

export function FastiRow({ person }: { person: PersonSummary }) {
  const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="block border-b px-1 py-2 transition-colors hover:bg-accent"
    >
      <p className="font-heading text-sm font-medium">
        {displayName}
        {person.reNumber && (
          <span className="ml-1 font-normal text-muted-foreground">
            ({person.reNumber})
          </span>
        )}
        {person.highestOffice && (
          <span className="ml-2 font-normal">— {person.highestOffice}</span>
        )}
      </p>
      <p className="text-xs text-muted-foreground">
        {person.filiation && <span>{person.filiation} · </span>}
        <EraRange from={person.eraFrom} to={person.eraTo} />
        {person.nomen && <span> · gens {person.nomen}</span>}
      </p>
    </Link>
  )
}
```

`results-list.tsx` — replace the card list with `FastiRow`s and add the control:

```tsx
      <div className="flex items-baseline justify-between">
        <p className="text-sm text-muted-foreground">
          {results.length.toLocaleString()} result{results.length === 1 ? "" : "s"}
        </p>
        <label className="text-xs text-muted-foreground">
          Sort{" "}
          <select
            value={sort ?? (hasQuery ? "relevance" : "earliest")}
            onChange={(e) => onSortChange(e.target.value as SearchState["sort"])}
            className="rounded-md border bg-transparent px-1 py-0.5"
          >
            <option value="earliest">Earliest first</option>
            <option value="latest">Latest first</option>
            <option value="name">Name A–Z</option>
            {hasQuery && <option value="relevance">Relevance</option>}
          </select>
        </label>
      </div>
```

`index.tsx`/`SearchResults`: `<ResultsList results={results} sort={state.sort} hasQuery={state.q.trim().length > 0} onSortChange={(sort) => updateState({ sort })} />`. Keep the existing empty-state rendering in results-list if present; otherwise: when `results.length === 0`, render `<p className="py-8 text-center text-sm text-muted-foreground">No persons match — try removing a filter.</p>`.

- [ ] **Step 2: Verify in dev** (rows show RE/filiation/office; default order is earliest-first; control switches; Relevance appears only with a query). `vp check && vp test src` green.

- [ ] **Step 3: Commit**

```bash
git add site/src/components/fasti-row.tsx site/src/components/results-list.tsx site/src/routes/index.tsx
git commit -m "feat: fasti-style result rows with earliest-first default sort"
```

---

### Task 17: Person page — career main + identity rail

**Files:**
- Create: `site/src/components/person-rail.tsx`
- Modify: `site/src/routes/persons.$id.tsx`

**Interfaces:**
- `PersonRail` props: `{ person: Person }` — renders four compact rail cards: **Identity** (praenomen, nomen, cognomen, filiation, RE, tribes as Links, DPRR ID, status badges), **Family** (relationships grouped by type: types alphabetical, `PersonLink`s by display name within each type), **Dates** (DateInformation sorted by `value` ascending, existing `DateEntry` markup moved in), **External links** (concordances, systems alphabetical).
- Route layout: header (name + one strong line: `EraRange` · highestOffice · badges inline), then `lg:grid lg:grid-cols-[1fr_280px] lg:gap-8`: main = Career section (existing `OfficeEntry` list — per-assertion scholarly notes now wrapped in a `Collapsible` defaulting closed with a "{n} notes" trigger) + person Notes section (sorted by note type); rail = `<PersonRail />` inside `lg:sticky lg:top-4 lg:self-start`. Mobile (below `lg`): Identity card renders directly under the header (before career), then career/notes, then the remaining rail sections — achieve by rendering `PersonRail` with a `variant: "stacked" | "rail"` prop OR simply rendering the grid in DOM order rail-last and using `order` utilities; the simple accepted approach: two-column grid where the rail div appears second in DOM; on mobile everything stacks with the rail after main, EXCEPT Identity which is duplicated as a `lg:hidden` block under the header. Implement `PersonRail` subcomponents (`IdentityCard`, `FamilyCard`, `DatesCard`, `LinksCard`) as named exports so the route can compose them.

- Ordering (binding, from the spec): relationships grouped by type alphabetically, people by display name within; dates by year ascending; notes by type; concordance systems alphabetical.

- [ ] **Step 1: Implement `person-rail.tsx`** — move `RelationshipEntry`, `DateEntry`, `ConcordanceList` logic from the route file into the card components (kept visually compact: `text-xs`/`text-sm`), applying the sorts:

```tsx
// grouping helper inside person-rail.tsx
function groupRelationships(rels: Relationship[]): [string, Relationship[]][] {
  const byType = new Map<string, Relationship[]>()
  for (const r of rels) {
    const list = byType.get(r.relationshipType) ?? []
    list.push(r)
    byType.set(r.relationshipType, list)
  }
  const strip = (s: string) => s.replace(/^[A-Z]{4}\d+ /, "")
  return [...byType]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([type, list]) => [
      type,
      [...list].sort((a, b) =>
        strip(a.relatedPersonName).localeCompare(strip(b.relatedPersonName))
      ),
    ])
}
```

Dates: `[...person.dateInformation].sort((a, b) => a.value - b.value)`. Notes (main column, stays in route): `[...person.personNotes].sort((a, b) => a.type.localeCompare(b.type))`. Concordances: sort the grouped systems `[...grouped].sort((a, b) => a[0].localeCompare(b[0]))`.

- [ ] **Step 2: Restructure the route** — header becomes:

```tsx
      <header className="mb-6">
        <h1 className="font-heading text-3xl font-bold">{displayName}</h1>
        <p className="mt-1 text-lg text-muted-foreground">
          <EraRange from={person.eraFrom} to={person.eraTo} />
          {person.highestOffice && <span> · {person.highestOffice}</span>}
          {person.isPatrician && <Badge variant="secondary" className="ml-2 align-middle">Patrician</Badge>}
          {person.isNobilis && <Badge variant="secondary" className="ml-1 align-middle">Nobilis</Badge>}
        </p>
      </header>
```

Career notes collapse: wrap the existing `assertion.notes.map(...)` block in the route's `OfficeEntry`:

```tsx
      {assertion.notes.length > 0 && (
        <Collapsible>
          <CollapsibleTrigger className="mt-1 text-xs text-muted-foreground hover:underline">
            {assertion.notes.length} scholarly note{assertion.notes.length === 1 ? "" : "s"} ▸
          </CollapsibleTrigger>
          <CollapsibleContent>
            {/* existing notes markup unchanged */}
          </CollapsibleContent>
        </Collapsible>
      )}
```

- [ ] **Step 3: Verify in dev** on a rich person (e.g. `/persons/LUCR0010` for uncertainty, plus one with relationships/concordances like `/persons/IUNI0001`): rail sticks on desktop; mobile stacks Identity → Career → rest; notes expand; all orders deterministic. `vp check && vp test src` green.

- [ ] **Step 4: Commit**

```bash
git add site/src/components/person-rail.tsx 'site/src/routes/persons.$id.tsx'
git commit -m "feat: restructure person page as career main with identity rail"
```

---

### Task 18: Location relabel (routes + nav)

**Files:**
- Modify: `site/src/routes/provinces.index.tsx`, `site/src/routes/provinces.$slug.tsx` (headings, titles, descriptions)
- Modify: `site/src/components/site-header.tsx` (nav label)

- [ ] **Step 1: Implement** — `provinces.index.tsx`: h1 "Locations", title "Locations — DPRR", description "Locations — provinces, courts, and spheres of responsibility with recorded office holders". `provinces.$slug.tsx`: title `` `${province.name} — Locations — DPRR` ``, description wording "…in {name}" unchanged otherwise. `site-header.tsx`: nav label "Locations" (the `to` path stays `/provinces`). Chips/sidebar labels were already relabeled in Tasks 12–13 — verify with `grep -rn '"Province' site/src` → no user-facing "Province" strings remain (the `province` state/param names are NOT user-facing and stay).

- [ ] **Step 2: Verify, check, commit**

```bash
cd site && vp check && vp test src
git add site/src/routes/provinces.index.tsx 'site/src/routes/provinces.$slug.tsx' site/src/components/site-header.tsx
git commit -m "feat: relabel province pages and nav as Locations"
```

---

### Task 19: Full verification

**Files:** none — verification only; fix-forward trivial breakage, report anything else.

- [ ] **Step 1: Suites** — `cd site && vp check && vp test` → fully green.

- [ ] **Step 2: Build + inventory**

```bash
vp build 2>&1 | tail -5
find dist/client/persons -name index.html | wc -l          # ~4876
ls dist/client/directory/ dist/client/data/                # directory page + both JSONs
wc -c dist/client/index.html                               # LANDING SIZE BUDGET: < 100 KB (expect ~15–40 KB)
wc -c dist/client/data/search-data.json dist/client/data/search-index.json  # record sizes
```

- [ ] **Step 3: Serve under `/dprr-data/` prefix** (same symlink+`python3 -m http.server` technique as Plan 1's Task 19 — serve the parent dir with a `dprr-data` symlink to `dist/client`): status-200 loop over `/dprr-data/`, `/dprr-data/directory/`, `/dprr-data/persons/IUNI0001/`, `/dprr-data/offices/consul/`, `/dprr-data/provinces/`, plus `curl` the landing HTML and confirm it contains the three "Browse by" cards and does NOT contain `search-data.json` content (no embedded summaries).

- [ ] **Step 4: Interactive spot-checks** (dev server + browser if available; otherwise document as manual checklist in the report):
  1. Landing → type "brutus" → results appear, Relevance default sort.
  2. `/?office=consul%2Ccensor&officeMode=all` → far fewer results than the same URL without `officeMode` (OR mode).
  3. Timeline drag narrows results; with `officeInRange=true` + office selection, counts differ from person-level mode.
  4. Subtree: selecting "Priesthoods" in the office tree matches augurs/pontifices.
  5. Life events facet filters; chips remove cleanly; every advanced param round-trips through a full page reload (deep-link).
  6. Person page rail sticks; career notes expand; mobile viewport stacks correctly.
- [ ] **Step 5: Report** — sizes, counts, any deviations. Confirm clean `git status`. Do NOT merge to `main`.

---

## Execution Notes

- **Task order is linear** (1→19); Tasks 9/10/11 (year input, timeline, combobox) are mutually independent leaf components and may be parallelized after Task 8; Tasks 16/17/18 are mutually independent after 15.
- Tasks 5, 12, 14, 15 each leave the site fully working — verify the dev server after each before moving on.
- The spec (`docs/superpowers/specs/2026-08-07-ux-pass-design.md`) is the authority on semantics; this plan is the authority on file layout and signatures.
