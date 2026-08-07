# Site Rollout Plan 1: Features & Deploy Readiness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the static site deployable to `https://gillisandrew.github.io/dprr-data/` and add the province facet (via curated mapping), reference pages (offices/tribes/provinces), and JSON-LD structured data.

**Architecture:** Build-time TTL→JSON pipeline (N3.js parsers in `site/src/data/`) feeds TanStack Start SSG with full prerendering. New province data comes from free-text `hasProvinceOriginal` strings on post assertions, normalized through a curated, checked-in mapping to canonical names from `reference/provinces.ttl`. Reference pages aggregate the parsed person data through pure functions in a new `aggregate-references.ts` module, exposed via server functions and prerendered like person pages.

**Tech Stack:** TanStack Start + Router (file-based routes), Vite+ (`vp`) toolchain, N3.js, MiniSearch, Tailwind 4 + shadcn/ui, tests via `vp test` (Vitest bundled with vite-plus).

## Global Constraints

- All site commands run from `site/`: `vp install`, `vp check`, `vp test`, `vp build`. NEVER use pnpm/npm/npx/vitest directly (see `site/CLAUDE.md`).
- Test imports come from `vite-plus/test`: `import { expect, test, describe } from "vite-plus/test"`.
- The deployed base path is `/dprr-data/` (GitHub project page). The public site URL is `https://gillisandrew.github.io/dprr-data`.
- Code style: no semicolons, double quotes, 2-space indent (enforced by `vp fmt`). Run `vp check` before every commit.
- Do NOT merge to `main` — all work lands on `feature/static-site`. Launch happens after Plan 2 (UX pass).
- The repo data root is two levels above `site/src/data/` at runtime (`join(process.cwd(), "..")` — see `loader.ts`).
- Display names strip the DPRR ID prefix with `name.replace(/^[A-Z]{4}\d+ /, "")` — reuse this existing convention.

---

### Task 1: Commit pending formatting changes

The working tree has formatting-only changes in 8 files from a `vp fmt` run (Tailwind class reordering, line joining). Get to a clean tree before feature work.

**Files:**
- Modify: none (commit existing changes)

- [ ] **Step 1: Verify the diff is formatting-only**

Run: `git -C /Users/gillisandrew/Projects/gillisandrew/dprr-data diff`
Expected: only class-string reordering and line re-wrapping in `site/src/components/*.tsx`, `site/src/lib/search.ts`, `site/src/routes/persons.$id.tsx`. No logic changes. If you see logic changes, STOP and ask.

- [ ] **Step 2: Validate**

Run: `cd site && vp check`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add site/src
git commit -m "style: apply vp fmt formatting"
```

Note: `.gitignore` and `.superpowers/` may appear untracked — leave them alone.

---

### Task 2: Base path configuration for GitHub project page

The site deploys to `gillisandrew.github.io/dprr-data/` but is built assuming root. Set the Vite `base`, fix the raw-anchor crawler links, and fix the 404 page link. The TanStack Start plugin propagates Vite's `base` to the router basepath automatically (via `TSS_ROUTER_BASEPATH`) — `Link` components need no changes.

**Files:**
- Modify: `site/vite.config.ts`
- Modify: `site/src/routes/index.tsx` (hidden crawler links)
- Modify: `site/public/404.html`

**Interfaces:**
- Produces: `import.meta.env.BASE_URL === "/dprr-data/"` available everywhere client-side.

- [ ] **Step 1: Add base to vite config**

In `site/vite.config.ts`, add `base` as the first property of the `defineConfig` object:

```ts
const config = defineConfig({
  base: "/dprr-data/",
  lint: { options: { typeAware: true, typeCheck: true } },
  ...
```

- [ ] **Step 2: Prefix the hidden crawler links**

In `site/src/routes/index.tsx`, the hidden prerender-crawler links use raw anchors. Change:

```tsx
        {summaries.map((p) => (
          <a key={p.id} href={`/persons/${p.id}`}>
            {p.id}
          </a>
        ))}
```

to:

```tsx
        {summaries.map((p) => (
          <a key={p.id} href={`${import.meta.env.BASE_URL}persons/${p.id}`}>
            {p.id}
          </a>
        ))}
```

(`BASE_URL` ends with `/`, so no separator slash.)

- [ ] **Step 3: Fix the 404 page link**

In `site/public/404.html`, change `<a href="/">Back to search</a>` to `<a href="/dprr-data/">Back to search</a>`.

- [ ] **Step 4: Build and verify page count**

Run: `cd site && vp build 2>&1 | tail -20`
Then: `find site/dist/client/persons -name index.html | wc -l`
Expected: ~4,876 person pages (must not be 0 or 1).

**If the count collapsed:** the prerender crawler is resolving the `/dprr-data/`-prefixed hrefs incorrectly. Fallback: revert Step 2 so the hidden links stay unprefixed (`/persons/${p.id}`) — they are `aria-hidden` crawler fodder, not user-facing navigation — rebuild, and confirm the count recovers. Record which variant you shipped in the commit message.

- [ ] **Step 5: Serve under the prefix and smoke-check**

```bash
mkdir -p /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/pages
ln -sfn "$(pwd)/site/dist/client" /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/pages/dprr-data
python3 -m http.server 8722 -d /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/pages &
sleep 1
curl -s http://localhost:8722/dprr-data/ | grep -c '/dprr-data/assets/'
curl -s http://localhost:8722/dprr-data/persons/IUNI0001/ | grep -o '<title>[^<]*</title>'
kill %1
```

Expected: asset count > 0 (all asset URLs carry the prefix); the person page title renders. If assets reference `/assets/...` without the prefix, the base config didn't take — stop and investigate before proceeding.

- [ ] **Step 6: Commit**

```bash
git add site/vite.config.ts site/src/routes/index.tsx site/public/404.html
git commit -m "feat: configure /dprr-data/ base path for GitHub project page"
```

---

### Task 3: Parse provinces into ReferenceMaps

Add `reference/provinces.ttl` to the reference parsing pipeline so canonical province names are available.

**Files:**
- Modify: `site/src/data/types.ts` (ReferenceMaps)
- Modify: `site/src/data/parse-references.ts`
- Modify: `site/src/data/loader.ts` (read + pass the file)
- Test: `site/src/data/parse-references.test.ts`
- Modify: `site/src/data/parse-persons.test.ts` (fixture gains the new required field)

**Interfaces:**
- Produces: `ReferenceMaps.provinces: Map<string, { name: string; parent: string | null }>` keyed by full province URI (e.g. `http://romanrepublic.ac.uk/rdf/entity/Province/9`).

- [ ] **Step 1: Write the failing test**

Add to `site/src/data/parse-references.test.ts` (match the existing fixture style — `parseReferenceTtl` takes named TTL strings):

```ts
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

describe("provinces", () => {
  test("parses province names and parents, skipping nameless entries", async () => {
    const refs = await parseReferenceTtl({ ...emptyInputs, provinces: PROVINCE_TTL })
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
```

Adapt `emptyInputs` to however the existing tests construct the input object (if they pass all fields explicitly, add `provinces: ""` there and `provinces: PROVINCE_TTL` here). Every existing `parseReferenceTtl` call in tests needs the new `provinces` key.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/data/parse-references.test.ts`
Expected: FAIL — type error / `provinces` undefined.

- [ ] **Step 3: Implement**

In `site/src/data/types.ts`, add to `ReferenceMaps`:

```ts
  provinces: Map<string, { name: string; parent: string | null }>
```

In `site/src/data/parse-references.ts`:
- Add `provinces: string` to `RawTtlInputs`.
- Declare `const provinces = new Map<string, { name: string; parent: string | null }>()` alongside the other maps.
- Add a parsing block modeled exactly on the Offices block:

```ts
  // Provinces
  if (inputs.provinces) {
    for (const [uri, props] of groupBySubject(parseTtl(inputs.provinces))) {
      const name = props.get(`${DPRR}hasName`)
      if (name) {
        provinces.set(uri, {
          name,
          parent: props.get(`${DPRR}hasParent`) ?? null,
        })
      }
    }
  }
```

- Add `provinces` to the returned object.

In `site/src/data/loader.ts`, extend the reference-file read:

```ts
  const [offices, sources, praenomina, tribes, relationships, misc, provinces] =
    await Promise.all([
      readTtl("reference/offices.ttl"),
      readTtl("reference/sources.ttl"),
      readTtl("reference/praenomina.ttl"),
      readTtl("reference/tribes.ttl"),
      readTtl("reference/relationships.ttl"),
      readTtl("reference/misc.ttl"),
      readTtl("reference/provinces.ttl"),
    ])
```

and pass `provinces` into `parseReferenceTtl({ ... , provinces })`.

Fix `site/src/data/parse-persons.test.ts` fixtures: wherever a `ReferenceMaps` literal is built, add `provinces: new Map()`.

- [ ] **Step 4: Run tests and check**

Run: `cd site && vp test src/data && vp check`
Expected: all PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/data
git commit -m "feat: parse reference provinces into lookup map"
```

---

### Task 4: Curated province mapping module

The person TTL export has only free-text `hasProvinceOriginal` / `hasProvinceOriginalExpanded` strings. Create the checked-in curated mapping from those strings to canonical province names, with a validation test against `reference/provinces.ttl`. **The mapping file is a user-review artifact — flag it for Andrew's review in the task report.**

**Files:**
- Create: `site/src/data/province-mapping.ts`
- Test: `site/src/data/province-mapping.test.ts`

**Interfaces:**
- Produces: `PROVINCE_MAPPING: Record<string, string[]>` (raw string → canonical province names), `mapProvinceText(raw: string): string[] | null` (null = unmapped), `collectUnmappedProvinces(rawValues: Iterable<string>): string[]` (sorted distinct unmapped values).

- [ ] **Step 1: Extract the distinct raw strings**

```bash
grep -rhoE 'dprr:hasProvinceOriginal(Expanded)? "[^"]*"' persons \
  | sed -E 's/dprr:hasProvinceOriginal(Expanded)? "(.*)"/\2/' \
  | sort | uniq -c | sort -rn > /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/province-strings.txt
wc -l /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/province-strings.txt
```

Also extract the canonical names:

```bash
grep -oE 'dprr:hasName "[^"]*"' reference/provinces.ttl | sed -E 's/dprr:hasName "(.*)"/\1/' | sort -u > /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/province-canonical.txt
```

- [ ] **Step 2: Write the failing validation test**

`site/src/data/province-mapping.test.ts`:

```ts
import { readFileSync } from "node:fs"
import { join } from "node:path"
import { expect, test, describe } from "vite-plus/test"
import {
  PROVINCE_MAPPING,
  mapProvinceText,
  collectUnmappedProvinces,
} from "./province-mapping"

function canonicalNames(): Set<string> {
  const ttl = readFileSync(
    join(process.cwd(), "..", "reference", "provinces.ttl"),
    "utf-8"
  )
  const names = new Set<string>()
  for (const m of ttl.matchAll(/dprr:hasName "([^"]+)"/g)) {
    names.add(m[1])
  }
  return names
}

describe("province mapping", () => {
  test("every mapping target is a canonical province name", () => {
    const canonical = canonicalNames()
    for (const [raw, targets] of Object.entries(PROVINCE_MAPPING)) {
      expect(targets.length, `"${raw}" maps to nothing`).toBeGreaterThan(0)
      for (const t of targets) {
        expect(canonical.has(t), `"${raw}" → "${t}" not canonical`).toBe(true)
      }
    }
  })

  test("known variants resolve", () => {
    expect(mapProvinceText("Sicily")).toEqual(["Sicilia"])
    expect(mapProvinceText("Sicilia")).toEqual(["Sicilia"])
    expect(mapProvinceText("Greece and Asia")).toEqual(["Greece", "Asia"])
  })

  test("unmapped strings return null and are collected", () => {
    expect(mapProvinceText("definitely-not-a-province")).toBeNull()
    expect(
      collectUnmappedProvinces(["Sicilia", "definitely-not-a-province"])
    ).toEqual(["definitely-not-a-province"])
  })

  test("high-frequency strings are covered", () => {
    for (const raw of ["Rome", "Asia", "Macedonia", "Sicilia", "Africa", "Syria", "Cilicia", "Hispania Ulterior", "Hispania Citerior"]) {
      expect(mapProvinceText(raw), `"${raw}" should be mapped`).not.toBeNull()
    }
  })
})
```

(If "Greece" or "Asia" are not canonical names in `province-canonical.txt`, adjust the "Greece and Asia" expectation to whatever the canonical equivalents are — the test asserts curation decisions, so set expectations from the actual canonical list.)

- [ ] **Step 3: Run test to verify it fails**

Run: `cd site && vp test src/data/province-mapping.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 4: Author the mapping**

Create `site/src/data/province-mapping.ts`:

```ts
// site/src/data/province-mapping.ts
//
// Curated mapping from free-text hasProvinceOriginal(Expanded) strings in
// the person TTL export to canonical province names in
// reference/provinces.ttl. The export has no structured province links,
// so this file IS the province resolution — every entry is a curation
// decision. Strings absent from this map are surfaced as build warnings
// and excluded from the province facet; their raw text still renders on
// person pages.

export const PROVINCE_MAPPING: Record<string, string[]> = {
  // Identity entries (raw string === canonical name) and variants.
  // Populate from scratchpad/province-strings.txt against
  // scratchpad/province-canonical.txt using these rules:
  //   1. Exact match to a canonical name → identity entry.
  //   2. Case/spelling variants (Sicily/Sicilia, Spain/Hispania) → canonical.
  //   3. Conjunctions ("Greece and Asia") → both canonical names.
  //   4. Qualified strings ("Hispania Citerior?") → the unqualified canonical.
  //   5. Genuinely ambiguous or non-geographic strings you cannot confidently
  //      assign ("Fleet", "Italy?") → LEAVE OUT (they become warnings).
  // Aim to cover at least every string with ≥5 occurrences.
  Sicilia: ["Sicilia"],
  Sicily: ["Sicilia"],
  // ... (all remaining curated entries)
}

export function mapProvinceText(raw: string): string[] | null {
  return PROVINCE_MAPPING[raw.trim()] ?? null
}

export function collectUnmappedProvinces(
  rawValues: Iterable<string>
): string[] {
  const unmapped = new Set<string>()
  for (const raw of rawValues) {
    if (raw && mapProvinceText(raw) === null) unmapped.add(raw)
  }
  return [...unmapped].sort()
}
```

Fill in the full entry set per the rules in the comment — this is deliberate curation work over the ~185 strings in `province-strings.txt`, not mechanical generation. Delete the placeholder `// ... ` comment when done.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd site && vp test src/data/province-mapping.test.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add site/src/data/province-mapping.ts site/src/data/province-mapping.test.ts
git commit -m "feat: add curated province mapping with validation tests"
```

In your task report, list roughly how many strings you mapped vs. left unmapped, with the top unmapped examples — the user reviews this file.

---

### Task 5: Extract provinces in the person parser

Post assertions gain the raw province text and its canonical resolution; person summaries gain the aggregated province list for faceting.

**Files:**
- Modify: `site/src/data/types.ts` (`PostAssertion`, `PersonSummary`)
- Modify: `site/src/data/parse-persons.ts`
- Modify: `site/src/data/loader.ts` (`toSummaries`)
- Test: `site/src/data/parse-persons.test.ts`

**Interfaces:**
- Consumes: `mapProvinceText(raw: string): string[] | null` from Task 4.
- Produces: `PostAssertion.provinceOriginal: string | null`, `PostAssertion.provinces: string[]` (canonical), `PersonSummary.provinces: string[]` (distinct canonical across all assertions), included by `toSummaries`.

- [ ] **Step 1: Write the failing test**

Add to `site/src/data/parse-persons.test.ts`, following the existing fixture style (a person TTL with a post assertion; refs built the same way neighboring tests do):

```ts
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
    const persons = parsePersonTtl(ttl, emptyRefs(), new Map())
    expect(persons).toHaveLength(1)
    const byOriginal = Object.fromEntries(
      persons[0].postAssertions.map((pa) => [pa.provinceOriginal, pa])
    )
    expect(byOriginal["Sicily"].provinces).toEqual(["Sicilia"])
    expect(byOriginal["not-a-real-province"].provinces).toEqual([])
    expect(persons[0].provinces).toEqual(["Sicilia"])
  })
})
```

(`emptyRefs()` = however the existing tests build an empty/minimal `ReferenceMaps`; reuse or add such a helper including `provinces: new Map()`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/data/parse-persons.test.ts`
Expected: FAIL — `provinceOriginal`/`provinces` do not exist.

- [ ] **Step 3: Implement**

`site/src/data/types.ts` — add to `PostAssertion`:

```ts
  /** Raw province text from the secondary source (may be unmapped). */
  provinceOriginal: string | null
  /** Canonical province names resolved via the curated mapping. */
  provinces: string[]
```

Add to `PersonSummary`:

```ts
  /** Distinct canonical provinces across all post assertions (for faceting). */
  provinces: string[]
```

`site/src/data/parse-persons.ts`:
- Import: `import { mapProvinceText } from "./province-mapping"`
- In `buildPostAssertions`, before `results.push`, compute:

```ts
      const provinceOriginal =
        first(g, "hasProvinceOriginal") ?? first(g, "hasProvinceOriginalExpanded")
      const provinceExpanded = first(g, "hasProvinceOriginalExpanded")
      const provinces = [
        ...new Set(
          [provinceOriginal, provinceExpanded]
            .filter((v): v is string => v !== null)
            .flatMap((v) => mapProvinceText(v) ?? [])
        ),
      ]
```

and include `provinceOriginal, provinces` in the pushed object.
- In the person-building loop, next to the existing `officeNames` aggregation:

```ts
    const provinceNames = [
      ...new Set(postAssertions.flatMap((pa) => pa.provinces)),
    ]
```

and add `provinces: provinceNames,` to the pushed person (a `Person` extends `PersonSummary`, so the summary field lives on the person).

`site/src/data/loader.ts` — add `provinces: p.provinces,` to the object returned by `toSummaries`.

- [ ] **Step 4: Run tests and check**

Run: `cd site && vp test src/data && vp check`
Expected: PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/data
git commit -m "feat: extract and canonicalize provinces on post assertions"
```

---

### Task 6: Loader warnings for unmapped province strings

Surface curation gaps at build time without failing the build.

**Files:**
- Modify: `site/src/data/loader.ts`

**Interfaces:**
- Consumes: `collectUnmappedProvinces` from Task 4 (already tested there).

- [ ] **Step 1: Implement the warning**

In `loadAllData()` in `site/src/data/loader.ts`, after the second-pass relationship resolution and before `_cache = ...`:

```ts
  // Surface province curation gaps: raw strings not in the curated mapping
  // are excluded from the facet but still displayed on person pages.
  const rawProvinceTexts = persons.flatMap((p) =>
    p.postAssertions
      .map((pa) => pa.provinceOriginal)
      .filter((v): v is string => v !== null)
  )
  const unmapped = collectUnmappedProvinces(rawProvinceTexts)
  if (unmapped.length > 0) {
    console.warn(
      `[data] ${unmapped.length} unmapped province strings (excluded from facet):`,
      unmapped.join("; ")
    )
  }
```

Import `collectUnmappedProvinces` from `./province-mapping`.

- [ ] **Step 2: Verify against real data**

Run: `cd site && vp build 2>&1 | grep -A1 "unmapped province" ; vp check`
Expected: build succeeds; the warning line lists the strings you intentionally left uncurated in Task 4 (or does not appear if coverage is total). Sanity-check that nothing high-frequency (Rome, Asia, Macedonia…) appears.

- [ ] **Step 3: Commit**

```bash
git add site/src/data/loader.ts
git commit -m "feat: warn on unmapped province strings at build time"
```

---

### Task 7: Province in search state

Wire the province facet through URL params, filtering, and facet counts. Also export the previously-private param helpers and give them tests.

**Files:**
- Modify: `site/src/data/types.ts` (`SearchState`)
- Modify: `site/src/lib/search.ts`
- Test: `site/src/lib/search.test.ts` (new)

**Interfaces:**
- Produces: `SearchState.province: string[]`; URL param `province` (comma-separated); `useSearchState(...).facets.province: FacetValue[]`. Exports `parseSearchParams` and `toSearchParams` from `@/lib/search`.

- [ ] **Step 1: Write the failing test**

Create `site/src/lib/search.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import { parseSearchParams, toSearchParams } from "./search"

describe("search param round-trip", () => {
  test("province parses from comma-separated param", () => {
    const state = parseSearchParams({ province: "Sicilia,Asia" })
    expect(state.province).toEqual(["Sicilia", "Asia"])
  })

  test("province serializes back to the URL", () => {
    const state = parseSearchParams({})
    expect(state.province).toEqual([])
    const params = toSearchParams({ ...state, province: ["Sicilia"] })
    expect(params.province).toBe("Sicilia")
  })

  test("full round-trip preserves all facets", () => {
    const input = {
      q: "brutus",
      office: "consul,praetor",
      province: "Sicilia",
      tribe: "Fabia",
      sex: "Male",
      patrician: "true",
      eraFrom: "-200",
      eraTo: "-100",
    }
    expect(toSearchParams(parseSearchParams(input))).toEqual({
      ...input,
      nomen: undefined,
      nobilis: undefined,
    })
  })
})
```

(The last assertion relies on `toSearchParams` omitting empty keys — `toEqual` treats missing and `undefined` keys the same.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/lib/search.test.ts`
Expected: FAIL — `parseSearchParams` not exported / `province` missing.

- [ ] **Step 3: Implement**

`site/src/data/types.ts` — add to `SearchState`:

```ts
  province: string[]
```

`site/src/lib/search.ts`:
- Add `export` to `parseSearchParams` and `toSearchParams`.
- `parseSearchParams`: add `province: params.province ? params.province.split(",") : [],`
- `toSearchParams`: add `if (state.province.length) params.province = state.province.join(",")`
- `matchesFacets`: add (after the office check, same pattern):

```ts
  if (
    state.province.length > 0 &&
    !state.province.some((pr) => person.provinces.includes(pr))
  )
    return false
```

- In the `facets` memo return: add `province: countWith("province", "provinces"),`

- [ ] **Step 4: Run tests and check**

Run: `cd site && vp test src/lib && vp check`
Expected: PASS. (`vp check` will flag the `FacetSidebar` facets prop type in the next task's files only if you change them now — don't; the hook's return is structurally wider, which is fine.)

- [ ] **Step 5: Commit**

```bash
git add site/src/data/types.ts site/src/lib/search.ts site/src/lib/search.test.ts
git commit -m "feat: add province to search state and facet counts"
```

---

### Task 8: Province facet UI

Sidebar group + filter chips.

**Files:**
- Modify: `site/src/components/facet-sidebar.tsx`
- Modify: `site/src/components/active-filter-chips.tsx`

**Interfaces:**
- Consumes: `facets.province`, `state.province` from Task 7.

- [ ] **Step 1: Add the sidebar group**

In `site/src/components/facet-sidebar.tsx`, add `province: FacetValue[]` to the `facets` field of `FacetSidebarProps`, and render after the Tribe group (secondary, collapsed, searchable — the list has dozens of values):

```tsx
      <FacetGroup
        title="Province"
        items={facets.province}
        selected={state.province}
        onChange={(province) => onUpdate({ province })}
        defaultOpen={false}
        searchable
      />
```

- [ ] **Step 2: Add chips**

In `site/src/components/active-filter-chips.tsx`, after the tribe loop:

```tsx
  for (const province of state.province) {
    chips.push({
      label: `Province: ${province}`,
      onRemove: () =>
        onRemove({ province: state.province.filter((p) => p !== province) }),
    })
  }
```

- [ ] **Step 3: Verify in the dev server**

Run: `cd site && vp dev --port 3000` (background), then open `http://localhost:3000/?province=Sicilia` — expect filtered results, an active chip "Province: Sicilia", and a Province group in the sidebar with counts. Kill the dev server. (Dev server runs at root base in dev mode; if the base path applies in dev, use `http://localhost:3000/dprr-data/?province=Sicilia`.)

- [ ] **Step 4: Check and commit**

Run: `cd site && vp check`

```bash
git add site/src/components/facet-sidebar.tsx site/src/components/active-filter-chips.tsx
git commit -m "feat: add province facet UI"
```

---

### Task 9: Slug utility and reference aggregation module

Pure, tested aggregation functions that reference pages consume. Slugs are derived from display names; a collision is a build-stopping error.

**Files:**
- Create: `site/src/lib/slug.ts`
- Test: `site/src/lib/slug.test.ts`
- Create: `site/src/data/aggregate-references.ts`
- Test: `site/src/data/aggregate-references.test.ts`

**Interfaces:**
- Produces (from `@/lib/slug`): `slugify(name: string): string`
- Produces (from `@/data/aggregate-references`):

```ts
export interface OfficeIndexEntry { slug: string; name: string; abbreviation: string | null; holderCount: number }
export interface OfficeHolder { personId: string; personName: string; dateStart: number | null; dateEnd: number | null; secondarySource: string }
export interface OfficeDetail { slug: string; name: string; abbreviation: string | null; holders: OfficeHolder[] }
export interface TribeIndexEntry { slug: string; name: string; memberCount: number }
export interface TribeDetail { slug: string; name: string; members: PersonSummary[] }
export interface ProvinceIndexEntry { slug: string; name: string; assertionCount: number }
export interface ProvinceAssertion { personId: string; personName: string; officeName: string; dateStart: number | null; dateEnd: number | null }
export interface ProvinceDetail { slug: string; name: string; assertions: ProvinceAssertion[] }
export function buildOfficeIndex(persons: Person[]): OfficeIndexEntry[]
export function buildOfficeDetail(persons: Person[], slug: string): OfficeDetail | null
export function buildTribeIndex(persons: Person[]): TribeIndexEntry[]
export function buildTribeDetail(persons: Person[], slug: string): TribeDetail | null
export function buildProvinceIndex(persons: Person[]): ProvinceIndexEntry[]
export function buildProvinceDetail(persons: Person[], slug: string): ProvinceDetail | null
```

Index functions sort alphabetically by name and include only entries with ≥1 holder/member/assertion. Detail holders/assertions sort chronologically by `dateStart ?? dateEnd ?? Infinity` ascending, ties by personName. Tribe members sort by name. Duplicate slugs from distinct names → `throw new Error(...)`.

- [ ] **Step 1: Write the failing slug test**

`site/src/lib/slug.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import { slugify } from "./slug"

describe("slugify", () => {
  test("lowercases and hyphenates", () => {
    expect(slugify("triumvir capitalis")).toBe("triumvir-capitalis")
  })
  test("strips punctuation and collapses runs", () => {
    expect(slugify("quaestio (de veneficiis)")).toBe("quaestio-de-veneficiis")
  })
  test("trims leading/trailing separators", () => {
    expect(slugify("  consul  ")).toBe("consul")
  })
})
```

Run: `cd site && vp test src/lib/slug.test.ts` — expect FAIL (module not found).

- [ ] **Step 2: Implement slugify**

`site/src/lib/slug.ts`:

```ts
// site/src/lib/slug.ts

/** URL slug from a display name: lowercase, ASCII-folded, hyphen-separated. */
export function slugify(name: string): string {
  return name
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
}
```

Run: `cd site && vp test src/lib/slug.test.ts` — expect PASS.

- [ ] **Step 3: Write the failing aggregation tests**

`site/src/data/aggregate-references.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import {
  buildOfficeIndex,
  buildOfficeDetail,
  buildTribeIndex,
  buildTribeDetail,
  buildProvinceIndex,
  buildProvinceDetail,
} from "./aggregate-references"
import type { Person, PostAssertion } from "./types"

function makeAssertion(over: Partial<PostAssertion>): PostAssertion {
  return {
    id: "pa1",
    officeName: "",
    officeAbbreviation: null,
    dateStart: null,
    dateEnd: null,
    dateSecondarySource: null,
    originalText: null,
    secondarySource: "Broughton MRR",
    notes: [],
    primarySourceRefs: [],
    provinceOriginal: null,
    provinces: [],
    ...over,
  }
}

function makePerson(over: Partial<Person>): Person {
  return {
    id: "TEST0001",
    uri: "http://example.org/1",
    name: "TEST0001 T. Testius",
    praenomen: "Titus",
    nomen: "Testius",
    cognomen: null,
    otherNames: null,
    filiation: null,
    reNumber: null,
    sex: "Male",
    isPatrician: false,
    isNobilis: false,
    nobilisNotes: null,
    highestOffice: null,
    eraFrom: null,
    eraTo: null,
    tribe: null,
    offices: [],
    provinces: [],
    postAssertions: [],
    relationships: [],
    dateInformation: [],
    personNotes: [],
    concordances: [],
    ...over,
  }
}

const consul100 = makeAssertion({
  id: "pa-consul-100",
  officeName: "consul",
  officeAbbreviation: "cos.",
  dateStart: -100,
  dateEnd: -100,
  provinceOriginal: "Sicily",
  provinces: ["Sicilia"],
})
const consul90 = makeAssertion({
  id: "pa-consul-90",
  officeName: "consul",
  officeAbbreviation: "cos.",
  dateStart: -90,
  dateEnd: -90,
})

const personA = makePerson({
  id: "AAAA0001",
  name: "AAAA0001 A. Aulus",
  tribe: "Fabia",
  offices: ["consul"],
  provinces: ["Sicilia"],
  postAssertions: [consul100],
})
const personB = makePerson({
  id: "BBBB0001",
  name: "BBBB0001 B. Brutus",
  offices: ["consul"],
  postAssertions: [consul90],
})

describe("offices", () => {
  test("index lists offices alphabetically with distinct-person counts", () => {
    const index = buildOfficeIndex([personA, personB])
    expect(index).toEqual([
      { slug: "consul", name: "consul", abbreviation: "cos.", holderCount: 2 },
    ])
  })

  test("detail sorts holders chronologically", () => {
    const detail = buildOfficeDetail([personB, personA], "consul")
    expect(detail?.holders.map((h) => h.personId)).toEqual([
      "AAAA0001",
      "BBBB0001",
    ])
    expect(detail?.holders[0].dateStart).toBe(-100)
  })

  test("unknown slug returns null", () => {
    expect(buildOfficeDetail([personA], "praetor")).toBeNull()
  })
})

describe("tribes", () => {
  test("index and detail", () => {
    expect(buildTribeIndex([personA, personB])).toEqual([
      { slug: "fabia", name: "Fabia", memberCount: 1 },
    ])
    const detail = buildTribeDetail([personA, personB], "fabia")
    expect(detail?.members.map((m) => m.id)).toEqual(["AAAA0001"])
    // members are summaries — no heavy fields
    expect(detail?.members[0]).not.toHaveProperty("postAssertions")
  })
})

describe("provinces", () => {
  test("index counts assertions and detail lists them chronologically", () => {
    expect(buildProvinceIndex([personA, personB])).toEqual([
      { slug: "sicilia", name: "Sicilia", assertionCount: 1 },
    ])
    const detail = buildProvinceDetail([personA, personB], "sicilia")
    expect(detail?.assertions).toEqual([
      {
        personId: "AAAA0001",
        personName: "AAAA0001 A. Aulus",
        officeName: "consul",
        dateStart: -100,
        dateEnd: -100,
      },
    ])
  })
})
```

Run: `cd site && vp test src/data/aggregate-references.test.ts` — expect FAIL (module not found).

- [ ] **Step 4: Implement the aggregation module**

`site/src/data/aggregate-references.ts`:

```ts
// site/src/data/aggregate-references.ts
import { slugify } from "../lib/slug"
import { toSummaries } from "./loader"
import type { Person, PersonSummary } from "./types"

export interface OfficeIndexEntry {
  slug: string
  name: string
  abbreviation: string | null
  holderCount: number
}
export interface OfficeHolder {
  personId: string
  personName: string
  dateStart: number | null
  dateEnd: number | null
  secondarySource: string
}
export interface OfficeDetail {
  slug: string
  name: string
  abbreviation: string | null
  holders: OfficeHolder[]
}
export interface TribeIndexEntry {
  slug: string
  name: string
  memberCount: number
}
export interface TribeDetail {
  slug: string
  name: string
  members: PersonSummary[]
}
export interface ProvinceIndexEntry {
  slug: string
  name: string
  assertionCount: number
}
export interface ProvinceAssertion {
  personId: string
  personName: string
  officeName: string
  dateStart: number | null
  dateEnd: number | null
}
export interface ProvinceDetail {
  slug: string
  name: string
  assertions: ProvinceAssertion[]
}

/** Chronological sort key: earliest known date, undated entries last. */
function dateKey(dateStart: number | null, dateEnd: number | null): number {
  return dateStart ?? dateEnd ?? Number.POSITIVE_INFINITY
}

function assertUniqueSlugs(names: Iterable<string>, kind: string): void {
  const seen = new Map<string, string>()
  for (const name of names) {
    const slug = slugify(name)
    const prior = seen.get(slug)
    if (prior !== undefined && prior !== name) {
      throw new Error(
        `${kind} slug collision: "${prior}" and "${name}" both → "${slug}"`
      )
    }
    seen.set(slug, name)
  }
}

export function buildOfficeIndex(persons: Person[]): OfficeIndexEntry[] {
  const byName = new Map<
    string,
    { abbreviation: string | null; holderIds: Set<string> }
  >()
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      if (!pa.officeName) continue
      let entry = byName.get(pa.officeName)
      if (!entry) {
        entry = { abbreviation: pa.officeAbbreviation, holderIds: new Set() }
        byName.set(pa.officeName, entry)
      }
      entry.abbreviation ??= pa.officeAbbreviation
      entry.holderIds.add(p.id)
    }
  }
  assertUniqueSlugs(byName.keys(), "Office")
  return [...byName]
    .map(([name, { abbreviation, holderIds }]) => ({
      slug: slugify(name),
      name,
      abbreviation,
      holderCount: holderIds.size,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildOfficeDetail(
  persons: Person[],
  slug: string
): OfficeDetail | null {
  let officeName: string | null = null
  let abbreviation: string | null = null
  const holders: OfficeHolder[] = []
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      if (!pa.officeName || slugify(pa.officeName) !== slug) continue
      officeName = pa.officeName
      abbreviation ??= pa.officeAbbreviation
      holders.push({
        personId: p.id,
        personName: p.name,
        dateStart: pa.dateStart,
        dateEnd: pa.dateEnd,
        secondarySource: pa.secondarySource,
      })
    }
  }
  if (officeName === null) return null
  holders.sort(
    (a, b) =>
      dateKey(a.dateStart, a.dateEnd) - dateKey(b.dateStart, b.dateEnd) ||
      a.personName.localeCompare(b.personName)
  )
  return { slug, name: officeName, abbreviation, holders }
}

export function buildTribeIndex(persons: Person[]): TribeIndexEntry[] {
  const byName = new Map<string, number>()
  for (const p of persons) {
    if (p.tribe) byName.set(p.tribe, (byName.get(p.tribe) ?? 0) + 1)
  }
  assertUniqueSlugs(byName.keys(), "Tribe")
  return [...byName]
    .map(([name, memberCount]) => ({ slug: slugify(name), name, memberCount }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildTribeDetail(
  persons: Person[],
  slug: string
): TribeDetail | null {
  const matching = persons.filter((p) => p.tribe && slugify(p.tribe) === slug)
  if (matching.length === 0) return null
  const members = toSummaries(matching).sort((a, b) =>
    a.name.localeCompare(b.name)
  )
  return { slug, name: matching[0].tribe as string, members }
}

export function buildProvinceIndex(persons: Person[]): ProvinceIndexEntry[] {
  const byName = new Map<string, number>()
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      for (const province of pa.provinces) {
        byName.set(province, (byName.get(province) ?? 0) + 1)
      }
    }
  }
  assertUniqueSlugs(byName.keys(), "Province")
  return [...byName]
    .map(([name, assertionCount]) => ({
      slug: slugify(name),
      name,
      assertionCount,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

export function buildProvinceDetail(
  persons: Person[],
  slug: string
): ProvinceDetail | null {
  let provinceName: string | null = null
  const assertions: ProvinceAssertion[] = []
  for (const p of persons) {
    for (const pa of p.postAssertions) {
      const match = pa.provinces.find((pr) => slugify(pr) === slug)
      if (!match) continue
      provinceName = match
      assertions.push({
        personId: p.id,
        personName: p.name,
        officeName: pa.officeName,
        dateStart: pa.dateStart,
        dateEnd: pa.dateEnd,
      })
    }
  }
  if (provinceName === null) return null
  assertions.sort(
    (a, b) =>
      dateKey(a.dateStart, a.dateEnd) - dateKey(b.dateStart, b.dateEnd) ||
      a.personName.localeCompare(b.personName)
  )
  return { slug, name: provinceName, assertions }
}
```

- [ ] **Step 5: Run tests and check**

Run: `cd site && vp test src/data src/lib && vp check`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add site/src/lib/slug.ts site/src/lib/slug.test.ts site/src/data/aggregate-references.ts site/src/data/aggregate-references.test.ts
git commit -m "feat: add slug utility and reference page aggregations"
```

---

### Task 10: Server functions for reference pages

**Files:**
- Modify: `site/src/server/data.ts`

**Interfaces:**
- Consumes: aggregation functions from Task 9, `loadAllData` from the loader.
- Produces server functions (all `method: "GET"`): `getOfficeIndex()`, `getOfficeDetail({ data: slug })`, `getTribeIndex()`, `getTribeDetail({ data: slug })`, `getProvinceIndex()`, `getProvinceDetail({ data: slug })`. Detail functions throw `Error("<Kind> not found: <slug>")` for unknown slugs (matching `getPersonById`'s pattern).

- [ ] **Step 1: Implement**

Append to `site/src/server/data.ts`:

```ts
import {
  buildOfficeIndex,
  buildOfficeDetail,
  buildTribeIndex,
  buildTribeDetail,
  buildProvinceIndex,
  buildProvinceDetail,
} from "../data/aggregate-references"

export const getOfficeIndex = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return buildOfficeIndex(persons)
  }
)

export const getOfficeDetail = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const { persons } = await loadAllData()
    const detail = buildOfficeDetail(persons, slug)
    if (!detail) throw new Error(`Office not found: ${slug}`)
    return detail
  })

export const getTribeIndex = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return buildTribeIndex(persons)
  }
)

export const getTribeDetail = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const { persons } = await loadAllData()
    const detail = buildTribeDetail(persons, slug)
    if (!detail) throw new Error(`Tribe not found: ${slug}`)
    return detail
  })

export const getProvinceIndex = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return buildProvinceIndex(persons)
  }
)

export const getProvinceDetail = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const { persons } = await loadAllData()
    const detail = buildProvinceDetail(persons, slug)
    if (!detail) throw new Error(`Province not found: ${slug}`)
    return detail
  })
```

(Move the import up with the other imports.)

- [ ] **Step 2: Check and commit**

Run: `cd site && vp check`

```bash
git add site/src/server/data.ts
git commit -m "feat: add server functions for reference pages"
```

---

### Task 11: Site header navigation

A persistent header so users (and the prerender crawler) can reach the new index pages from anywhere.

**Files:**
- Create: `site/src/components/site-header.tsx`
- Modify: `site/src/routes/__root.tsx`

**Interfaces:**
- Produces: `<SiteHeader />`; root route gains a `component` that renders it above the route outlet.

- [ ] **Step 1: Create the header component**

`site/src/components/site-header.tsx`:

```tsx
// site/src/components/site-header.tsx
import { Link } from "@tanstack/react-router"

const links = [
  { to: "/offices", label: "Offices" },
  { to: "/tribes", label: "Tribes" },
  { to: "/provinces", label: "Provinces" },
] as const

export function SiteHeader() {
  return (
    <header className="border-b">
      <nav className="mx-auto flex max-w-6xl items-baseline gap-6 px-4 py-3">
        <Link to="/" className="font-heading font-bold">
          DPRR
        </Link>
        {links.map((l) => (
          <Link
            key={l.to}
            to={l.to}
            className="text-sm text-muted-foreground hover:text-foreground"
            activeProps={{ className: "text-sm font-medium text-foreground" }}
          >
            {l.label}
          </Link>
        ))}
      </nav>
    </header>
  )
}
```

Note: the `to` values reference routes created in Tasks 12–14. If implementing this task before those routes exist, TypeScript will reject the route paths — in that case implement Tasks 12–14 first, or temporarily use `to="/"` placeholders and finish this task last. Preferred order: do this task after Task 14 if executing sequentially; the plan lists it here because Tasks 12–14 reference `<SiteHeader />` conceptually but do not import it.

- [ ] **Step 2: Mount in the root route**

In `site/src/routes/__root.tsx`, add imports:

```tsx
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/react-router"
import { SiteHeader } from "@/components/site-header"
```

Add a layout component and register it:

```tsx
export const Route = createRootRoute({
  head: () => ({ ... }),        // unchanged
  shellComponent: RootDocument, // unchanged
  component: RootLayout,
})

function RootLayout() {
  return (
    <>
      <SiteHeader />
      <Outlet />
    </>
  )
}
```

- [ ] **Step 3: Check and commit**

Run: `cd site && vp check && vp test src`

```bash
git add site/src/components/site-header.tsx site/src/routes/__root.tsx
git commit -m "feat: add site header navigation"
```

---

### Task 12: Office pages

**Files:**
- Create: `site/src/routes/offices.index.tsx`
- Create: `site/src/routes/offices.$slug.tsx`

**Interfaces:**
- Consumes: `getOfficeIndex`, `getOfficeDetail` (Task 10); `Section` from `@/components/section` (props: `title: string`, `count?: number`, children); `DateDisplay`/`EraRange` from `@/components/date-display`; `PersonLink` from `@/components/person-card` (props `id`, `name`); `SourceCitation` from `@/components/source-citation` (props `name`, `className?`).

- [ ] **Step 1: Create the index route**

`site/src/routes/offices.index.tsx`:

```tsx
// site/src/routes/offices.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getOfficeIndex } from "@/server/data"

export const Route = createFileRoute("/offices/")({
  loader: () => getOfficeIndex(),
  head: () => ({
    meta: [
      { title: "Offices — DPRR" },
      {
        name: "description",
        content:
          "Offices and priesthoods of the Roman Republic, with all known holders",
      },
    ],
  }),
  component: OfficesPage,
})

function OfficesPage() {
  const offices = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">Offices</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {offices.length} offices and priesthoods
      </p>
      <ul className="mt-6 space-y-1">
        {offices.map((o) => (
          <li key={o.slug}>
            <Link
              to="/offices/$slug"
              params={{ slug: o.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
                {o.name}
                {o.abbreviation && (
                  <span className="ml-1 text-sm font-normal text-muted-foreground">
                    ({o.abbreviation})
                  </span>
                )}
              </span>
              <span className="text-sm text-muted-foreground">
                {o.holderCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
```

- [ ] **Step 2: Create the detail route**

`site/src/routes/offices.$slug.tsx`:

```tsx
// site/src/routes/offices.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getOfficeDetail } from "@/server/data"
import { DateDisplay, EraRange } from "@/components/date-display"
import { PersonLink } from "@/components/person-card"
import { SourceCitation } from "@/components/source-citation"

export const Route = createFileRoute("/offices/$slug")({
  loader: ({ params }) => getOfficeDetail({ data: params.slug }),
  head: ({ loaderData: office }) => {
    if (!office) return {}
    const title = `${office.name} — Offices — DPRR`
    const desc = `${office.holders.length} recorded holders of the office of ${office.name} in the Roman Republic`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: OfficePage,
})

function OfficePage() {
  const office = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">
        {office.name}
        {office.abbreviation && (
          <span className="ml-2 text-xl font-normal text-muted-foreground">
            ({office.abbreviation})
          </span>
        )}
      </h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {office.holders.length} recorded holders, listed chronologically
      </p>
      <ol className="mt-6 space-y-2">
        {office.holders.map((h, i) => (
          <li
            key={`${h.personId}-${i}`}
            className="flex flex-wrap items-baseline gap-x-3 border-l-2 pl-4"
          >
            <span className="min-w-24 text-sm text-muted-foreground tabular-nums">
              {h.dateStart !== null && h.dateEnd !== null ? (
                h.dateStart === h.dateEnd ? (
                  <DateDisplay year={h.dateStart} />
                ) : (
                  <EraRange from={h.dateStart} to={h.dateEnd} />
                )
              ) : h.dateStart !== null || h.dateEnd !== null ? (
                <DateDisplay year={(h.dateStart ?? h.dateEnd) as number} />
              ) : (
                "undated"
              )}
            </span>
            <PersonLink id={h.personId} name={h.personName} />
            <SourceCitation
              name={h.secondarySource}
              className="text-xs text-muted-foreground"
            />
          </li>
        ))}
      </ol>
    </div>
  )
}
```

If `DateDisplay`/`EraRange`/`SourceCitation` props differ from what's shown (check the actual component files), match their real signatures — `persons.$id.tsx` uses them exactly this way.

- [ ] **Step 3: Verify in dev, check, commit**

Run: `cd site && vp check`, then `vp dev --port 3000` and load `/offices/` and `/offices/consul` — expect the index list and a chronological holder list. Kill the server.

```bash
git add site/src/routes/offices.index.tsx 'site/src/routes/offices.$slug.tsx'
git commit -m "feat: add office index and detail pages"
```

(routeTree.gen.ts regenerates on dev/build — commit it if it changed: `git add site/src/routeTree.gen.ts`.)

---

### Task 13: Tribe pages

**Files:**
- Create: `site/src/routes/tribes.index.tsx`
- Create: `site/src/routes/tribes.$slug.tsx`

**Interfaces:**
- Consumes: `getTribeIndex`, `getTribeDetail` (Task 10); `PersonCard` from `@/components/person-card` (prop `person: PersonSummary`).

- [ ] **Step 1: Create the index route**

`site/src/routes/tribes.index.tsx` — identical shape to `offices.index.tsx`:

```tsx
// site/src/routes/tribes.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getTribeIndex } from "@/server/data"

export const Route = createFileRoute("/tribes/")({
  loader: () => getTribeIndex(),
  head: () => ({
    meta: [
      { title: "Tribes — DPRR" },
      {
        name: "description",
        content: "Voting tribes of the Roman Republic and their known members",
      },
    ],
  }),
  component: TribesPage,
})

function TribesPage() {
  const tribes = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">Tribes</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {tribes.length} voting tribes with known members
      </p>
      <ul className="mt-6 space-y-1">
        {tribes.map((t) => (
          <li key={t.slug}>
            <Link
              to="/tribes/$slug"
              params={{ slug: t.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
                {t.name}
              </span>
              <span className="text-sm text-muted-foreground">
                {t.memberCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
```

- [ ] **Step 2: Create the detail route**

`site/src/routes/tribes.$slug.tsx`:

```tsx
// site/src/routes/tribes.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getTribeDetail } from "@/server/data"
import { PersonCard } from "@/components/person-card"

export const Route = createFileRoute("/tribes/$slug")({
  loader: ({ params }) => getTribeDetail({ data: params.slug }),
  head: ({ loaderData: tribe }) => {
    if (!tribe) return {}
    const title = `${tribe.name} — Tribes — DPRR`
    const desc = `${tribe.members.length} known members of the tribus ${tribe.name}`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: TribePage,
})

function TribePage() {
  const tribe = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">{tribe.name}</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {tribe.members.length} known members
      </p>
      <div className="mt-6 space-y-2">
        {tribe.members.map((m) => (
          <PersonCard key={m.id} person={m} />
        ))}
      </div>
    </div>
  )
}
```

- [ ] **Step 3: Verify, check, commit**

Run: `cd site && vp check`; dev-server spot check `/tribes/` and one tribe page.

```bash
git add site/src/routes/tribes.index.tsx 'site/src/routes/tribes.$slug.tsx' site/src/routeTree.gen.ts
git commit -m "feat: add tribe index and detail pages"
```

---

### Task 14: Province pages

**Files:**
- Create: `site/src/routes/provinces.index.tsx`
- Create: `site/src/routes/provinces.$slug.tsx`

**Interfaces:**
- Consumes: `getProvinceIndex`, `getProvinceDetail` (Task 10).

- [ ] **Step 1: Create the index route**

`site/src/routes/provinces.index.tsx` — same shape as the other indexes:

```tsx
// site/src/routes/provinces.index.tsx
import { Link, createFileRoute } from "@tanstack/react-router"
import { getProvinceIndex } from "@/server/data"

export const Route = createFileRoute("/provinces/")({
  loader: () => getProvinceIndex(),
  head: () => ({
    meta: [
      { title: "Provinces — DPRR" },
      {
        name: "description",
        content:
          "Provinces and spheres of responsibility recorded for offices of the Roman Republic",
      },
    ],
  }),
  component: ProvincesPage,
})

function ProvincesPage() {
  const provinces = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">Provinces</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {provinces.length} provinces with recorded office holders
      </p>
      <ul className="mt-6 space-y-1">
        {provinces.map((p) => (
          <li key={p.slug}>
            <Link
              to="/provinces/$slug"
              params={{ slug: p.slug }}
              className="group flex items-baseline justify-between gap-2 rounded px-2 py-1.5 hover:bg-accent"
            >
              <span className="font-medium group-hover:underline">
                {p.name}
              </span>
              <span className="text-sm text-muted-foreground">
                {p.assertionCount}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
```

- [ ] **Step 2: Create the detail route**

`site/src/routes/provinces.$slug.tsx`:

```tsx
// site/src/routes/provinces.$slug.tsx
import { createFileRoute } from "@tanstack/react-router"
import { getProvinceDetail } from "@/server/data"
import { DateDisplay, EraRange } from "@/components/date-display"
import { PersonLink } from "@/components/person-card"

export const Route = createFileRoute("/provinces/$slug")({
  loader: ({ params }) => getProvinceDetail({ data: params.slug }),
  head: ({ loaderData: province }) => {
    if (!province) return {}
    const title = `${province.name} — Provinces — DPRR`
    const desc = `${province.assertions.length} recorded office holders in ${province.name}`
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: ProvincePage,
})

function ProvincePage() {
  const province = Route.useLoaderData()
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="font-heading text-3xl font-bold">{province.name}</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        {province.assertions.length} recorded office holders, listed
        chronologically
      </p>
      <ol className="mt-6 space-y-2">
        {province.assertions.map((a, i) => (
          <li
            key={`${a.personId}-${i}`}
            className="flex flex-wrap items-baseline gap-x-3 border-l-2 pl-4"
          >
            <span className="min-w-24 text-sm text-muted-foreground tabular-nums">
              {a.dateStart !== null && a.dateEnd !== null ? (
                a.dateStart === a.dateEnd ? (
                  <DateDisplay year={a.dateStart} />
                ) : (
                  <EraRange from={a.dateStart} to={a.dateEnd} />
                )
              ) : a.dateStart !== null || a.dateEnd !== null ? (
                <DateDisplay year={(a.dateStart ?? a.dateEnd) as number} />
              ) : (
                "undated"
              )}
            </span>
            <PersonLink id={a.personId} name={a.personName} />
            {a.officeName && (
              <span className="text-sm text-muted-foreground">
                {a.officeName}
              </span>
            )}
          </li>
        ))}
      </ol>
    </div>
  )
}
```

- [ ] **Step 3: Verify, check, commit**

Run: `cd site && vp check`; dev-server spot check `/provinces/` and `/provinces/sicilia`.

```bash
git add site/src/routes/provinces.index.tsx 'site/src/routes/provinces.$slug.tsx' site/src/routeTree.gen.ts
git commit -m "feat: add province index and detail pages"
```

---

### Task 15: Cross-links from person pages

Office names, tribes, and provinces on person pages become links to the new reference pages.

**Files:**
- Modify: `site/src/routes/persons.$id.tsx`

**Interfaces:**
- Consumes: `slugify` (Task 9), route paths from Tasks 12–14, `mapProvinceText` (Task 4).

- [ ] **Step 1: Link office names in OfficeEntry**

In `persons.$id.tsx`, add imports:

```tsx
import { Link } from "@tanstack/react-router"
import { slugify } from "@/lib/slug"
```

In `OfficeEntry`, replace the office-name paragraph content:

```tsx
      <p className="font-medium">
        {assertion.officeName ? (
          <Link
            to="/offices/$slug"
            params={{ slug: slugify(assertion.officeName) }}
            className="hover:underline"
          >
            {assertion.officeName}
          </Link>
        ) : (
          assertion.officeName
        )}
        {assertion.officeAbbreviation && (
          <span className="ml-1 text-sm text-muted-foreground">
            ({assertion.officeAbbreviation})
          </span>
        )}
      </p>
```

- [ ] **Step 2: Show and link provinces in OfficeEntry**

After the date paragraph in `OfficeEntry`, add:

```tsx
      {assertion.provinceOriginal && (
        <p className="text-sm text-muted-foreground">
          Province:{" "}
          {assertion.provinces.length > 0 ? (
            assertion.provinces.map((pr, i) => (
              <span key={pr}>
                {i > 0 && ", "}
                <Link
                  to="/provinces/$slug"
                  params={{ slug: slugify(pr) }}
                  className="hover:underline"
                >
                  {pr}
                </Link>
              </span>
            ))
          ) : (
            <span>{assertion.provinceOriginal}</span>
          )}
          {assertion.provinces.length > 0 &&
            assertion.provinces.join(", ") !== assertion.provinceOriginal && (
              <span className="italic"> ({assertion.provinceOriginal})</span>
            )}
        </p>
      )}
```

(Mapped: canonical links, with the raw source text in parentheses when it differs. Unmapped: raw text only.)

- [ ] **Step 3: Link the tribe in PersonHeader**

Replace the tribe `<dd>`:

```tsx
            <dd>
              <Link
                to="/tribes/$slug"
                params={{ slug: slugify(person.tribe) }}
                className="hover:underline"
              >
                {person.tribe}
              </Link>
            </dd>
```

- [ ] **Step 4: Verify, check, commit**

Run: `cd site && vp check`; dev-server: open a person with offices/tribe/province (e.g. a person whose TTL has `hasProvinceOriginal` — `grep -rl hasProvinceOriginal persons | head -1` gives a file; its ID is the filename). Confirm all three link types navigate.

```bash
git add 'site/src/routes/persons.$id.tsx'
git commit -m "feat: link offices, tribes, and provinces from person pages"
```

---

### Task 16: JSON-LD structured data

Schema.org `Person` on person pages (with `sameAs` from concordances — the main linked-data payoff) and `Dataset` on the search page. No birth/death dates: schema.org BC-date handling is unreliable; canonical dates live in the RDF.

**Files:**
- Create: `site/src/lib/site.ts`
- Modify: `site/src/routes/persons.$id.tsx` (head)
- Modify: `site/src/routes/index.tsx` (head)

**Interfaces:**
- Produces: `SITE_URL = "https://gillisandrew.github.io/dprr-data"` from `@/lib/site`.

- [ ] **Step 1: Create the site constant**

`site/src/lib/site.ts`:

```ts
// site/src/lib/site.ts

/** Canonical public URL of the deployed site (no trailing slash). */
export const SITE_URL = "https://gillisandrew.github.io/dprr-data"
```

- [ ] **Step 2: Person JSON-LD**

In `persons.$id.tsx`, import `SITE_URL` and replace the `head` function:

```tsx
  head: ({ loaderData: person }) => {
    if (!person) return {}
    const displayName = person.name.replace(/^[A-Z]{4}\d+ /, "")
    const desc = [person.highestOffice, person.isPatrician ? "Patrician" : null]
      .filter(Boolean)
      .join(" · ")
    const jsonLd = {
      "@context": "https://schema.org",
      "@type": "Person",
      name: displayName,
      ...(person.otherNames ? { alternateName: person.otherNames } : {}),
      gender: person.sex,
      ...(desc ? { description: desc } : {}),
      identifier: person.id,
      url: `${SITE_URL}/persons/${person.id}`,
      ...(person.concordances.length > 0
        ? { sameAs: person.concordances.map((c) => c.uri) }
        : {}),
    }
    return {
      meta: [
        { title: `${displayName} (${person.id}) — DPRR` },
        { name: "description", content: desc },
        { property: "og:title", content: `${displayName} — DPRR` },
        { property: "og:description", content: desc },
        { property: "og:type", content: "profile" },
      ],
      scripts: [
        { type: "application/ld+json", children: JSON.stringify(jsonLd) },
      ],
    }
  },
```

- [ ] **Step 3: Dataset JSON-LD on the search page**

In `site/src/routes/index.tsx`, import `SITE_URL` from `@/lib/site` and add a `head` to the route options:

```tsx
export const Route = createFileRoute("/")({
  validateSearch: (search: Record<string, unknown>) => search,
  loader: () => getSearchData(),
  head: () => ({
    scripts: [
      {
        type: "application/ld+json",
        children: JSON.stringify({
          "@context": "https://schema.org",
          "@type": "Dataset",
          name: "Digital Prosopography of the Roman Republic",
          description:
            "Prosopographical data for 4,876 persons of the Roman Republic (509–31 BC): offices held, relationships, dates, and sources.",
          url: SITE_URL,
        }),
      },
    ],
  }),
  component: SearchPage,
})
```

- [ ] **Step 4: Verify the script tags render**

Run: `cd site && vp dev --port 3000`, then:

```bash
curl -s http://localhost:3000/ | grep -c 'application/ld+json'
curl -s http://localhost:3000/persons/IUNI0001 | grep -o '"@type":"Person"'
```

Expected: `1` and `"@type":"Person"`. (Adjust URLs with the `/dprr-data/` prefix if dev serves under base.) If `scripts` with `children` doesn't render, check the TanStack Start head API — the alternative is rendering the `<script>` tag inside the page component with `dangerouslySetInnerHTML`; prefer the head API if it works. Kill the server.

- [ ] **Step 5: Check and commit**

Run: `cd site && vp check`

```bash
git add site/src/lib/site.ts 'site/src/routes/persons.$id.tsx' site/src/routes/index.tsx
git commit -m "feat: add JSON-LD structured data to person and search pages"
```

---

### Task 17: Uncertainty markers and chronological careers

The original DPRR renders scholarly uncertainty explicitly (*Tribunus Militum?* 508 — italic, trailing "?") and lists careers chronologically. Our TTL export carries the flags — 6,748 `dprr:isUncertain`, 2,596 `dprr:isDateStartUncertain`, 2,018 `dprr:isDateEndUncertain` on post assertions — but the parser drops them, and careers render in TTL iteration order. Presenting an uncertain magistracy as certain misrepresents the scholarship.

**Files:**
- Modify: `site/src/data/types.ts` (`PostAssertion`)
- Modify: `site/src/data/parse-persons.ts`
- Test: `site/src/data/parse-persons.test.ts`
- Modify: `site/src/data/aggregate-references.ts` (`OfficeHolder`, `ProvinceAssertion`)
- Modify: `site/src/data/aggregate-references.test.ts` (fixture defaults)
- Modify: `site/src/routes/persons.$id.tsx`
- Modify: `site/src/routes/offices.$slug.tsx`
- Modify: `site/src/routes/provinces.$slug.tsx`

**Interfaces:**
- Produces: `PostAssertion.isUncertain: boolean`, `PostAssertion.isDateStartUncertain: boolean`, `PostAssertion.isDateEndUncertain: boolean`; `postAssertions` sorted by `dateStart ?? dateEnd ?? Infinity` ascending; `OfficeHolder.isUncertain: boolean`; `ProvinceAssertion.isUncertain: boolean`.

- [ ] **Step 1: Write the failing test**

Add to `site/src/data/parse-persons.test.ts`:

```ts
describe("uncertainty and career order", () => {
  test("reads uncertainty flags and sorts assertions chronologically", () => {
    const ttl = `
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://romanrepublic.ac.uk/rdf/entity/Person/1> a dprr:Person ;
  dprr:hasDprrID "TEST0001" ;
  dprr:hasPersonName "TEST0001 T. Testius" .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateStart "-100"^^xsd:integer ;
  dprr:isUncertain true ;
  dprr:isDateStartUncertain true .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> ;
  dprr:hasDateStart "-200"^^xsd:integer .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/3> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/1> .
`
    const persons = parsePersonTtl(ttl, emptyRefs(), new Map())
    const pas = persons[0].postAssertions
    // Chronological: -200 first, -100 second, undated last
    expect(pas.map((pa) => pa.dateStart)).toEqual([-200, -100, null])
    expect(pas[1].isUncertain).toBe(true)
    expect(pas[1].isDateStartUncertain).toBe(true)
    expect(pas[1].isDateEndUncertain).toBe(false)
    expect(pas[0].isUncertain).toBe(false)
  })
})
```

(Reuse the same `emptyRefs()` helper as the province extraction tests.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd site && vp test src/data/parse-persons.test.ts`
Expected: FAIL — flags do not exist / order wrong.

- [ ] **Step 3: Implement parser and types**

`site/src/data/types.ts` — add to `PostAssertion`:

```ts
  /** True when the source scholarship marks this post itself as uncertain. */
  isUncertain: boolean
  isDateStartUncertain: boolean
  isDateEndUncertain: boolean
```

`site/src/data/parse-persons.ts` — in `buildPostAssertions`, include in the pushed object:

```ts
        isUncertain: first(g, "isUncertain") === "true",
        isDateStartUncertain: first(g, "isDateStartUncertain") === "true",
        isDateEndUncertain: first(g, "isDateEndUncertain") === "true",
```

and after the loop, before `return results`:

```ts
    // Chronological career order (undated entries last), matching DPRR
    results.sort(
      (a, b) =>
        (a.dateStart ?? a.dateEnd ?? Number.POSITIVE_INFINITY) -
        (b.dateStart ?? b.dateEnd ?? Number.POSITIVE_INFINITY)
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd site && vp test src/data/parse-persons.test.ts`
Expected: PASS. Then fix the `makeAssertion` fixture in `site/src/data/aggregate-references.test.ts` — add the three new fields to its defaults:

```ts
    isUncertain: false,
    isDateStartUncertain: false,
    isDateEndUncertain: false,
```

- [ ] **Step 5: Thread through aggregations**

In `site/src/data/aggregate-references.ts`:
- Add `isUncertain: boolean` to `OfficeHolder` and `ProvinceAssertion`.
- In `buildOfficeDetail`, add `isUncertain: pa.isUncertain,` to the pushed holder.
- In `buildProvinceDetail`, add `isUncertain: pa.isUncertain,` to the pushed assertion.
- Update the province detail test in `aggregate-references.test.ts` (it uses exact `toEqual`) to include `isUncertain: false`.

Run: `cd site && vp test src/data/aggregate-references.test.ts` — expect PASS.

- [ ] **Step 6: Render the "?" convention**

In `persons.$id.tsx` `OfficeEntry` — wrap the office name (the `Link` or plain text from Task 15) so uncertain posts render italic with a trailing "?":

```tsx
        <span className={assertion.isUncertain ? "italic" : undefined}>
          {/* existing office-name Link / plain text goes here unchanged */}
          {assertion.isUncertain && "?"}
        </span>
```

and after the date expression inside the date paragraph, append:

```tsx
          {(assertion.isDateStartUncertain || assertion.isDateEndUncertain) &&
            "?"}
```

In `offices.$slug.tsx`, mark the holder line: wrap the `PersonLink` in
`<span className={h.isUncertain ? "italic" : undefined}>` and render `{h.isUncertain && <span className="text-muted-foreground">?</span>}` immediately after it.

In `provinces.$slug.tsx`, do the same for `a.isUncertain` around its `PersonLink`.

- [ ] **Step 7: Verify, check, commit**

Run: `cd site && vp test src && vp check`. Dev-server spot check: `grep -rl "isUncertain true" persons | head -1` gives a TTL file whose person ID (filename) has an uncertain post — open that person page and confirm the italic name + "?".

```bash
git add site/src/data 'site/src/routes/persons.$id.tsx' 'site/src/routes/offices.$slug.tsx' 'site/src/routes/provinces.$slug.tsx'
git commit -m "feat: surface scholarly uncertainty flags and sort careers chronologically"
```

---

### Task 18: Office and province hierarchy grouping

`reference/offices.ttl` organizes its 204 offices under 8 roots via `hasParent` — Magisterial Posts, Promagisterial Posts, Priesthoods, Non-magisterial Posts, Equestrian Functions, Distinctions, plus two standalone entries (max depth 3, e.g. Magisterial Posts → consul → consul suffectus). Provinces nest under 12 roots (Italia, Africa, Asia, Mediterranean, …; max depth 2). The original DPRR groups its Career and Location facets this way; our flat alphabetical lists should follow.

**Files:**
- Modify: `site/src/data/aggregate-references.ts` (`buildNameHierarchy`, `categoryOf`, `category` on `OfficeIndexEntry`)
- Modify: `site/src/data/aggregate-references.test.ts`
- Modify: `site/src/server/data.ts` (`getOfficeIndex` passes hierarchy; `getSearchData` returns hierarchy maps)
- Modify: `site/src/routes/offices.index.tsx` (grouped rendering)
- Create: `site/src/components/facet-hierarchy-group.tsx`
- Modify: `site/src/components/facet-sidebar.tsx` (Office + Province facets)
- Modify: `site/src/routes/index.tsx` (thread hierarchy maps)

**Interfaces:**
- Produces (from `@/data/aggregate-references`):

```ts
/** child name → parent name (null for roots), from a ReferenceMaps-style map */
export function buildNameHierarchy(
  entries: Map<string, { name: string; parent: string | null }>
): Record<string, string | null>
/** Walk parentOf to the root; returns the root name (or name itself if unknown). */
export function categoryOf(
  name: string,
  parentOf: Record<string, string | null>
): string
```

`OfficeIndexEntry` gains `category: string`; `buildOfficeIndex(persons, parentOf)` gains the second parameter.
- Produces (from `@/server/data`): `getSearchData()` additionally returns `officeHierarchy: Record<string, string | null>` and `provinceHierarchy: Record<string, string | null>`.
- Produces: `<FacetHierarchyGroup title items parentOf selected onChange defaultOpen? />`.

- [ ] **Step 1: Write the failing tests**

Add to `site/src/data/aggregate-references.test.ts`:

```ts
import { buildNameHierarchy, categoryOf } from "./aggregate-references"

describe("hierarchy", () => {
  const refMap = new Map([
    ["uri:root", { name: "Magisterial Posts", parent: null }],
    ["uri:consul", { name: "consul", parent: "uri:root" }],
    ["uri:suff", { name: "consul suffectus", parent: "uri:consul" }],
  ])

  test("buildNameHierarchy maps child names to parent names", () => {
    expect(buildNameHierarchy(refMap)).toEqual({
      "Magisterial Posts": null,
      consul: "Magisterial Posts",
      "consul suffectus": "consul",
    })
  })

  test("categoryOf walks to the root", () => {
    const h = buildNameHierarchy(refMap)
    expect(categoryOf("consul suffectus", h)).toBe("Magisterial Posts")
    expect(categoryOf("consul", h)).toBe("Magisterial Posts")
    expect(categoryOf("unknown office", h)).toBe("unknown office")
  })

  test("office index carries the category", () => {
    const h = buildNameHierarchy(refMap)
    const index = buildOfficeIndex([personA, personB], h)
    expect(index[0].category).toBe("Magisterial Posts")
  })
})
```

Update the existing `buildOfficeIndex`/`buildOfficeDetail` tests to pass `{}` as the second argument where they don't care about categories, and extend the exact-equality index expectation with `category: "consul"` (with an empty hierarchy an office is its own category).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd site && vp test src/data/aggregate-references.test.ts`
Expected: FAIL — exports missing.

- [ ] **Step 3: Implement the data side**

In `site/src/data/aggregate-references.ts`:

```ts
export function buildNameHierarchy(
  entries: Map<string, { name: string; parent: string | null }>
): Record<string, string | null> {
  const parentOf: Record<string, string | null> = {}
  for (const { name, parent } of entries.values()) {
    parentOf[name] = parent ? (entries.get(parent)?.name ?? null) : null
  }
  return parentOf
}

export function categoryOf(
  name: string,
  parentOf: Record<string, string | null>
): string {
  let current = name
  const seen = new Set<string>()
  while (parentOf[current] != null && !seen.has(current)) {
    seen.add(current)
    current = parentOf[current] as string
  }
  return current
}
```

Change `buildOfficeIndex` to `buildOfficeIndex(persons: Person[], parentOf: Record<string, string | null>)` and include `category: categoryOf(name, parentOf),` in the mapped entry. Add `category: string` to `OfficeIndexEntry`.

In `site/src/server/data.ts`:
- `getOfficeIndex`: `return buildOfficeIndex(persons, buildNameHierarchy(refs.offices))` (destructure `refs` from `loadAllData()`).
- `getSearchData`: add to the returned object:

```ts
      officeHierarchy: buildNameHierarchy(refs.offices),
      provinceHierarchy: buildNameHierarchy(refs.provinces),
```

(`refs.offices` values have `abbreviation` too — structurally compatible with the `{ name, parent }` parameter type.) The handler has an explicit `Promise<{...}>` return annotation — extend it with `officeHierarchy: Record<string, string | null>` and `provinceHierarchy: Record<string, string | null>`.

Run: `cd site && vp test src/data && vp check` — expect PASS.

- [ ] **Step 4: Group the office index page**

In `site/src/routes/offices.index.tsx`, group entries by `category` and render a heading per group, in DPRR's order:

```tsx
const CATEGORY_ORDER = [
  "Magisterial Posts",
  "Promagisterial Posts",
  "Priesthoods",
  "Non-magisterial Posts",
  "Equestrian Functions",
  "Distinctions",
]

function groupByCategory(offices: OfficeIndexEntry[]) {
  const groups = new Map<string, OfficeIndexEntry[]>()
  for (const o of offices) {
    const list = groups.get(o.category) ?? []
    list.push(o)
    groups.set(o.category, list)
  }
  return [...groups].sort(
    (a, b) =>
      (CATEGORY_ORDER.indexOf(a[0]) + 1 || 99) -
        (CATEGORY_ORDER.indexOf(b[0]) + 1 || 99) ||
      a[0].localeCompare(b[0])
  )
}
```

(import `type { OfficeIndexEntry } from "@/data/aggregate-references"`). In the component, replace the single `<ul>` with one `<section>` per group: an `<h2 className="mt-8 font-heading text-xl font-semibold">{category}</h2>` followed by the existing `<ul>` markup for that group's entries.

- [ ] **Step 5: Create the hierarchical facet component**

`site/src/components/facet-hierarchy-group.tsx`:

```tsx
// site/src/components/facet-hierarchy-group.tsx
import { useMemo, useState } from "react"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import type { FacetValue } from "@/data/types"

interface FacetHierarchyGroupProps {
  title: string
  items: FacetValue[]
  /** child name → parent name (null/absent for roots) */
  parentOf: Record<string, string | null>
  selected: string[]
  onChange: (values: string[]) => void
  defaultOpen?: boolean
}

interface TreeNode {
  name: string
  count: number | null // null → structural label only, not selectable
  children: TreeNode[]
}

function buildTree(
  items: FacetValue[],
  parentOf: Record<string, string | null>
): TreeNode[] {
  const countByName = new Map(items.map((i) => [i.value, i.count]))
  // Universe: item names plus all their ancestors
  const keep = new Set<string>()
  for (const i of items) {
    let current: string | null = i.value
    while (current && !keep.has(current)) {
      keep.add(current)
      current = parentOf[current] ?? null
    }
  }
  const childrenOf = new Map<string, string[]>()
  const roots: string[] = []
  for (const name of keep) {
    const parent = parentOf[name] ?? null
    if (parent && keep.has(parent)) {
      const list = childrenOf.get(parent) ?? []
      list.push(name)
      childrenOf.set(parent, list)
    } else {
      roots.push(name)
    }
  }
  function toNode(name: string): TreeNode {
    const children = (childrenOf.get(name) ?? []).map(toNode)
    children.sort((a, b) => (b.count ?? 0) - (a.count ?? 0))
    return { name, count: countByName.get(name) ?? null, children }
  }
  return roots.map(toNode).sort((a, b) => a.name.localeCompare(b.name))
}

export function FacetHierarchyGroup({
  title,
  items,
  parentOf,
  selected,
  onChange,
  defaultOpen = true,
}: FacetHierarchyGroupProps) {
  const [open, setOpen] = useState(defaultOpen)
  const [filter, setFilter] = useState("")
  const tree = useMemo(() => buildTree(items, parentOf), [items, parentOf])

  function toggle(value: string) {
    onChange(
      selected.includes(value)
        ? selected.filter((v) => v !== value)
        : [...selected, value]
    )
  }

  const filtered = filter.trim()
    ? items.filter((i) =>
        i.value.toLowerCase().includes(filter.trim().toLowerCase())
      )
    : null

  function renderNode(node: TreeNode, depth: number) {
    return (
      <div key={node.name} style={{ paddingLeft: depth * 12 }}>
        {node.count !== null ? (
          <label className="flex cursor-pointer items-center gap-2 py-0.5 text-sm">
            <Checkbox
              checked={selected.includes(node.name)}
              onCheckedChange={() => toggle(node.name)}
            />
            <span className="min-w-0 truncate">{node.name}</span>
            <span className="ml-auto text-xs text-muted-foreground">
              {node.count}
            </span>
          </label>
        ) : (
          <p className="pt-2 pb-0.5 text-xs font-semibold text-muted-foreground uppercase">
            {node.name}
          </p>
        )}
        {node.children.map((c) => renderNode(c, node.count === null ? depth : depth + 1))}
      </div>
    )
  }

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex w-full items-center gap-1 py-2 text-sm font-semibold">
        {title}
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-3 pl-5">
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={`Filter ${title.toLowerCase()}...`}
          className="mb-1 h-7 text-xs"
        />
        {filtered
          ? filtered.map((i) => (
              <label
                key={i.value}
                className="flex cursor-pointer items-center gap-2 py-0.5 text-sm"
              >
                <Checkbox
                  checked={selected.includes(i.value)}
                  onCheckedChange={() => toggle(i.value)}
                />
                <span className="min-w-0 truncate">{i.value}</span>
                <span className="ml-auto text-xs text-muted-foreground">
                  {i.count}
                </span>
              </label>
            ))
          : tree.map((n) => renderNode(n, 0))}
      </CollapsibleContent>
    </Collapsible>
  )
}
```

Match the exact CollapsibleTrigger markup (chevron icon etc.) used in `facet-group.tsx` so the sidebar stays visually consistent — copy its trigger JSX verbatim.

- [ ] **Step 6: Wire into the sidebar and search page**

`site/src/components/facet-sidebar.tsx`:
- Add to `FacetSidebarProps`: `officeHierarchy: Record<string, string | null>` and `provinceHierarchy: Record<string, string | null>`.
- Replace the Office `FacetGroup` with:

```tsx
      <FacetHierarchyGroup
        title="Office"
        items={facets.office}
        parentOf={officeHierarchy}
        selected={state.office}
        onChange={(office) => onUpdate({ office })}
      />
```

- Replace the Province `FacetGroup` (from Task 8) with the same pattern (`parentOf={provinceHierarchy}`, `defaultOpen={false}`).

`site/src/routes/index.tsx` — destructure `officeHierarchy, provinceHierarchy` from `Route.useLoaderData()` and pass them to `<FacetSidebar />`.

- [ ] **Step 7: Verify, check, commit**

Run: `cd site && vp test src && vp check`. Dev-server: the Office facet should show category labels (MAGISTERIAL POSTS, …) with offices beneath and sub-offices (consul suffectus) indented under their parents; filtering within the facet falls back to a flat list; the `/offices/` page shows grouped headings.

```bash
git add site/src/data site/src/server 'site/src/routes/offices.index.tsx' site/src/routes/index.tsx site/src/components
git commit -m "feat: group office and province facets and office index by hierarchy"
```

---

### Task 19: Full build verification

Prove the complete feature set builds and serves correctly under the deployment prefix.

**Files:** none created — verification only. Fix-forward anything this uncovers.

- [ ] **Step 1: Full validation suite**

Run: `cd site && vp check && vp test`
Expected: everything passes.

- [ ] **Step 2: Full production build**

Run: `cd site && vp build 2>&1 | tail -30`
Expected: success. Note any unmapped-province warning (acceptable) — no other warnings/errors.

- [ ] **Step 3: Verify prerendered page inventory**

```bash
find site/dist/client/persons -name index.html | wc -l    # expect ~4,876
find site/dist/client/offices -name index.html | wc -l    # expect > 100 (index + details)
find site/dist/client/tribes -name index.html | wc -l     # expect > 30
find site/dist/client/provinces -name index.html | wc -l  # expect > 20
```

If office/tribe/province counts are 0: the crawler didn't reach them — confirm the SiteHeader links render in prerendered HTML (`grep -o 'href="[^"]*offices[^"]*"' site/dist/client/index.html | head`) and diagnose from there.

- [ ] **Step 4: Serve under the prefix and smoke-check everything**

```bash
ln -sfn "$(pwd)/site/dist/client" /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/pages/dprr-data
python3 -m http.server 8722 -d /private/tmp/claude-502/-Users-gillisandrew-Projects-gillisandrew-dprr-data/7ac392d5-4d5f-4572-8701-00efbce17515/scratchpad/pages &
sleep 1
for path in "" "persons/IUNI0001/" "offices/" "offices/consul/" "tribes/" "provinces/" ; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8722/dprr-data/${path}")
  echo "${code} /dprr-data/${path}"
done
curl -s http://localhost:8722/dprr-data/persons/IUNI0001/ | grep -c 'application/ld+json'
curl -s http://localhost:8722/dprr-data/ | grep -c '/dprr-data/assets/'
kill %1
```

Expected: all `200`; JSON-LD count ≥ 1; prefixed asset count > 0.

- [ ] **Step 5: Report**

Summarize in the task report: page counts, unmapped-province warning contents, and any fallback decisions taken (e.g. Task 2 crawler-link variant). Do NOT merge to `main`.

```bash
git status   # confirm clean tree; commit any stragglers (routeTree.gen.ts etc.)
```

---

## Execution Notes

- **Task order:** 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 12 → 13 → 14 → 11 → 15 → 16 → 17 → 18 → 19. Task 11 (SiteHeader) is listed early for interface context but typechecks only after the routes in 12–14 exist — execute it after Task 14 (see its Step 1 note).
- Tasks 3–6 (pipeline) and 7–8 (search UI) are sequential chains. Tasks 12/13/14 are mutually independent. Tasks 17–18 (scholarly-convention alignment from the romanrepublic.ac.uk review) intentionally revisit files from earlier tasks; running them after Task 16 keeps each earlier task's diff small and reviewable.
- The province mapping (Task 4) is a **user-review artifact** — flag it explicitly when reporting.
