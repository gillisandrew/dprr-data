# Semantics & Source-Surfacing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface DPRR's stripped/ignored predicates (canonical career order, Broughton post labels, status details, origins, uncertainty flags), explain domain terms via a glossary with ⓘ popovers, make hierarchy select-all visible, and fix SPARQL dark-mode highlighting plus scrollbar layout shift.

**Architecture:** Workstream 1 re-shards the repo's RDF data keeping three ordering predicates. Workstreams 2–3 extend the TTL parsers (`site/src/data/`) and the person page to display the new fields, add a glossary module driving an `InfoHint` popover component, and add implied-check rendering to the hierarchy facet. Workstream 4 is two independent CSS/editor fixes.

**Tech Stack:** Python 3 + pyoxigraph (via `uv run --with pyoxigraph`), React 19 + TanStack Start, Tailwind 4, Radix (via existing `radix-ui` package), CodeMirror 6 (`@codemirror/*`, already installed), vp (vite-plus) toolchain.

**Spec:** `docs/superpowers/specs/2026-08-12-semantics-surfacing-design.md`

## Global Constraints

- Web commands run from `site/`; use `vp` for everything (`vp test`, `vp check`, `vp run build`) — never npm/pnpm/npx.
- Test imports come from `vite-plus/test` (`import { expect, test, describe } from "vite-plus/test"`), not `vitest`.
- No new package.json dependencies. Python tooling runs via `uv run --with pyoxigraph python3 …`.
- Commits go directly on `main`, message style `feat:`/`fix:`/`data:`/`docs:`. Commit signing may fail once with "agent refused operation" — retry the same `git commit` once before reporting a problem.
- URL search params and filter *semantics* must not change (presentation of hierarchy selection changes; `descendantSet` matching does not).
- Search index/payload shapes unchanged (new fields are person-page data, not search facets).
- The scratch directory is the session scratchpad (never `/tmp`).

---

### Task 1: Re-shard with display-ordering predicates kept

**Files:**
- Modify: `shard.py:38-58` (STRIP_PREDICATES block)
- Modify: `README.md` (the "changes applied by shard.py" list, around line 38)
- Regenerate: `persons/**`, `reference/*.ttl`, `ontology.ttl` (script output)

**Interfaces:**
- Produces: person TTL files whose `PostAssertion` subjects carry `dprr:hasPosition` (integer), relationship-assertion subjects carry `dprr:hasRelationshipNumber` (integer), and `reference/relationships.ttl` entries carry `dprr:hasOrderNumber` (integer). Tasks 2–3 parse these.

- [ ] **Step 1: Baseline re-shard with the UNMODIFIED script (upstream-refresh check)**

```bash
cd /Users/gillisandrew/Projects/gillisandrew/dprr-data
uv run --with pyoxigraph python3 shard.py ~/Downloads/dprr.ttl .
git status --porcelain | head -20
```

If the tree is dirty, the Apr 1 dump differs from what generated the repo.
Inspect `git diff --stat | tail -5`; commit ALL of it as its own commit:

```bash
git add -A persons reference ontology.ttl
git commit -m "data: upstream refresh (2026-04 dump)"
```

If the tree is clean, skip the commit and continue.

- [ ] **Step 2: Edit shard.py to keep the three predicates**

In `STRIP_PREDICATES`, delete these three lines and their comment:

```python
    # UI display ordering: controls list position on the DPRR website
    NamedNode(VOCAB + "hasOrderNumber"),
    NamedNode(VOCAB + "hasPosition"),
    NamedNode(VOCAB + "hasRelationshipNumber"),
```

Add above the frozenset (module comment level, next to the existing block
comment) so the decision is recorded:

```python
# NOTE: hasPosition (PostAssertion career order), hasOrderNumber
# (Relationship-type order), and hasRelationshipNumber (order within a
# relationship group) were originally stripped as "UI display ordering".
# They are DPRR's canonical display ordering and the site consumes them;
# they are deliberately KEPT.
```

- [ ] **Step 3: Re-run the shard and verify the predicates landed**

```bash
uv run --with pyoxigraph python3 shard.py ~/Downloads/dprr.ttl .
grep -c "hasPosition" persons/ABUR/ABUR1215.ttl   # expect >= 1
grep -c "hasOrderNumber" reference/relationships.ttl  # expect ~44
grep -rl "hasRelationshipNumber" persons/A* | head -3  # expect hits
```

- [ ] **Step 4: Verify the site still builds against the enriched data**

Run (from `site/`): `vp test`
Expected: PASS — parsers ignore unknown predicates, nothing breaks.

- [ ] **Step 5: Update README's derivation list**

In `README.md`, the bullet list of stripped predicates under "changes were
applied by shard.py": remove any mention of
`hasOrderNumber`/`hasPosition`/`hasRelationshipNumber` being stripped and
add one line:

```markdown
- `hasPosition`, `hasOrderNumber`, and `hasRelationshipNumber` (DPRR's
  display ordering for careers and relationships) are **kept** — the site
  uses them to order person-page sections the way DPRR curated them.
```

(If the README lists stripped predicates generically without naming these
three, just add the "kept" line.)

- [ ] **Step 6: Commit**

```bash
git add -A persons reference ontology.ttl shard.py README.md
git commit -m "data: keep display-ordering predicates (hasPosition, hasOrderNumber, hasRelationshipNumber)"
```

---

### Task 2: Relationship-type order in the reference maps

**Files:**
- Modify: `site/src/data/types.ts:126` (`ReferenceMaps.relationships`)
- Modify: `site/src/data/parse-references.ts:78`
- Test: `site/src/data/parse-references.test.ts` (extend; create if absent — check first)

**Interfaces:**
- Consumes: Task 1's `reference/relationships.ttl` with `dprr:hasOrderNumber`.
- Produces: `ReferenceMaps.relationships: Map<string, { name: string; orderNumber: number | null }>` — Task 3 reads `.get(uri)?.name` and `.get(uri)?.orderNumber`.

- [ ] **Step 1: Write the failing test**

Check whether `site/src/data/parse-references.test.ts` exists; extend or
create with:

```ts
import { expect, test, describe } from "vite-plus/test"
import { parseReferenceTtl } from "./parse-references"

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
    ).toEqual({ name: "son of", orderNumber: 3 })
    expect(
      refs.relationships.get(
        "http://romanrepublic.ac.uk/rdf/entity/Relationship/13"
      )
    ).toEqual({ name: "brother of", orderNumber: null })
  })
})
```

- [ ] **Step 2: Run to verify it fails**

Run (from `site/`): `vp test src/data/parse-references.test.ts`
Expected: FAIL — map value is a bare string today.

- [ ] **Step 3: Implement**

`types.ts`: change the `relationships` entry of `ReferenceMaps` to:

```ts
  relationships: Map<string, { name: string; orderNumber: number | null }>
```

`parse-references.ts`: import `firstNum` from `./ttl` (alongside `first`)
and change line 78 to:

```ts
    relationships: namedMap(inputs.relationships, (name, g) => ({
      name,
      orderNumber: firstNum(g, "hasOrderNumber"),
    })),
```

`parse-persons.ts:217` currently reads
`refs.relationships.get(relTypeUri)` as a string — update it now so the
suite compiles (full Relationship changes land in Task 3):

```ts
        relationshipType:
          (relTypeUri && refs.relationships.get(relTypeUri)?.name) ?? "",
```

- [ ] **Step 4: Run tests**

Run: `vp test src/data/` then full `vp test && vp check`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/data
git commit -m "feat: relationship-type order numbers in reference maps"
```

---

### Task 3: Parse the new person predicates

**Files:**
- Modify: `site/src/data/types.ts` (PostAssertion, Person, new StatusAssertion, Relationship)
- Modify: `site/src/data/parse-persons.ts`
- Test: `site/src/data/parse-persons.test.ts` (extend)

**Interfaces:**
- Consumes: Task 2's relationship map shape.
- Produces (Task 4 renders these exact names):
  - `PostAssertion` += `position: number | null`, `officeXref: string | null`, `dateSourceText: string | null`
  - `interface StatusAssertion { id: string; statusName: string; dateStart: number | null; dateEnd: number | null; isDateStartUncertain: boolean; isDateEndUncertain: boolean; isUncertain: boolean; secondarySource: string; notes: Note[] }`
  - `Person.statusAssertions: StatusAssertion[]` (was `string[]`)
  - `Person` += `origin: string | null`, `novusNotes: string | null`, `isNomenUncertain: boolean`, `isCognomenUncertain: boolean`, `isPraenomenUncertain: boolean`, `isFiliationUncertain: boolean`, `isOtherNamesUncertain: boolean`
  - `Relationship` += `typeOrderNumber: number | null`, `relationshipNumber: number | null`

- [ ] **Step 1: Write the failing tests**

Extend `site/src/data/parse-persons.test.ts` (follow its existing fixture
style — it builds TTL strings and a stub `ReferenceMaps`; reuse its
helpers). Add a fixture person exercising every new field:

```ts
const ENRICHED_TTL = `@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/42> a dprr:Person ;
  dprr:hasDprrID "TEST0042" ;
  dprr:hasNomen "Testius" ;
  dprr:isNomenUncertain true ;
  dprr:isCognomenUncertain true ;
  dprr:hasOrigin "Tusculum" ;
  dprr:isNovus true ;
  dprr:hasNovusNotes "Cic. Mur. 17" ;
  dprr:hasEraFrom -100 ;
  dprr:hasEraTo -50 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasPosition 2 ;
  dprr:hasOfficeXref "Pr. 66" ;
  dprr:hasDateSourceText "before Kal. Ian." ;
  dprr:hasDateStart -66 ; dprr:hasDateEnd -66 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasPosition 1 ;
  dprr:hasDateStart -63 ; dprr:hasDateEnd -63 .
<http://romanrepublic.ac.uk/rdf/entity/PostAssertion/3> a dprr:PostAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasDateStart -70 ; dprr:hasDateEnd -70 .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertion/9> a dprr:StatusAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/42> ;
  dprr:hasStatus <http://romanrepublic.ac.uk/rdf/entity/Status/2> ;
  dprr:hasDateStart -70 ; dprr:hasDateEnd -65 ;
  dprr:isDateStartUncertain true ;
  dprr:hasSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> ;
  dprr:hasStatusAssertionNote <http://romanrepublic.ac.uk/rdf/entity/StatusAssertionNote/5> .
<http://romanrepublic.ac.uk/rdf/entity/StatusAssertionNote/5> a dprr:StatusAssertionNote ;
  dprr:hasNoteText "listed among the equites" ;
  dprr:hasSecondarySourceForNote <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> .
`
```

(Register `Status/2` → `{ name: "eques Romanus", abbreviation: null }` and
`SecondarySource/1` → a named source in the stub `ReferenceMaps`, matching
how the file's existing tests build their stubs.)

Tests:

```ts
test("career sorts by position, positionless fall back chronologically after", () => {
  const [p] = parsePersonTtl(ENRICHED_TTL, refs, new Map())
  expect(p.postAssertions.map((pa) => pa.id)).toEqual([
    "http://romanrepublic.ac.uk/rdf/entity/PostAssertion/2", // position 1
    "http://romanrepublic.ac.uk/rdf/entity/PostAssertion/1", // position 2
    "http://romanrepublic.ac.uk/rdf/entity/PostAssertion/3", // no position
  ])
  expect(p.postAssertions[1].officeXref).toBe("Pr. 66")
  expect(p.postAssertions[1].dateSourceText).toBe("before Kal. Ian.")
  expect(p.postAssertions[0].officeXref).toBeNull()
})

test("status assertions carry dates, uncertainty, source, and notes", () => {
  const [p] = parsePersonTtl(ENRICHED_TTL, refs, new Map())
  expect(p.statusAssertions).toHaveLength(1)
  const sa = p.statusAssertions[0]
  expect(sa.statusName).toBe("eques Romanus")
  expect(sa.dateStart).toBe(-70)
  expect(sa.isDateStartUncertain).toBe(true)
  expect(sa.notes[0].text).toBe("listed among the equites")
  // statuses summary still derives capitalized names + boolean flags
  expect(p.statuses).toContain("Eques Romanus")
  expect(p.statuses).toContain("Novus")
})

test("origin, novusNotes, and name-part uncertainty flags parse", () => {
  const [p] = parsePersonTtl(ENRICHED_TTL, refs, new Map())
  expect(p.origin).toBe("Tusculum")
  expect(p.novusNotes).toBe("Cic. Mur. 17")
  expect(p.isNomenUncertain).toBe(true)
  expect(p.isCognomenUncertain).toBe(true)
  expect(p.isPraenomenUncertain).toBe(false)
})
```

Plus a relationship-order test (extend an existing relationship fixture or
add one): two relationship assertions with `dprr:hasRelationshipNumber 2`
and `1` → expect `relationshipNumber` parsed, and `typeOrderNumber` pulled
from the (Task 2-shaped) relationships stub map.

- [ ] **Step 2: Run to verify failures**

Run: `vp test src/data/parse-persons.test.ts`
Expected: FAIL — unknown fields / wrong statusAssertions shape.

- [ ] **Step 3: Implement types.ts**

- `PostAssertion`: add after `officeAbbreviation`:

```ts
  /** DPRR's canonical career-display position (lower = earlier in list). */
  position: number | null
  /** Broughton's abbreviated post label, e.g. "Pr. Peregrinus", "cos. 63". */
  officeXref: string | null
  /** The source's original wording for the date, e.g. "ca. 51 BC". */
  dateSourceText: string | null
```

- New interface after `PostAssertionNote`:

```ts
export interface StatusAssertion {
  id: string
  /** Raw status name from the authority list, e.g. "eques Romanus". */
  statusName: string
  dateStart: number | null
  dateEnd: number | null
  isDateStartUncertain: boolean
  isDateEndUncertain: boolean
  isUncertain: boolean
  secondarySource: string
  notes: Note[]
}
```

- `Person`: change `statusAssertions: string[]` to
  `statusAssertions: StatusAssertion[]` (update the doc comment: "Full
  StatusAssertion records with dates, sources, and notes."), and add:

```ts
  /** Plausible geographic origin per DPRR ("hasOrigin"). */
  origin: string | null
  novusNotes: string | null
  isNomenUncertain: boolean
  isCognomenUncertain: boolean
  isPraenomenUncertain: boolean
  isFiliationUncertain: boolean
  isOtherNamesUncertain: boolean
```

(`nobilisNotes` already exists on Person.)

- `Relationship`: add:

```ts
  /** The relationship TYPE's curated display order (hasOrderNumber). */
  typeOrderNumber: number | null
  /** Order within the group (hasRelationshipNumber). */
  relationshipNumber: number | null
```

- [ ] **Step 4: Implement parse-persons.ts**

1. Add constants + group map:

```ts
const STATUS_ASSERTION_NOTE_TYPE = `${DPRR}StatusAssertionNote`
```

collect `statusAssertionNoteGroups` in the type switch like the other
note types.

2. In `buildPostAssertions`, add to the pushed object:

```ts
        position: firstNum(g, "hasPosition"),
        officeXref: first(g, "hasOfficeXref"),
        dateSourceText: first(g, "hasDateSourceText"),
```

and replace the sort with:

```ts
    // DPRR's canonical career order (hasPosition) first; entries without
    // a position fall back to chronological and sort after positioned ones.
    results.sort((a, b) => {
      const pa = a.position ?? Number.MAX_SAFE_INTEGER
      const pb = b.position ?? Number.MAX_SAFE_INTEGER
      if (pa !== pb) return pa - pb
      return (
        (a.dateStart ?? a.dateEnd ?? Number.MAX_SAFE_INTEGER) -
        (b.dateStart ?? b.dateEnd ?? Number.MAX_SAFE_INTEGER)
      )
    })
```

3. Replace `buildStatusAssertions` with a full builder:

```ts
  // Build StatusAssertions (with dates, sources, notes) for a person URI
  function buildStatusAssertions(personUri: string): StatusAssertion[] {
    const results: StatusAssertion[] = []
    for (const [saUri, g] of statusAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const statusUri = first(g, "hasStatus")
      const statusName =
        (statusUri && refs.statuses.get(statusUri)?.name) ?? ""
      if (!statusName) continue
      const notes = all(g, "hasStatusAssertionNote")
        .map((uri) => {
          const ng = statusAssertionNoteGroups.get(uri)
          return ng ? buildNoteFields(ng) : null
        })
        .filter((n): n is Note => n !== null)
      results.push({
        id: saUri,
        statusName,
        dateStart: firstNum(g, "hasDateStart"),
        dateEnd: firstNum(g, "hasDateEnd"),
        isDateStartUncertain: first(g, "isDateStartUncertain") === "true",
        isDateEndUncertain: first(g, "isDateEndUncertain") === "true",
        isUncertain: first(g, "isUncertain") === "true",
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
        notes,
      })
    }
    // Chronological, undated last
    results.sort(
      (a, b) =>
        (a.dateStart ?? a.dateEnd ?? Number.MAX_SAFE_INTEGER) -
        (b.dateStart ?? b.dateEnd ?? Number.MAX_SAFE_INTEGER)
    )
    return results
  }
```

Import `StatusAssertion` in the type imports.

4. Statuses summary derivation (in the person loop) becomes:

```ts
    const statusAssertions = buildStatusAssertions(personUri)
    const statusNames = [
      ...new Set(statusAssertions.map((sa) => sa.statusName)),
    ]
    const statuses = [
      ...(isPatrician ? ["Patrician"] : []),
      ...(isNobilis ? ["Nobilis"] : []),
      ...(isNovus ? ["Novus"] : []),
      // "eques Romanus" → "Eques Romanus"
      ...statusNames.map((s) => s.charAt(0).toUpperCase() + s.slice(1)),
    ]
```

5. In `buildRelationships`, add to the pushed object (relTypeUri already
in scope):

```ts
        typeOrderNumber:
          (relTypeUri && refs.relationships.get(relTypeUri)?.orderNumber) ??
          null,
        relationshipNumber: firstNum(g, "hasRelationshipNumber"),
```

6. In the person push, add:

```ts
      origin: first(g, "hasOrigin"),
      novusNotes: first(g, "hasNovusNotes"),
      isNomenUncertain: first(g, "isNomenUncertain") === "true",
      isCognomenUncertain: first(g, "isCognomenUncertain") === "true",
      isPraenomenUncertain: first(g, "isPraenomenUncertain") === "true",
      isFiliationUncertain: first(g, "isFiliationUncertain") === "true",
      isOtherNamesUncertain: first(g, "isOtherNamesUncertain") === "true",
```

- [ ] **Step 5: Run tests, fix fixture fallout**

Run: `vp test`
Expected: the new tests PASS. Fixture objects in `context-line.test.ts`,
`dedupe.test.ts`, `search-payload.test.ts`, `aggregate-references.test.ts`
use `statusAssertions: []` — an empty array satisfies both old and new
types, but they may need the seven new Person fields added
(`origin: null, novusNotes: null, isNomenUncertain: false, …`) if they
build full `Person` objects. Add exactly those fields where TypeScript
complains; change nothing else. Then `vp check`.

- [ ] **Step 6: Commit**

```bash
git add src/data
git commit -m "feat: parse career order, Broughton labels, status details, origin, uncertainty flags"
```

---

### Task 4: Person-page display of the new fields

**Files:**
- Modify: `site/src/routes/persons.$id.tsx` (groupRelationships, OfficeEntry, new StatusSection)
- Modify: `site/src/components/person-registry.tsx` (Origin field, uncertainty markers)
- Test: none new (presentation; parser logic already covered) — `vp check` + visual pass gate this task.

**Interfaces:**
- Consumes: Task 3's `PostAssertion.officeXref/dateSourceText`, `StatusAssertion`, `Person.origin/novusNotes/is*Uncertain`, `Relationship.typeOrderNumber/relationshipNumber`.
- Produces: nothing downstream.

- [ ] **Step 1: Career entry additions**

In `OfficeEntry` (persons.$id.tsx): after the `officeAbbreviation` span,
add the Broughton label:

```tsx
          {assertion.officeXref && (
            <span className="ml-1 text-sm text-muted-foreground italic">
              {assertion.officeXref}
            </span>
          )}
```

Where the source citation renders, prepend the original date wording when
present (adjacent to the existing `SourceCitation` for
`assertion.secondarySource`):

```tsx
        {assertion.dateSourceText && (
          <p className="text-xs text-muted-foreground italic">
            date: {assertion.dateSourceText}
          </p>
        )}
```

- [ ] **Step 2: Status section**

Add after the Career `Section` in `PersonPage`:

```tsx
      {person.statusAssertions.length > 0 && (
        <Section title="Status" count={person.statusAssertions.length}>
          <div>
            {person.statusAssertions.map((sa) => (
              <StatusEntry key={sa.id} assertion={sa} />
            ))}
          </div>
        </Section>
      )}
```

New component in the same file (mirror `DateEntry`'s structure):

```tsx
function StatusEntry({ assertion }: { assertion: StatusAssertion }) {
  return (
    <div className="ledger-row flex gap-3">
      <span className="year-col text-sm">
        {(assertion.dateStart !== null || assertion.dateEnd !== null) && (
          <>
            {assertion.dateStart !== null &&
            assertion.dateEnd !== null &&
            assertion.dateStart !== assertion.dateEnd ? (
              <EraRange from={assertion.dateStart} to={assertion.dateEnd} />
            ) : (
              <DateDisplay
                year={(assertion.dateStart ?? assertion.dateEnd) as number}
              />
            )}
            {(assertion.isDateStartUncertain ||
              assertion.isDateEndUncertain) &&
              "?"}
          </>
        )}
      </span>
      <div className="min-w-0 flex-1 text-sm">
        <span
          className={cn("small-caps", assertion.isUncertain && "italic")}
        >
          {assertion.statusName}
          {assertion.isUncertain && "?"}
        </span>
        {assertion.notes.map((note, i) => (
          <p key={i} className="text-sm text-muted-foreground">
            {note.text}
          </p>
        ))}
        <SourceCitation
          name={assertion.secondarySource}
          className="text-xs text-muted-foreground"
        />
      </div>
    </div>
  )
}
```

Import `StatusAssertion` from `@/data/types` and `cn` from `@/lib/utils`
if not present.

- [ ] **Step 3: novusNotes beside nobilisNotes**

Directly after the existing `person.nobilisNotes` paragraph:

```tsx
      {person.novusNotes && (
        <p className="mt-2 text-sm text-muted-foreground italic">
          {person.novusNotes}
        </p>
      )}
```

- [ ] **Step 4: Relationship ordering**

Replace `groupRelationships` with:

```tsx
/** Groups relationships by type in DPRR's curated order (hasOrderNumber,
 * alphabetical fallback); members by hasRelationshipNumber, then name. */
function groupRelationships(rels: Relationship[]): [string, Relationship[]][] {
  const byType = new Map<string, Relationship[]>()
  for (const r of rels) {
    const list = byType.get(r.relationshipType) ?? []
    list.push(r)
    byType.set(r.relationshipType, list)
  }
  const orderOf = (list: Relationship[]) =>
    list[0].typeOrderNumber ?? Number.MAX_SAFE_INTEGER
  return [...byType]
    .sort(
      (a, b) =>
        orderOf(a[1]) - orderOf(b[1]) || a[0].localeCompare(b[0])
    )
    .map(([type, list]) => [
      type,
      [...list].sort(
        (a, b) =>
          (a.relationshipNumber ?? Number.MAX_SAFE_INTEGER) -
            (b.relationshipNumber ?? Number.MAX_SAFE_INTEGER) ||
          displayName(a.relatedPersonName).localeCompare(
            displayName(b.relatedPersonName)
          )
      ),
    ])
}
```

- [ ] **Step 5: Registry — Origin + uncertainty markers**

In `person-registry.tsx`:

- After the Filiation field add:

```tsx
      {person.origin && <Field label="Origin">{person.origin}</Field>}
```

- Uncertainty `?` markers (glossary popover attaches in Task 5; plain text
  here): Praenomen becomes
  `{person.praenomen}{person.isPraenomenUncertain && "?"}`; the Nomen
  link's text `{person.nomen}` gains `{person.isNomenUncertain && "?"}`
  *outside* the Link; Cognomen `{person.cognomen}{person.isCognomenUncertain && "?"}`;
  Filiation `{person.filiation}{person.isFiliationUncertain && "?"}`. Add
  an Other-names field if one exists (grep first — if the registry has no
  otherNames field today, leave it; the flag still parses for the header).

- [ ] **Step 6: Verify and commit**

Run: `vp check && vp test`, then `vp dev --port 3000` and eyeball
`/persons/ABUR1215` (career shows "Pr. Peregrinus" xref, Status section
lists the eques/senator rows with sources).

```bash
git add src/routes/persons.\$id.tsx src/components/person-registry.tsx
git commit -m "feat: person page shows canonical career order, Broughton labels, status detail, origin"
```

---

### Task 5: Glossary module, InfoHint popover, About glossary, placements

**Files:**
- Restore: `site/src/components/ui/popover.tsx` (from git history)
- Modify: `site/src/styles.css` (re-add `--popover*` tokens)
- Create: `site/src/lib/glossary.ts`
- Create: `site/src/components/info-hint.tsx`
- Modify: `site/src/routes/about.tsx` (Glossary section)
- Modify: `site/src/components/filter-panel.tsx` (section ⓘ + office-toggle ⓘ)
- Modify: `site/src/components/person-registry.tsx`, `site/src/routes/persons.$id.tsx` (field-label ⓘ, `?` popovers)
- Test: `site/src/lib/glossary.test.ts`

**Interfaces:**
- Produces: `GLOSSARY: Record<GlossaryTermId, { label: string; text: string }>`, `type GlossaryTermId`, `<InfoHint term={GlossaryTermId} mark?: string />`.

- [ ] **Step 1: Restore the popover primitive and tokens**

```bash
git show b7060832~1:site/src/components/ui/popover.tsx > src/components/ui/popover.tsx
```

(Run from `site/`. That commit deleted it as dead code; it has consumers
again.) Re-add the theme tokens in `styles.css`: in `:root` after
`--card-foreground`:

```css
  --popover: oklch(0.993 0.003 83);
  --popover-foreground: oklch(0.147 0.004 49.3);
```

in `.dark` after `--card-foreground`:

```css
  --popover: oklch(0.214 0.009 43.1);
  --popover-foreground: oklch(0.986 0.002 67.8);
```

in `@theme inline` next to the other color mappings:

```css
  --color-popover-foreground: var(--popover-foreground);
  --color-popover: var(--popover);
```

- [ ] **Step 2: Write the glossary module + failing test**

`site/src/lib/glossary.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import { GLOSSARY } from "./glossary"

describe("glossary", () => {
  test("every entry has a label and non-trivial text", () => {
    for (const [id, entry] of Object.entries(GLOSSARY)) {
      expect(entry.label.length, id).toBeGreaterThan(0)
      expect(entry.text.length, id).toBeGreaterThan(40)
    }
  })
})
```

`site/src/lib/glossary.ts` — the exact copy (adapted from the ontology's
rdfs:comments; keep DPRR attribution phrasing):

```ts
// site/src/lib/glossary.ts
// User-facing explanations of DPRR's domain terms. Single source of truth
// for InfoHint popovers and the About-page glossary. Texts are adapted
// from the DPRR ontology's rdfs:comment annotations.

export interface GlossaryEntry {
  label: string
  text: string
}

export const GLOSSARY = {
  patrician: {
    label: "Patrician",
    text: "A member of Rome's hereditary aristocracy, the small group of families that originally monopolized major offices and priesthoods. DPRR marks a person patrician only when it believes the sources support it; a following ? records that the attribution is uncertain.",
  },
  nobilis: {
    label: "Nobilis",
    text: "A member of the nobilitas — by the usual modern definition, a descendant of a consul. DPRR flags a person as nobilis when scholarship considered the family noble; the note shown beside the flag records why, usually one or two brief primary-source references.",
  },
  novus: {
    label: "Novus",
    text: "A novus homo, \"new man\": the first of his family to reach the Senate or, in the stricter sense, the consulship. DPRR asserts this only where it believes the sources support it; a note may record the evidence.",
  },
  "eques-romanus": {
    label: "Eques Romanus",
    text: "A member of the equestrian order, the property class ranking below the Senate. Recorded as a dated status assertion with its supporting source rather than as a permanent attribute.",
  },
  senator: {
    label: "Senator",
    text: "Attested membership of the Senate, recorded as a dated status assertion with its supporting source — used chiefly where a person is known as a senator without a specific recorded magistracy.",
  },
  praenomen: {
    label: "Praenomen",
    text: "The Roman personal name (Marcus, Gaius, Lucius…), conventionally abbreviated (M., C., L.). Only a small set was in use, so it rarely identifies a person on its own.",
  },
  nomen: {
    label: "Nomen (gens)",
    text: "The family or clan name (Iulius, Cornelius, Claudius…), shared by all members of a gens. Filtering by gens finds every recorded member of the clan.",
  },
  cognomen: {
    label: "Cognomen",
    text: "The surname distinguishing branches of a gens (Caesar, Scipio, Cicero…). Not every person carries one, especially in the early Republic.",
  },
  filiation: {
    label: "Filiation",
    text: "The patronymic formula recorded in Roman naming: typically the father's (f.) and grandfather's (n. for nepos) praenomina, e.g. \"M. f. M. n.\" — son of Marcus, grandson of Marcus. DPRR records the filiation chosen by its research team from the sources.",
  },
  "re-number": {
    label: "RE number",
    text: "The person's entry number in the Realencyclopädie der classischen Altertumswissenschaft (Pauly–Wissowa), the standard reference work — e.g. Cicero is Tullius (29). Numbers distinguish homonymous members of the same gens.",
  },
  tribe: {
    label: "Tribe",
    text: "One of the 35 Roman voting tribes (tribus) in which citizens were registered, e.g. Cornelia or Fabia. A person's tribe is part of their formal identification; differing source claims can leave a person with more than one recorded tribe.",
  },
  office: {
    label: "Office",
    text: "A political or religious post in the Roman state (consul, praetor, tribune, pontifex…). Offices form a hierarchy — selecting a parent like \"Magisterial Posts\" includes every office beneath it.",
  },
  location: {
    label: "Location",
    text: "The province or region a recorded post was connected to, as identified from the secondary source. Locations form a hierarchy; selecting a parent includes the regions beneath it.",
  },
  "life-events": {
    label: "Life events",
    text: "Dated biographical events recorded for a person beyond office-holding: birth, death (including violent death), exile, proscription, and similar. Each carries its supporting source.",
  },
  era: {
    label: "Era dates",
    text: "The date range in which DPRR believes the person lived. Birth and death dates rarely survive, so these are estimates derived from the attested record, not precise lifespans.",
  },
  uncertain: {
    label: "Uncertainty (?)",
    text: "A question mark records that the DPRR team considers the flagged element uncertain — a name part, a date, or a whole assertion. It reflects the state of the evidence, not an error.",
  },
  "office-and-mode": {
    label: "Require every office",
    text: "With this on, results must have held ALL of the selected offices at some point (an intersection), instead of any one of them. Useful for questions like \"who was both consul and censor?\".",
  },
  "office-in-range": {
    label: "Offices in time range",
    text: "With this on, the selected time period applies to when the offices were held, not just to when the person lived — \"praetors in the 60s BC\" rather than \"praetors alive in the 60s BC\".",
  },
  status: {
    label: "Status",
    text: "Social-rank attributes recorded by DPRR: Patrician, Nobilis, and Novus are person-level flags; Eques Romanus and Senator are dated, sourced assertions. Selecting several requires all of them (AND).",
  },
  origin: {
    label: "Origin",
    text: "A plausible geographic origin for the person — a town or region — as suggested by the primary and secondary sources and recorded by DPRR.",
  },
  "broughton-label": {
    label: "Source label",
    text: "The abbreviated post as it appears in the source scholarship (chiefly Broughton's Magistrates of the Roman Republic), e.g. \"cos. 63\" or \"Pr. Peregrinus\" — sometimes more specific than the standardized office name.",
  },
} as const satisfies Record<string, GlossaryEntry>

export type GlossaryTermId = keyof typeof GLOSSARY
```

Run: `vp test src/lib/glossary.test.ts` → FAIL (module missing) before
writing the module, PASS after.

- [ ] **Step 3: InfoHint component**

`site/src/components/info-hint.tsx`:

```tsx
// site/src/components/info-hint.tsx
// Small ⓘ (or custom mark) trigger opening a glossary explanation.
import { Info } from "lucide-react"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { GLOSSARY, type GlossaryTermId } from "@/lib/glossary"

export function InfoHint({
  term,
  mark,
}: {
  term: GlossaryTermId
  /** Custom trigger text (e.g. "?" for uncertainty markers); ⓘ otherwise. */
  mark?: string
}) {
  const entry = GLOSSARY[term]
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`What does ${entry.label} mean?`}
          className="inline-flex cursor-help items-center align-baseline text-muted-foreground hover:text-foreground"
        >
          {mark ?? <Info aria-hidden="true" className="h-3 w-3" />}
        </button>
      </PopoverTrigger>
      <PopoverContent className="max-w-72 text-xs">
        <p className="micro-label mb-1">{entry.label}</p>
        <p>{entry.text}</p>
      </PopoverContent>
    </Popover>
  )
}
```

- [ ] **Step 4: About-page glossary**

In `about.tsx`, add a section following the existing `<h2>` section
pattern (before the final section or at the end of the sections list):

```tsx
        <section>
          <h2 className="mb-2 font-heading text-xl font-semibold">
            Glossary
          </h2>
          <dl className="space-y-3">
            {Object.entries(GLOSSARY).map(([id, entry]) => (
              <div key={id}>
                <dt className="text-sm font-medium">{entry.label}</dt>
                <dd className="text-sm text-muted-foreground">
                  {entry.text}
                </dd>
              </div>
            ))}
          </dl>
        </section>
```

Import `GLOSSARY` from `@/lib/glossary`.

- [ ] **Step 5: Placements**

`filter-panel.tsx`:
- Map section → term at module level:

```ts
const SECTION_TERM: Record<PanelSection, GlossaryTermId> = {
  office: "office",
  name: "nomen",
  status: "status",
  tribe: "tribe",
  location: "location",
  events: "life-events",
}
```

- In the open-section inset (the `role="region"` div), render a header
  line above `<SectionBody …>`:

```tsx
          <div className="mb-2 flex items-center gap-1.5">
            <span className="micro-label-muted">
              {[...BASIC, ...ADVANCED].find((s) => s.key === openKey)!.label}
            </span>
            <InfoHint term={SECTION_TERM[openKey]} />
          </div>
```

- Office Options toggles: append `<InfoHint term="office-and-mode" />`
  after the "Require every selected office (AND)" span and
  `<InfoHint term="office-in-range" />` after the "Apply time period to
  offices (held in range)" span (inside the labels, after the text span).

`person-registry.tsx`:
- RE field label row: `Field` renders a plain string label; extend `Field`
  to accept `hint?: GlossaryTermId`:

```tsx
function Field({
  label,
  hint,
  children,
}: {
  label: string
  hint?: GlossaryTermId
  children: React.ReactNode
}) {
  return (
    <div className="min-w-0">
      <p className="micro-label-muted">
        {label}
        {hint && (
          <span className="ml-1">
            <InfoHint term={hint} />
          </span>
        )}
      </p>
      <p className="text-sm break-words">{children}</p>
    </div>
  )
}
```

  Then: Praenomen → `hint="praenomen"`, Nomen → `hint="nomen"`, Cognomen →
  `hint="cognomen"`, Filiation → `hint="filiation"`, RE →
  `hint="re-number"`, Tribe → `hint="tribe"`, Origin → `hint="origin"`.
- Uncertainty `?` markers from Task 4 become popovers:
  replace the literal `"?"` appends with `<InfoHint term="uncertain" mark="?" />`.

`persons.$id.tsx`:
- Locate the `Section` component definition in this file and extend it
  with an optional `hint?: GlossaryTermId` prop, rendered as
  `{hint && <InfoHint term={hint} />}` immediately after the count in its
  header, matching the header's existing markup style.
- Career section: `hint="broughton-label"` (explains the new italic
  source labels). Status section: `hint="status"`. Dates section:
  `hint="life-events"`. Per-row elements (officeXref spans, `?` marks in
  `year-col`) stay unadorned — one hint per section, not per row; the
  registry-level `?` popover documents the uncertainty convention.

- [ ] **Step 6: Verify and commit**

Run: `vp check && vp test`. Dev-server spot check: ⓘ opens on Office
section, About shows the glossary, registry labels have hints, `?` marks
open the uncertainty explanation.

```bash
git add src/lib/glossary.ts src/lib/glossary.test.ts src/components/info-hint.tsx src/components/ui/popover.tsx src/components/person-registry.tsx src/components/filter-panel.tsx src/routes/about.tsx src/routes/persons.\$id.tsx src/styles.css
git commit -m "feat: glossary with info popovers across filters, person page, and About"
```

---

### Task 6: Implied-check hierarchy trees + chip wording

**Files:**
- Modify: `site/src/components/facet-hierarchy-group.tsx`
- Modify: `site/src/components/filter-panel.tsx` (pass `childNoun` for Location)
- Modify: `site/src/components/active-filter-chips.tsx` + `site/src/routes/index.tsx` (chip suffixes)
- Test: `site/src/lib/filter.test.ts` untouched (semantics unchanged) — presentation gated by `vp check` + visual pass.

**Interfaces:**
- Consumes: existing `parentOf: Record<string, string | null>` props.
- Produces: `FacetHierarchyGroup` new optional prop `childNoun?: string` (default `"sub-offices"`); `ActiveFilterChips` new optional props `officesWithChildren?: Set<string>`, `provincesWithChildren?: Set<string>`.

- [ ] **Step 1: Implied-check rendering in FacetHierarchyGroup**

Add helpers inside the component (after `toggle`):

```tsx
  /** Nearest selected ancestor of `name`, or null. */
  function selectedAncestor(name: string): string | null {
    let cur = parentOf[name] ?? null
    while (cur) {
      if (selected.includes(cur)) return cur
      cur = parentOf[cur] ?? null
    }
    return null
  }
```

Add a `selectableDescendants` count to `TreeNode`: in `buildTree`'s
`toNode`, compute

```ts
    const selectableDescendants = children.reduce(
      (sum, c) => sum + c.selectableDescendants + (c.count !== null ? 1 : 0),
      0
    )
```

and include it in the returned node (extend the `TreeNode` interface with
`selectableDescendants: number`).

In `renderNode`, for selectable rows (`node.count !== null`), replace the
label body with implied-aware rendering:

```tsx
          {(() => {
            const ancestor = selectedAncestor(node.name)
            const isSelected = selected.includes(node.name)
            if (ancestor && !isSelected) {
              return (
                <label
                  className="flex items-center gap-2 py-0.5 text-[0.8125rem] leading-6 opacity-55"
                  title={`Included via ${ancestor}`}
                >
                  <Checkbox checked disabled aria-label={`${node.name} — included via ${ancestor}`} />
                  <span className="min-w-0 truncate">{node.name}</span>
                  {!hideCounts && (
                    <span className="ml-auto text-xs text-muted-foreground">
                      {node.count}
                    </span>
                  )}
                </label>
              )
            }
            return (
              <label className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6">
                <Checkbox
                  checked={isSelected}
                  onCheckedChange={() => toggle(node.name)}
                />
                <span className="min-w-0 truncate">{node.name}</span>
                {isSelected && node.selectableDescendants > 0 && (
                  <span className="text-xs text-muted-foreground italic">
                    — incl. {node.selectableDescendants} {childNoun}
                  </span>
                )}
                {!hideCounts && (
                  <span className="ml-auto text-xs text-muted-foreground">
                    {node.count}
                  </span>
                )}
              </label>
            )
          })()}
```

Structural (non-selectable) parent labels: same implied treatment is not
needed (they cannot be selected), but when a structural label's ancestor
is selected its checkbox should also render `checked disabled` with the
muted styling — apply the same `selectedAncestor` check there.

Add the prop with default: `childNoun = "sub-offices"` in the
destructured props and `childNoun?: string` in
`FacetHierarchyGroupProps`.

- [ ] **Step 2: Pass childNoun for Location**

In `filter-panel.tsx`'s `SectionBody`, the Location
`FacetHierarchyGroup` gains `childNoun="sub-locations"`. (Office keeps
the default.)

- [ ] **Step 3: Chip suffixes**

`index.tsx` (`SearchResults`): compute once and pass down:

```tsx
  const officesWithChildren = useMemo(
    () =>
      new Set(
        Object.values(bundle.payload.officeHierarchy).filter(
          (p): p is string => p !== null
        )
      ),
    [bundle]
  )
  const provincesWithChildren = useMemo(
    () =>
      new Set(
        Object.values(bundle.payload.provinceHierarchy).filter(
          (p): p is string => p !== null
        )
      ),
    [bundle]
  )
```

Pass `officesWithChildren={officesWithChildren}`
`provincesWithChildren={provincesWithChildren}` to `<ActiveFilterChips>`.
Import `useMemo`.

`active-filter-chips.tsx`: add the two optional props (type
`Set<string>`, default `new Set()` via destructuring default) and change
the two loops:

```ts
  for (const office of state.office) {
    const suffix = officesWithChildren.has(office) ? " + sub-offices" : ""
    chips.push({
      label: `Office: ${office}${suffix}`,
      onRemove: () =>
        onRemove({ office: state.office.filter((o) => o !== office) }),
    })
  }
```

and for provinces with `" + sub-locations"` respectively.

- [ ] **Step 4: Verify and commit**

Run: `vp check && vp test`. Dev server: select "Magisterial Posts" →
descendants render implied-checked/muted, row shows "— incl. N
sub-offices", chip reads "Office: Magisterial Posts + sub-offices";
unchecking restores normal rows; leaf selections unaffected.

```bash
git add src/components src/routes/index.tsx
git commit -m "feat: implied-check hierarchy selection with sub-tree annotations"
```

---

### Task 7: SPARQL dark-mode highlighting + scrollbar-gutter

**Files:**
- Modify: `site/src/routes/sparql.tsx:113-119` (highlight style)
- Modify: `site/src/styles.css` (scrollbar-gutter)

**Interfaces:** none.

- [ ] **Step 1: Theme-aware highlight style**

In `sparql.tsx`, replace the `defaultHighlightStyle` import usage. Import:

```ts
import { HighlightStyle, syntaxHighlighting, StreamLanguage } from "@codemirror/language"
import { tags } from "@lezer/highlight"
```

(`@lezer/highlight` is a direct dependency of `@codemirror/language`, so
the import resolves without adding anything to package.json. If `vp check`
nevertheless fails to resolve it, `@codemirror/language` does not re-export
`tags` — in that case report the resolution error rather than adding a
dependency.)

Define at module level:

```ts
// Token colors read the site's CSS variables, so both themes (and live
// theme toggles) are handled without recreating the editor.
const sparqlHighlight = HighlightStyle.define([
  { tag: tags.keyword, color: "var(--accent-ink)", fontWeight: "500" },
  { tag: tags.string, color: "var(--chart-2)" },
  { tag: tags.number, color: "var(--chart-2)" },
  { tag: tags.variableName, color: "var(--foreground)", fontWeight: "500" },
  { tag: tags.comment, color: "var(--muted-foreground)", fontStyle: "italic" },
  { tag: tags.operator, color: "var(--muted-foreground)" },
  { tag: tags.bracket, color: "var(--muted-foreground)" },
  { tag: tags.atom, color: "var(--accent-ink)" },
])
```

Replace `syntaxHighlighting(defaultHighlightStyle, { fallback: true })`
with `syntaxHighlighting(sparqlHighlight, { fallback: true })` and remove
the now-unused `defaultHighlightStyle` import.

- [ ] **Step 2: Scrollbar gutter**

In `styles.css`, next to the existing bare `body` rule:

```css
/* Reserve scrollbar space so short pages (landing) and long pages
   (results) keep identical centering — no shift when the scrollbar
   appears. No-op where overlay scrollbars are in use. */
html {
  scrollbar-gutter: stable;
}
```

- [ ] **Step 3: Verify and commit**

Run: `vp check && vp test`. Dev server: `/sparql` — keywords (SELECT,
WHERE, PREFIX), strings, comments legible in light AND dark, including
toggling theme while the editor is open. Navigate landing → results →
person and confirm the nav doesn't shift horizontally (test with
System Settings → Appearance → "Show scroll bars: Always" if on
overlay-scrollbar macOS defaults).

```bash
git add src/routes/sparql.tsx src/styles.css
git commit -m "fix: theme-aware SPARQL highlighting and stable scrollbar gutter"
```

---

### Task 8: Final verification and build

**Files:** none new.

- [ ] **Step 1: Full gate**

Run (from `site/`): `vp check && vp test && vp run build`
Expected: all PASS; build (prerender + sitemap normalize) completes. Note
the build regenerates ~6k pages against the re-sharded data — confirm the
person-page JSON payloads carry the new fields:

```bash
grep -l "officeXref" dist/client/data/persons/ABUR*.json 2>/dev/null | head -1
```

(Adjust the path if person JSON lives elsewhere under `dist/client/data` —
find it with `ls dist/client/data`.)

- [ ] **Step 2: Visual QA checklist (dev server + browser)**

1. `/persons/ABUR1215` — career ordered by DPRR position, "Pr.
   Peregrinus" style xref labels visible, Status section with source
   citations, registry ⓘ hints work.
2. A novus person (`grep -rl hasNovusNotes persons/ | head -1` → its ID)
   shows the novus note paragraph.
3. Office tree: parent selection → implied checks + "incl. N
   sub-offices" + chip suffix.
4. About → Glossary section renders every term.
5. `/sparql` dark/light highlighting.
6. Landing ↔ results ↔ person: no horizontal shift.
7. Source audit (spec requirement): on a data-rich person page, confirm
   every assertion row — career, status, dates, notes, relationships —
   renders its `SourceCitation` and any note text; report any row type
   that lacks one as a finding instead of silently passing.

- [ ] **Step 3: Report**

Summarize what shipped, note anything visual the executor could not
verify, and list the commits. Do not push — the user decides when to
deploy.
