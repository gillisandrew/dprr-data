# Source Completeness & Provincia Terminology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface the four missing source/qualifier displays (tribe sources+notes, uncertain relationships, date intervals, date sources), rename visible "Location" labels to "Provincia" with a corrected glossary entry, and make hint popovers open on hover.

**Architecture:** Parser additions in `site/src/data/` (tribe assertion records, relationship uncertainty); a pure date-interval formatter in `lib/dates.ts`; a shared `HoverPopover` wrapper adopted by `InfoHint` and a new `SourceHint`; and a mechanical label sweep. No URL, param, slug, or data-field renames.

**Tech Stack:** React 19 + TanStack Start, Radix popover (existing `ui/popover.tsx`), vp (vite-plus) toolchain, lucide-react.

**Spec:** `docs/superpowers/specs/2026-08-12-source-completeness-provincia-design.md`

## Global Constraints

- Web commands run from `site/`; use `vp` only (`vp test`, `vp check`, `vp run build`). Test imports from `vite-plus/test`, never `vitest`.
- No new package.json dependencies.
- URLs, query params (`?province=`), route paths (`/provinces`), slugs, and data field names (`province`, `provinces`, `provinceOriginal`) are UNCHANGED — the rename touches user-visible labels only.
- Filter semantics unchanged.
- Commits on `main`, `feat:`/`fix:` style. Commit signing may fail with "agent refused operation" — retry the same `git commit` once; if it still fails, stop and report (the user unlocks the signing agent).
- Never `git checkout`/`git restore`/`git stash` anything you did not create.

---

### Task 1: Parser — tribe assertion records + relationship uncertainty

**Files:**
- Modify: `site/src/data/types.ts` (new `TribeAssertionRecord`, `Person.tribeAssertions`, `Relationship.isUncertain`)
- Modify: `site/src/data/parse-persons.ts` (`buildTribes` sibling, `buildRelationships`)
- Test: `site/src/data/parse-persons.test.ts` (extend)

**Interfaces:**
- Produces (Task 3 renders these):
  - `interface TribeAssertionRecord { tribeName: string; secondarySource: string; notes: string | null; isUncertain: boolean }`
  - `Person.tribeAssertions: TribeAssertionRecord[]`
  - `Relationship.isUncertain: boolean`
  - `PersonSummary.tribes: string[]` stays and must remain derived from the same assertions (deduped names).

- [ ] **Step 1: Write the failing tests**

Append to `site/src/data/parse-persons.test.ts` (reuse `makeRefs`; register
Tribe/38 in the stub — see step body):

```ts
const TRIBE_REL_TTL = `@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
<http://romanrepublic.ac.uk/rdf/entity/Person/50> a dprr:Person ;
  dprr:hasDprrID "TEST0050" ;
  dprr:hasNomen "Testius" .
<http://romanrepublic.ac.uk/rdf/entity/Person/51> a dprr:Person ;
  dprr:hasDprrID "TEST0051" ;
  dprr:hasPersonName "TEST0051 T. Testius Junior" ;
  dprr:hasNomen "Testius" .
<http://romanrepublic.ac.uk/rdf/entity/TribeAssertion/7> a dprr:TribeAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/50> ;
  dprr:hasTribe <http://romanrepublic.ac.uk/rdf/entity/Tribe/38> ;
  dprr:hasSecondarySource <http://romanrepublic.ac.uk/rdf/entity/SecondarySource/1> ;
  dprr:hasNotes "p187." ;
  dprr:isUncertain true .
<http://romanrepublic.ac.uk/rdf/entity/RelationshipAssertion/8> a dprr:RelationshipAssertion ;
  dprr:isAboutPerson <http://romanrepublic.ac.uk/rdf/entity/Person/50> ;
  dprr:isUncertain true ;
  dprr:hasRelationship <http://romanrepublic.ac.uk/rdf/entity/Relationship/3> ;
  dprr:hasRelatedPerson <http://romanrepublic.ac.uk/rdf/entity/Person/51> .
`

describe("tribe assertion records and relationship uncertainty", () => {
  function tribeRefs(): ReferenceMaps {
    const refs = makeRefs()
    refs.tribes.set("http://romanrepublic.ac.uk/rdf/entity/Tribe/38", {
      name: "Camilia",
      abbreviation: "Cam.",
    })
    refs.relationships.set(
      "http://romanrepublic.ac.uk/rdf/entity/Relationship/3",
      { name: "father of", orderNumber: 1 }
    )
    return refs
  }

  test("tribe assertions carry source, notes, and uncertainty", () => {
    const persons = parsePersonTtl(TRIBE_REL_TTL, tribeRefs(), new Map())
    const p = persons.find((x) => x.id === "TEST0050")!
    expect(p.tribeAssertions).toEqual([
      {
        tribeName: "Camilia",
        secondarySource: "Broughton MRR",
        notes: "p187.",
        isUncertain: true,
      },
    ])
    // Flat facet list still derives from the same assertions
    expect(p.tribes).toEqual(["Camilia"])
  })

  test("relationship isUncertain parses", () => {
    const persons = parsePersonTtl(TRIBE_REL_TTL, tribeRefs(), new Map())
    const p = persons.find((x) => x.id === "TEST0050")!
    expect(p.relationships).toHaveLength(1)
    expect(p.relationships[0].isUncertain).toBe(true)
  })
})
```

- [ ] **Step 2: Run to verify failure**

Run (from `site/`): `vp test src/data/parse-persons.test.ts`
Expected: FAIL — unknown `tribeAssertions` / `isUncertain` fields.

- [ ] **Step 3: Implement types.ts**

After the `Relationship` interface add:

```ts
export interface TribeAssertionRecord {
  tribeName: string
  secondarySource: string
  notes: string | null
  isUncertain: boolean
}
```

`Person` gains (near `tribes` doc reference):

```ts
  /** Full tribe assertions with sources and notes; `tribes` on the summary
   * is the deduped name list derived from these. */
  tribeAssertions: TribeAssertionRecord[]
```

`Relationship` gains:

```ts
  /** True when the source scholarship marks the relationship itself as
   * uncertain. */
  isUncertain: boolean
```

- [ ] **Step 4: Implement parse-persons.ts**

Replace `buildTribes` with a builder returning records, and derive names:

```ts
  // Build tribe assertion records (with sources/notes) for a person URI
  function buildTribeAssertions(personUri: string): TribeAssertionRecord[] {
    const results: TribeAssertionRecord[] = []
    for (const [, g] of tribeAssertionGroups) {
      if (first(g, "isAboutPerson") !== personUri) continue
      const tribeUri = first(g, "hasTribe")
      const tribeName = tribeUri ? refs.tribes.get(tribeUri)?.name : null
      if (!tribeName) continue
      results.push({
        tribeName,
        secondarySource: resolveSource(first(g, "hasSecondarySource")),
        notes: first(g, "hasNotes"),
        isUncertain: first(g, "isUncertain") === "true",
      })
    }
    return results
  }
```

In the person loop:

```ts
    const tribeAssertions = buildTribeAssertions(personUri)
```

and in the person push, replace `tribes: buildTribes(personUri),` with:

```ts
      tribes: [...new Set(tribeAssertions.map((t) => t.tribeName))],
      tribeAssertions,
```

(delete the old `buildTribes`). Import `TribeAssertionRecord` in the type
imports. In `buildRelationships`, add to the pushed object:

```ts
        isUncertain: first(g, "isUncertain") === "true",
```

- [ ] **Step 5: Run tests, fix fixture fallout**

Run: `vp test` — the fixture factories in `dedupe.test.ts` (and any other
test building full `Person`/`Relationship` literals) need
`tribeAssertions: []` on Person and `isUncertain: false` on Relationship
where TypeScript complains. Add exactly those; nothing else. Then
`vp check`.

- [ ] **Step 6: Commit**

```bash
git add src/data
git commit -m "feat: parse tribe assertion sources and relationship uncertainty"
```

---

### Task 2: Date intervals + date-source display

**Files:**
- Modify: `site/src/lib/dates.ts` (new formatter)
- Modify: `site/src/routes/persons.$id.tsx` (`DateEntry`, `OfficeEntry` date line)
- Test: `site/src/lib/dates.test.ts` (extend)

**Interfaces:**
- Produces: `formatYearWithInterval(year: number, interval: string | null, uncertain?: boolean): string` in `@/lib/dates`.

- [ ] **Step 1: Write the failing tests**

Append to `site/src/lib/dates.test.ts`:

```ts
describe("formatYearWithInterval", () => {
  test("B prefixes before, A prefixes after", () => {
    expect(formatYearWithInterval(-216, "B")).toBe("before 216 BC")
    expect(formatYearWithInterval(-216, "A")).toBe("after 216 BC")
  })

  test("S and null defer to formatYear exactly", () => {
    expect(formatYearWithInterval(-216, "S")).toBe(formatYear(-216))
    expect(formatYearWithInterval(-216, null)).toBe(formatYear(-216))
  })

  test("uncertainty composes with the interval", () => {
    expect(formatYearWithInterval(-216, "B", true)).toBe("before c. 216 BC")
    expect(formatYearWithInterval(14, "A", true)).toBe("after c. AD 14")
  })
})
```

(Import `formatYearWithInterval` alongside the file's existing imports from
`./dates`.)

- [ ] **Step 2: Run to verify failure**

Run: `vp test src/lib/dates.test.ts`
Expected: FAIL — no such export.

- [ ] **Step 3: Implement in dates.ts**

```ts
/**
 * Format a year with its DateInformation interval qualifier:
 * "B" = before, "A" = after, "S"/null = a single year (plain formatYear).
 * The uncertainty "c." prefix composes inside: "before c. 216 BC".
 */
export function formatYearWithInterval(
  year: number,
  interval: string | null,
  uncertain: boolean = false
): string {
  const base = formatYear(year, uncertain)
  if (interval === "B") return `before ${base}`
  if (interval === "A") return `after ${base}`
  return base
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `vp test src/lib/dates.test.ts` → PASS.

- [ ] **Step 5: Use it in DateEntry**

In `persons.$id.tsx`, `DateEntry` currently renders
`<DateDisplay year={dateInfo.value} uncertain={dateInfo.isUncertain} />`.
Replace with:

```tsx
      <span className="year-col text-sm">
        {formatYearWithInterval(
          dateInfo.value,
          dateInfo.interval,
          dateInfo.isUncertain
        )}
      </span>
```

(keeping the surrounding markup; import `formatYearWithInterval` from
`@/lib/dates`; remove the `DateDisplay` import only if now unused — it is
still used by `OfficeEntry`/`StatusEntry`, so it stays.)

- [ ] **Step 6: Date-source display in OfficeEntry**

In `OfficeEntry`, replace the existing `dateSourceText` paragraph:

```tsx
        {assertion.dateSourceText && (
          <p className="text-xs text-muted-foreground italic">
            date: {assertion.dateSourceText}
          </p>
        )}
```

with a version that also credits a distinct date source
(`dateSecondarySource` is parsed but currently never displayed):

```tsx
        {(assertion.dateSourceText ||
          (assertion.dateSecondarySource &&
            assertion.dateSecondarySource !== assertion.secondarySource)) && (
          <p className="text-xs text-muted-foreground italic">
            {assertion.dateSourceText
              ? `date: ${assertion.dateSourceText}`
              : "date"}
            {assertion.dateSecondarySource &&
              assertion.dateSecondarySource !== assertion.secondarySource && (
                <> per {assertion.dateSecondarySource}</>
              )}
          </p>
        )}
```

(Renders: `date: ca. 51 BC per Rüpke`, `date: ca. 51 BC`, or
`date per Rüpke`; suppressed entirely when neither exists or the date
source merely repeats the assertion's own citation.)

- [ ] **Step 7: Verify and commit**

Run: `vp check && vp test` → PASS.

```bash
git add src/lib/dates.ts src/lib/dates.test.ts src/routes/persons.\$id.tsx
git commit -m "feat: render date intervals (before/after) and distinct date sources"
```

---

### Task 3: HoverPopover, SourceHint, tribe hints, uncertain relationships

**Files:**
- Create: `site/src/components/hover-popover.tsx`
- Create: `site/src/components/source-hint.tsx`
- Modify: `site/src/components/info-hint.tsx` (adopt HoverPopover)
- Modify: `site/src/components/person-registry.tsx` (tribe field)
- Modify: `site/src/routes/persons.$id.tsx` (`RelationshipEntry` uncertainty)

**Interfaces:**
- Consumes: Task 1's `TribeAssertionRecord`, `Person.tribeAssertions`, `Relationship.isUncertain`.
- Produces: `<HoverPopover trigger={ReactElement} contentClassName?>{children}</HoverPopover>`; `<SourceHint sources={{secondarySource, notes}[]} label={string} />`.

- [ ] **Step 1: HoverPopover**

`site/src/components/hover-popover.tsx`:

```tsx
// site/src/components/hover-popover.tsx
// Popover that opens on hover (with a short delay) or keyboard focus, and
// still toggles on click/tap so touch devices work. Content stays open
// while the pointer is over the trigger or the content.
import { useRef, useState } from "react"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"

export function HoverPopover({
  trigger,
  contentClassName,
  children,
}: {
  /** The trigger element (a button); handlers are merged onto it. */
  trigger: React.ReactElement
  contentClassName?: string
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined)

  const openSoon = () => {
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setOpen(true), 150)
  }
  const closeSoon = () => {
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setOpen(false), 100)
  }
  const cancelClose = () => clearTimeout(timer.current)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        asChild
        onMouseEnter={openSoon}
        onMouseLeave={closeSoon}
        onFocus={(e) => {
          // Keyboard focus only — a tap/click focuses too, and letting it
          // open here would make the subsequent click-toggle close it.
          if (e.target.matches(":focus-visible")) setOpen(true)
        }}
        onBlur={() => setOpen(false)}
      >
        {trigger}
      </PopoverTrigger>
      <PopoverContent
        onMouseEnter={cancelClose}
        onMouseLeave={closeSoon}
        onOpenAutoFocus={(e) => e.preventDefault()}
        className={contentClassName}
      >
        {children}
      </PopoverContent>
    </Popover>
  )
}
```

- [ ] **Step 2: InfoHint adopts it**

Rewrite `info-hint.tsx`'s JSX to:

```tsx
  return (
    <HoverPopover
      contentClassName="max-w-72 text-xs"
      trigger={
        <button
          type="button"
          aria-label={`What does ${entry.label} mean?`}
          className="inline-flex cursor-help items-center align-baseline text-muted-foreground hover:text-foreground"
        >
          {mark ?? <Info aria-hidden="true" className="h-3 w-3" />}
        </button>
      }
    >
      <p className="micro-label mb-1">{entry.label}</p>
      <p>{entry.text}</p>
    </HoverPopover>
  )
```

(imports: drop the direct Popover imports, add
`import { HoverPopover } from "./hover-popover"`).

- [ ] **Step 3: SourceHint**

`site/src/components/source-hint.tsx`:

```tsx
// site/src/components/source-hint.tsx
// Small book icon revealing the source citation(s) and note(s) behind a
// compact value (e.g. a tribe name in the registry strip).
import { BookOpen } from "lucide-react"
import { HoverPopover } from "./hover-popover"
import { SourceCitation } from "./source-citation"

export function SourceHint({
  sources,
  label,
}: {
  sources: { secondarySource: string; notes: string | null }[]
  label: string
}) {
  if (sources.length === 0) return null
  return (
    <HoverPopover
      contentClassName="max-w-72 text-xs"
      trigger={
        <button
          type="button"
          aria-label={label}
          className="ml-0.5 inline-flex cursor-help items-center align-baseline text-muted-foreground hover:text-foreground"
        >
          <BookOpen aria-hidden="true" className="h-3 w-3" />
        </button>
      }
    >
      <div className="space-y-2">
        {sources.map((s, i) => (
          <div key={i}>
            <SourceCitation name={s.secondarySource} className="text-xs" />
            {s.notes && <p className="text-muted-foreground">{s.notes}</p>}
          </div>
        ))}
      </div>
    </HoverPopover>
  )
}
```

(Check `SourceCitation`'s props in `source-citation.tsx` before wiring — it
takes `name` and `className`; if it renders nothing for empty names, an
empty `secondarySource` row will just show the note, which is correct.)

- [ ] **Step 4: Registry tribe field**

In `person-registry.tsx`, replace the Tribe field body: iterate
`person.tribeAssertions` grouped by name instead of `person.tribes`:

```tsx
      {person.tribeAssertions.length > 0 && (
        <Field label="Tribe" hint="tribe">
          {[
            ...Map.groupBy(person.tribeAssertions, (t) => t.tribeName),
          ].map(([name, asserts], i) => (
            <span key={name}>
              {i > 0 && ", "}
              <Link
                to="/tribes/$slug"
                params={{ slug: slugify(name) }}
                className="text-accent-ink hover:underline"
              >
                {name}
              </Link>
              {asserts.some((a) => a.isUncertain) && (
                <InfoHint term="uncertain" mark="?" />
              )}
              <SourceHint
                sources={asserts}
                label={`Sources for tribe ${name}`}
              />
            </span>
          ))}
        </Field>
      )}
```

If `Map.groupBy` is unavailable in the TS lib target (check `vp check`),
group with a plain reduce into a `Map<string, TribeAssertionRecord[]>`.
Import `SourceHint`.

- [ ] **Step 5: Uncertain relationships**

In `persons.$id.tsx` `RelationshipEntry`, wrap the related-person span with
the uncertainty convention:

```tsx
        <span className={relationship.isUncertain ? "italic" : undefined}>
          {relationship.relatedPersonId ? (
            <PersonLink
              id={relationship.relatedPersonId}
              name={relationship.relatedPersonName}
            />
          ) : (
            <span>{displayName(relationship.relatedPersonName)}</span>
          )}
          {relationship.isUncertain && "?"}
        </span>
```

- [ ] **Step 6: Verify and commit**

Run: `vp check && vp test` → PASS. Quick SSR check that AEMI2895's tribe
field carries the hint markup:
`vp dev --port 3000` then
`curl -s http://localhost:3000/dprr-data/persons/AEMI2895 | python3 -c "import sys; h=sys.stdin.read(); print('p187.' in h, 'Sources for tribe' in h)"`
→ expect `True True`. Kill the server after.

```bash
git add src/components src/routes/persons.\$id.tsx
git commit -m "feat: tribe source hints, uncertain relationships, hover-opening popovers"
```

---

### Task 4: Provincia rename + glossary + README

**Files:**
- Modify: `site/src/routes/persons.$id.tsx:376` ("Location:" label)
- Modify: `site/src/components/filter-panel.tsx:33,377,386` (section label, group title, childNoun)
- Modify: `site/src/components/active-filter-chips.tsx:76` (chip label + suffix)
- Modify: `site/src/components/site-header.tsx:15` (nav label)
- Modify: `site/src/routes/provinces.index.tsx:9,13,25,27` and `site/src/routes/provinces.$slug.tsx:22` (titles/headings)
- Modify: `site/src/routes/directory.tsx` (prose link text "locations", if present)
- Modify: `site/src/lib/glossary.ts` (location entry) and `site/src/lib/filter-panel.ts:44` (comment)
- Modify: `README.md` (province-mapping phrasing, if it says "location")

**Interfaces:** none new. URL paths, params, and slugs unchanged.

- [ ] **Step 1: Label sweep**

Exact replacements (visible strings only — do NOT rename variables, keys,
routes, or params):

- `persons.$id.tsx`: career-row `Location:{" "}` → `Provincia:{" "}`
- `filter-panel.tsx`: ADVANCED entry `label: "Location"` → `label: "Provincia"`; the Location `FacetHierarchyGroup`'s `title="Location"` → `title="Provincia"` (the title feeds the filter placeholder "Filter provincia..."); `childNoun="sub-locations"` → `childNoun="sub-provinciae"`; update the file-top comment's mention of "Location".
- `active-filter-chips.tsx`: `` `Location: ${province}${suffix}` `` → `` `Provincia: ${province}${suffix}` `` and the suffix string `" + sub-locations"` → `" + sub-provinciae"`.
- `site-header.tsx`: `{ to: "/provinces", label: "Locations" }` → `label: "Provinciae"`.
- `provinces.index.tsx`: title `"Locations — DPRR"` → `"Provinciae — DPRR"`; meta description `"Locations — provinces, courts, and spheres of responsibility…"` → `"Provinciae — territories, courts, and spheres of responsibility with recorded office holders"`; `<h1>` `Locations` → `Provinciae`; the count line `"{n} provinces with recorded office holders"` → `"{n} provinciae with recorded office holders"`.
- `provinces.$slug.tsx`: title suffix `— Locations — DPRR` → `— Provinciae — DPRR`.
- `directory.tsx`: the prose link currently reading "locations" → "provinciae" (keep the `/provinces` link target).
- `lib/filter-panel.ts` comment "(Tribe, Location, Events)" → "(Tribe, Provincia, Events)".

- [ ] **Step 2: Glossary entry**

In `glossary.ts`, replace the `location` entry (id unchanged):

```ts
  location: {
    label: "Provincia",
    text: "The sphere of responsibility assigned with a post — often a territory (Sicilia, Hispania), but equally a task or command: a war, a fleet, the courts, the grain supply. DPRR records the provincia as given in the sources; the geographic grouping used for browsing is this site's curation.",
  },
```

- [ ] **Step 3: README touch-up**

Search README.md for "location"/"Locations" in the province-mapping
sentence ("mapping of free-text province strings to canonical provinces" —
if it already says provinces, leave it; change only wording that calls
provinciae "locations").

- [ ] **Step 4: Sweep check**

`grep -rn "Location" site/src --include="*.tsx" --include="*.ts" | grep -v "location:"` —
remaining hits must be non-visible identifiers only (route ids, variable
names). The glossary/About page and filter section pick up the new label
automatically.

- [ ] **Step 5: Verify and commit**

Run: `vp check && vp test` → PASS.

```bash
git add src ../README.md
git commit -m "feat: provincia terminology across visible labels and glossary"
```

---

### Task 5: Final verification

**Files:** none new.

- [ ] **Step 1: Full gate**

Run (from `site/`): `vp check && vp test && vp run build`
Expected: all PASS.

- [ ] **Step 2: Visual QA (dev server + browser)**

1. `/persons/AEMI2895`: Tribe shows the name + book-icon hint; hover opens
   the popover with the source and "p187."; uncertainty `?` where flagged.
2. A person with an uncertain relationship (search TTL for
   `RelationshipAssertion` + `isUncertain true`, e.g. MINU2466): the
   related person renders italic with `?`.
3. A person with a B/A date interval (grep `hasDateInterval "B"`): Dates
   row reads "before N BC".
4. Career rows read "Provincia:"; filter section, chips, nav, and
   /provinces pages all say Provincia/Provinciae; glossary ⓘ shows the new
   text.
5. ⓘ and book hints open on hover (~150ms), stay open moving into the
   content, close on leave; click still toggles; keyboard Tab focus opens.
6. Confirm suppressed date-source case: a post whose
   `dateSecondarySource` equals its `secondarySource` shows no "per …".

- [ ] **Step 3: Report**

Summarize; list commits; do not push (user decides).
