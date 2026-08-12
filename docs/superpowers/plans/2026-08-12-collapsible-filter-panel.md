# Collapsible Filter Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the popover-based filter band on the search page with in-place collapsible sections (accordion, progressive disclosure), regenerate the site icon's raster derivatives from the updated non-square SVG, and put the icon in the nav in place of the "DPRR" wordmark.

**Architecture:** A new `FilterPanel` component renders a header row of pill triggers (Office, Name, Status, then a "More filters" reveal for Tribe, Location, Events) with the single open section expanding full-width below the row. Pure disclosure logic (active counts, auto-reveal predicates) lives in `site/src/lib/filter-panel.ts` where it is unit-testable with the existing vitest setup. Existing facet body components (`FacetGroup`, `FacetHierarchyGroup`, `FacetCombobox`) are reused; the popover/bottom-sheet layer is deleted.

**Tech Stack:** React 19, TanStack Router/Start, Tailwind 4, Radix (via `radix-ui`), vite-plus (`vp`) toolchain, lucide-react icons.

**Spec:** `docs/superpowers/specs/2026-08-12-collapsible-filter-panel-design.md`

## Global Constraints

- All commands run from `site/` unless the command shows another directory.
- Use `vp` for everything (`vp test`, `vp check`, `vp build`, `vp dlx`) — never npm/pnpm/npx directly.
- Test imports come from `vite-plus/test` (`import { expect, test, describe } from "vite-plus/test"`), not `vitest`.
- No new package.json dependencies anywhere in this plan. One-off icon tooling runs via `vp dlx`.
- `SearchState`, URL-param serialization, and filter logic must not change.
- One filter-UI code path for all viewports — no `matchMedia`/breakpoint branching in the new component.
- Commits go directly on `main` (repo convention). Commit messages follow the existing `feat:`/`fix:`/`docs:` style.
- Commit signing may fail once with "agent refused operation" — retry the same `git commit` once before reporting a problem (the user approves the signing prompt).
- The temp/scratch directory for icon intermediates is the session scratchpad, referred to as `$SCRATCH` below — never `/tmp`.

---

### Task 1: Disclosure-logic helpers (`filter-panel.ts`)

Pure functions the component will consume; unit-tested first, TDD.

**Files:**
- Create: `site/src/lib/filter-panel.ts`
- Test: `site/src/lib/filter-panel.test.ts`

**Interfaces:**
- Consumes: `SearchState` from `@/data/types`; `parseSearchParams` from `./search-params` (test-only, to build states).
- Produces (Task 2 relies on these exact names/signatures):
  - `type PanelSection = "office" | "name" | "status" | "tribe" | "location" | "events"`
  - `sectionCount(state: SearchState, key: PanelSection): number`
  - `advancedActiveCount(state: SearchState): number`
  - `nameExtrasActive(state: SearchState): boolean`
  - `officeOptionsActive(state: SearchState): boolean`

- [ ] **Step 1: Write the failing test**

Create `site/src/lib/filter-panel.test.ts`:

```ts
import { expect, test, describe } from "vite-plus/test"
import {
  sectionCount,
  advancedActiveCount,
  nameExtrasActive,
  officeOptionsActive,
} from "./filter-panel"
import { parseSearchParams } from "./search-params"

const blank = () => parseSearchParams({})

describe("sectionCount", () => {
  test("all zero on a blank state", () => {
    for (const key of [
      "office",
      "name",
      "status",
      "tribe",
      "location",
      "events",
    ] as const) {
      expect(sectionCount(blank(), key)).toBe(0)
    }
  })

  test("office counts selections plus non-default options", () => {
    const state = {
      ...blank(),
      office: ["consul", "praetor"],
      officeMode: "all" as const,
      officeInRange: true,
    }
    expect(sectionCount(state, "office")).toBe(4)
  })

  test("name counts all six name fields", () => {
    const state = {
      ...blank(),
      praenomen: ["Gaius"],
      nomen: ["Iulius"],
      cognomen: ["Caesar"],
      father: ["Gaius"],
      grandfather: ["Gaius"],
      re: "131",
    }
    expect(sectionCount(state, "name")).toBe(6)
  })

  test("status counts status and sex together", () => {
    const state = { ...blank(), status: ["Patrician"], sex: ["Female"] }
    expect(sectionCount(state, "status")).toBe(2)
  })

  test("tribe, location, events count their facet arrays", () => {
    const state = {
      ...blank(),
      tribe: ["Fabia"],
      province: ["Hispania", "Gallia"],
      event: ["triumph"],
    }
    expect(sectionCount(state, "tribe")).toBe(1)
    expect(sectionCount(state, "location")).toBe(2)
    expect(sectionCount(state, "events")).toBe(1)
  })
})

describe("advancedActiveCount", () => {
  test("zero on blank, sums tribe + location + events", () => {
    expect(advancedActiveCount(blank())).toBe(0)
    const state = {
      ...blank(),
      tribe: ["Fabia"],
      province: ["Hispania"],
      event: ["triumph", "augur"],
    }
    expect(advancedActiveCount(state)).toBe(4)
  })
})

describe("nameExtrasActive", () => {
  test("false on blank and when only gens/cognomen set", () => {
    expect(nameExtrasActive(blank())).toBe(false)
    expect(
      nameExtrasActive({ ...blank(), nomen: ["Iulius"], cognomen: ["Caesar"] })
    ).toBe(false)
  })

  test.each([
    ["praenomen", { praenomen: ["Gaius"] }],
    ["father", { father: ["Gaius"] }],
    ["grandfather", { grandfather: ["Gaius"] }],
    ["re", { re: "46a" }],
  ])("true when %s is set", (_label, patch) => {
    expect(nameExtrasActive({ ...blank(), ...patch })).toBe(true)
  })

  test("whitespace-only RE number does not count", () => {
    expect(nameExtrasActive({ ...blank(), re: "  " })).toBe(false)
  })
})

describe("officeOptionsActive", () => {
  test("false on defaults, true when either option is non-default", () => {
    expect(officeOptionsActive(blank())).toBe(false)
    expect(officeOptionsActive({ ...blank(), officeMode: "all" })).toBe(true)
    expect(officeOptionsActive({ ...blank(), officeInRange: true })).toBe(true)
  })
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run (from `site/`): `vp test src/lib/filter-panel.test.ts`
Expected: FAIL — cannot resolve `./filter-panel`.

- [ ] **Step 3: Write the implementation**

Create `site/src/lib/filter-panel.ts`:

```ts
// site/src/lib/filter-panel.ts
// Disclosure logic for the search page's collapsible filter panel: which
// trigger shows what active count, and when tucked-away tiers/fields must
// start revealed so an active filter is never hidden.
import type { SearchState } from "@/data/types"

export type PanelSection =
  | "office"
  | "name"
  | "status"
  | "tribe"
  | "location"
  | "events"

/** Active-filter count shown on a section's trigger pill. */
export function sectionCount(state: SearchState, key: PanelSection): number {
  switch (key) {
    case "office":
      return (
        state.office.length +
        (state.officeMode === "all" ? 1 : 0) +
        (state.officeInRange ? 1 : 0)
      )
    case "name":
      return (
        state.praenomen.length +
        state.nomen.length +
        state.cognomen.length +
        state.father.length +
        state.grandfather.length +
        (state.re.trim() ? 1 : 0)
      )
    case "status":
      return state.status.length + state.sex.length
    case "tribe":
      return state.tribe.length
    case "location":
      return state.province.length
    case "events":
      return state.event.length
  }
}

/** Combined active count of the advanced tier (Tribe, Location, Events);
 * shown on the collapsed "More filters" trigger and used to force the tier
 * open on deep links. */
export function advancedActiveCount(state: SearchState): number {
  return state.tribe.length + state.province.length + state.event.length
}

/** True when any tucked Name field (praenomen, father, grandfather, RE)
 * carries a value — forces the "More name fields" reveal open. */
export function nameExtrasActive(state: SearchState): boolean {
  return (
    state.praenomen.length > 0 ||
    state.father.length > 0 ||
    state.grandfather.length > 0 ||
    state.re.trim().length > 0
  )
}

/** True when either office option is non-default — forces the Office
 * "Options" reveal open. */
export function officeOptionsActive(state: SearchState): boolean {
  return state.officeMode === "all" || state.officeInRange
}
```

Note the one deliberate behavior change vs the old band: the RE-number
contribution uses `state.re.trim()` (the old band counted `"  "` as active).

- [ ] **Step 4: Run the test to verify it passes**

Run: `vp test src/lib/filter-panel.test.ts`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add src/lib/filter-panel.ts src/lib/filter-panel.test.ts
git commit -m "feat: disclosure-logic helpers for collapsible filter panel"
```

---

### Task 2: `FilterPanel` component

The full component, not yet wired into the route (Task 3 does the swap).
There is no component-test infrastructure in this repo — verification for
this task is `vp check` (types + lint) plus the manual pass in Task 3.

**Files:**
- Create: `site/src/components/filter-panel.tsx`
- Modify: `site/src/components/facet-group.tsx` (add optional `listClassName` prop; add `break-inside-avoid` to the item label class)

**Interfaces:**
- Consumes: Task 1's `sectionCount`, `advancedActiveCount`, `nameExtrasActive`, `officeOptionsActive`, `PanelSection`; existing `FacetGroup`, `FacetHierarchyGroup`, `FacetCombobox`, `Checkbox`, `Input`, `cn`; `SearchState`, `FacetValue` from `@/data/types`.
- Produces: `FilterPanel` React component with props identical to the old `FilterBandProps` (Task 3 swaps it in with no prop changes):

```ts
interface FilterPanelProps {
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
  initialFocus?: "office" | "gens"
}
```

- [ ] **Step 1: Add `listClassName` to `FacetGroup`**

In `site/src/components/facet-group.tsx`, add to the props (after `frameless`):

```ts
  /** Extra classes on the checkbox-list container — used by the filter
   * panel to flow long flat lists into CSS columns. */
  listClassName?: string
```

(add `listClassName` to the destructured parameters), change the list
container `<div>` to `<div className={listClassName}>`, and add
`break-inside-avoid` to the item `<label>` class string so rows don't split
across CSS columns:

```tsx
className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6 break-inside-avoid"
```

- [ ] **Step 2: Write `FilterPanel`**

Create `site/src/components/filter-panel.tsx`:

```tsx
// site/src/components/filter-panel.tsx
// Collapsible filter sections for the search page: a header row of pill
// triggers with the single open section expanding in place below it.
// Progressive disclosure at two levels — the advanced tier (Tribe,
// Location, Events) sits behind "More filters", and Name/Office tuck their
// rarely-used fields behind in-section reveals. Replaces the popover band.
import { useState } from "react"
import { ChevronDown } from "lucide-react"
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { FacetCombobox } from "./facet-combobox"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import {
  sectionCount,
  advancedActiveCount,
  nameExtrasActive,
  officeOptionsActive,
  type PanelSection,
} from "@/lib/filter-panel"
import type { SearchState, FacetValue } from "@/data/types"

const BASIC: { key: PanelSection; label: string }[] = [
  { key: "office", label: "Office" },
  { key: "name", label: "Name" },
  { key: "status", label: "Status" },
]
const ADVANCED: { key: PanelSection; label: string }[] = [
  { key: "tribe", label: "Tribe" },
  { key: "location", label: "Location" },
  { key: "events", label: "Events" },
]

interface FilterPanelProps {
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
  /** Section to open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "gens"
}

export function FilterPanel({
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
  initialFocus,
}: FilterPanelProps) {
  // Accordion: at most one section open, the panel owns which.
  const [openKey, setOpenKey] = useState<PanelSection | null>(() =>
    initialFocus === "office"
      ? "office"
      : initialFocus === "gens"
        ? "name"
        : null
  )
  // Deep links into Tribe/Location/Events must not hide their own filters.
  const [advancedRevealed, setAdvancedRevealed] = useState(
    () => advancedActiveCount(state) > 0
  )

  const toggle = (key: PanelSection) =>
    setOpenKey((cur) => (cur === key ? null : key))
  const collapseAdvanced = () => {
    setAdvancedRevealed(false)
    // Closing the tier must not leave an advanced section's body orphaned
    // below a header row that no longer shows its trigger.
    setOpenKey((cur) =>
      cur !== null && ADVANCED.some((s) => s.key === cur) ? null : cur
    )
  }

  const hiddenCount = advancedActiveCount(state)
  const triggers = advancedRevealed ? [...BASIC, ...ADVANCED] : BASIC

  return (
    <div className="rule-lead pt-2.5 pb-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
        {triggers.map(({ key, label }) => (
          <SectionTrigger
            key={key}
            id={`filter-trigger-${key}`}
            controls={`filter-section-${key}`}
            label={label}
            count={sectionCount(state, key)}
            open={openKey === key}
            onClick={() => toggle(key)}
          />
        ))}
        <button
          type="button"
          onClick={() =>
            advancedRevealed ? collapseAdvanced() : setAdvancedRevealed(true)
          }
          className="inline-flex items-center gap-1.5 rounded-[4px] border border-dashed border-border px-2.5 py-1 text-[0.6875rem] font-medium tracking-[0.1em] uppercase text-muted-foreground transition-colors hover:border-muted-foreground hover:text-foreground"
        >
          {advancedRevealed ? "Fewer filters" : "More filters"}
          {!advancedRevealed && hiddenCount > 0 && (
            <span className="text-accent-ink">({hiddenCount})</span>
          )}
        </button>
      </div>

      {openKey !== null && (
        <div
          role="region"
          id={`filter-section-${openKey}`}
          aria-labelledby={`filter-trigger-${openKey}`}
          className="mt-2 max-h-[45vh] overflow-y-auto rounded-[4px] border border-rule-hair p-3"
        >
          <SectionBody
            section={openKey}
            facets={facets}
            officeHierarchy={officeHierarchy}
            provinceHierarchy={provinceHierarchy}
            state={state}
            onUpdate={onUpdate}
          />
        </div>
      )}
    </div>
  )
}

/** Pill trigger, same visual language as the old band: accent ink when the
 * section carries active selections, quiet otherwise. */
function SectionTrigger({
  id,
  controls,
  label,
  count,
  open,
  onClick,
}: {
  id: string
  controls: string
  label: string
  count: number
  open: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      id={id}
      aria-expanded={open}
      aria-controls={controls}
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-[4px] border px-2.5 py-1 text-[0.6875rem] font-medium tracking-[0.1em] uppercase transition-colors",
        count > 0
          ? "border-accent-ink text-accent-ink"
          : "border-border text-muted-foreground hover:border-muted-foreground hover:text-foreground"
      )}
    >
      {label}
      {count > 0 && <span>({count})</span>}
      <ChevronDown
        aria-hidden="true"
        className={cn("h-3 w-3 transition-transform", open && "rotate-180")}
      />
    </button>
  )
}

/** In-section reveal for rarely-used fields. Forced open (with the button
 * hidden) while any tucked field is active, so no active filter is ever
 * invisible; once clicked it stays open for the component's lifetime. */
function InSectionReveal({
  label,
  forced,
  children,
}: {
  label: string
  forced: boolean
  children: React.ReactNode
}) {
  const [revealed, setRevealed] = useState(false)
  if (!revealed && !forced) {
    return (
      <button
        type="button"
        onClick={() => setRevealed(true)}
        className="mt-2 text-xs text-muted-foreground hover:underline"
      >
        {label}
      </button>
    )
  }
  return <>{children}</>
}

function SectionBody({
  section,
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
}: {
  section: PanelSection
  facets: FilterPanelProps["facets"]
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
}) {
  switch (section) {
    case "office":
      return (
        <>
          <FacetHierarchyGroup
            title="Office"
            items={facets.office}
            parentOf={officeHierarchy}
            selected={state.office}
            onChange={(office) => onUpdate({ office })}
            frameless
            hideCounts={state.officeMode === "all" || state.officeInRange}
          />
          <InSectionReveal label="Options…" forced={officeOptionsActive(state)}>
            <div className="mt-3 space-y-2 border-t border-rule-hair pt-3">
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
          </InSectionReveal>
        </>
      )
    case "name":
      return (
        <div className="space-y-3">
          <div className="grid gap-3 md:grid-cols-2">
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
          </div>
          <InSectionReveal
            label="More name fields…"
            forced={nameExtrasActive(state)}
          >
            <div className="grid gap-3 border-t border-rule-hair pt-3 md:grid-cols-2">
              <FacetCombobox
                label="Praenomen"
                values={facets.praenomen}
                selected={state.praenomen}
                onChange={(praenomen) => onUpdate({ praenomen })}
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
          </InSectionReveal>
        </div>
      )
    case "status":
      return (
        <>
          <FacetGroup
            title="Status"
            items={facets.status}
            selected={state.status}
            onChange={(status) => onUpdate({ status })}
            frameless
            listClassName="sm:columns-2 md:columns-3 gap-x-6"
          />
          <p className="micro-label-muted mt-3 mb-1 border-t border-rule-hair pt-3">
            Sex
          </p>
          <FacetGroup
            title="Sex"
            items={facets.sex}
            selected={state.sex}
            onChange={(sex) => onUpdate({ sex })}
            frameless
          />
        </>
      )
    case "tribe":
      return (
        <FacetGroup
          title="Tribe"
          items={facets.tribe}
          selected={state.tribe}
          onChange={(tribe) => onUpdate({ tribe })}
          frameless
          searchable
          listClassName="sm:columns-2 md:columns-3 gap-x-6"
        />
      )
    case "location":
      return (
        <FacetHierarchyGroup
          title="Location"
          items={facets.province}
          parentOf={provinceHierarchy}
          selected={state.province}
          onChange={(province) => onUpdate({ province })}
          frameless
        />
      )
    case "events":
      return (
        <FacetGroup
          title="Life events"
          items={facets.event}
          selected={state.event}
          onChange={(event) => onUpdate({ event })}
          frameless
          listClassName="sm:columns-2 md:columns-3 gap-x-6"
        />
      )
  }
}
```

Note: `FilterPanelProps` is referenced by `SectionBody` via
`FilterPanelProps["facets"]` — keep the interface declaration above both.

- [ ] **Step 3: Type-check and lint**

Run: `vp check`
Expected: PASS. An "unused export FilterPanel" style warning is acceptable
at this point (the route swap lands in Task 3); any other error is not.

- [ ] **Step 4: Run the full unit-test suite**

Run: `vp test`
Expected: PASS — the `FacetGroup` prop addition must not break anything.

- [ ] **Step 5: Commit**

```bash
git add src/components/filter-panel.tsx src/components/facet-group.tsx
git commit -m "feat: collapsible FilterPanel component with progressive disclosure"
```

---

### Task 3: Wire the panel into the search route, delete the popover layer

**Files:**
- Modify: `site/src/routes/index.tsx` (import + one JSX swap, ~lines 14 and 189–202)
- Delete: `site/src/components/filter-band.tsx`, `site/src/components/filter-popover.tsx`

**Interfaces:**
- Consumes: `FilterPanel` from Task 2 (props identical to old `FilterBandProps`).
- Produces: nothing new — the search page now renders `FilterPanel`.

- [ ] **Step 1: Swap the component in the route**

In `site/src/routes/index.tsx`, replace the import

```tsx
import { FilterBand } from "@/components/filter-band"
```

with

```tsx
import { FilterPanel } from "@/components/filter-panel"
```

and in `SearchResults`, replace `<FilterBand` with `<FilterPanel` (all props
stay exactly as they are — `facets`, `state`, `onUpdate`, `officeHierarchy`,
`provinceHierarchy`, `initialFocus`).

- [ ] **Step 2: Delete the dead components**

```bash
git rm src/components/filter-band.tsx src/components/filter-popover.tsx
```

Do NOT touch `src/components/ui/popover.tsx` — `routes/persons.$id.tsx`
still uses it. The `--popover*` CSS variables in `styles.css` stay for the
same reason.

- [ ] **Step 3: Verify types, lint, tests**

Run: `vp check && vp test`
Expected: both PASS, no references to the deleted files anywhere.

- [ ] **Step 4: Manual smoke pass in the dev server**

Run: `vp dev --port 3000` (background), then check on `http://localhost:3000`
— use the browser tools or ask the user if browser automation is unavailable:

1. Landing page → type a query → panel shows Office / Name / Status /
   More filters; no Tribe/Location/Events triggers.
2. Open Office → tree renders in a bordered inset below the row, internal
   scroll if tall; open Name → Office closes (accordion). Clicking Name
   again closes it.
3. Name shows only Gens + Cognomen; "More name fields…" reveals the other
   four; set an RE number, reload the URL → reveal is forced open.
4. "More filters" → Tribe/Location/Events triggers appear, label flips to
   "Fewer filters"; open Tribe, then "Fewer filters" → tier and open
   section both collapse.
5. Select a tribe, collapse the tier → "More filters (1)" shows the count.
   Reload the deep-link URL → the tier starts revealed.
6. Landing "Browse by office" card → panel opens with Office expanded;
   "Browse by gens" → Name expanded.
7. Narrow the window below 768px → same stacked behavior, triggers wrap,
   no bottom sheet.
8. Active-filter chips, era timeline, results, and sort all still work.

- [ ] **Step 5: Commit**

```bash
git add -A src/routes/index.tsx src/components
git commit -m "feat: collapsible filter sections replace popover filter band"
```

---

### Task 4: Regenerate icon raster derivatives from the updated SVG

The new `site/public/icon.svg` is 550×420 (landscape); every raster target
is square, so the artwork is letterboxed by widening the viewBox to a
550×550 square with the content vertically centered ((550−420)/2 = 65px
offset). The SVG uses plain hex fills, so rasterizers handle it directly.
Targets (per spec): transparent background for `favicon.ico`,
`icon-192.png`, `icon-512.png`; opaque paper background `#fefdfa` (the
value already in `manifest.json` `background_color`) for
`apple-touch-icon.png` since iOS composites transparency onto black.

**Files:**
- Modify (regenerate): `site/public/favicon.ico`, `site/public/icon-192.png`, `site/public/icon-512.png`, `site/public/apple-touch-icon.png`
- Commit alongside: `site/public/icon.svg` (already modified in the working tree — do not edit it)

**Interfaces:** none — static assets already wired in `__root.tsx` `head()`.

- [ ] **Step 1: Create the letterboxed square SVG in the scratchpad**

```bash
sed 's|width="550" height="420" viewBox="0 0 550 420"|width="550" height="550" viewBox="0 -65 550 550"|' \
  public/icon.svg > "$SCRATCH/icon-square.svg"
grep -o 'viewBox="[^"]*"' "$SCRATCH/icon-square.svg"
```

Expected: `viewBox="0 -65 550 550"`. If the grep shows the old viewBox, the
SVG's opening tag didn't match the sed pattern — open `public/icon.svg`,
read the actual `<svg …>` attributes, and adjust the sed accordingly (the
result must be a square viewBox with the 420-tall content centered).

- [ ] **Step 2: Rasterize the PNG targets with sharp-cli via vp dlx**

```bash
cd "$SCRATCH"
vp dlx sharp-cli -i icon-square.svg -o icon-512.png resize 512 512
vp dlx sharp-cli -i icon-square.svg -o icon-192.png resize 192 192
vp dlx sharp-cli -i icon-square.svg -o icon-32.png  resize 32 32
vp dlx sharp-cli -i icon-square.svg -o icon-16.png  resize 16 16
vp dlx sharp-cli -i icon-square.svg -o apple-touch-icon.png \
  resize 180 180 flatten --background "#fefdfa"
```

If sharp-cli's argument syntax rejects these (it has changed between major
versions), run `vp dlx sharp-cli --help` and adapt — the operations needed
are exactly: SVG input, PNG output, resize to the stated square size, and
(apple-touch only) flatten onto `#fefdfa`.

- [ ] **Step 3: Build favicon.ico from the 32px and 16px PNGs**

```bash
cd "$SCRATCH"
vp dlx png-to-ico icon-32.png icon-16.png > favicon.ico
```

- [ ] **Step 4: Verify and move into public/**

```bash
file "$SCRATCH"/icon-512.png "$SCRATCH"/icon-192.png "$SCRATCH"/apple-touch-icon.png "$SCRATCH"/favicon.ico
```

Expected: PNGs report their exact square dimensions (512×512, 192×192,
180×180); the ico reports MS Windows icon resource with 2 icons. Then view
`icon-192.png` (Read tool renders images) and confirm the full artwork is
visible, centered, with letterbox margins top/bottom and nothing cropped.

```bash
cp "$SCRATCH"/icon-512.png "$SCRATCH"/icon-192.png "$SCRATCH"/apple-touch-icon.png "$SCRATCH"/favicon.ico \
  /Users/gillisandrew/Projects/gillisandrew/dprr-data/site/public/
```

- [ ] **Step 5: Commit the SVG together with all four derivatives**

```bash
cd /Users/gillisandrew/Projects/gillisandrew/dprr-data
git add site/public/icon.svg site/public/favicon.ico site/public/icon-192.png site/public/icon-512.png site/public/apple-touch-icon.png
git commit -m "feat: updated site icon with regenerated raster derivatives"
```

---

### Task 5: Icon replaces the "DPRR" wordmark in the nav

**Files:**
- Modify: `site/src/components/site-header.tsx` (the home `Link`, lines 16–18)

**Interfaces:**
- Consumes: `site/public/icon.svg` (via `import.meta.env.BASE_URL`, matching the favicon wiring in `__root.tsx`).

- [ ] **Step 1: Swap the wordmark for the icon**

In `site/src/components/site-header.tsx`, replace

```tsx
        <Link to="/" className="font-heading font-bold">
          DPRR
        </Link>
```

with

```tsx
        <Link to="/" aria-label="DPRR — home" className="self-center">
          <img
            src={`${import.meta.env.BASE_URL}icon.svg`}
            alt=""
            className="-my-1 block h-7 w-auto"
          />
        </Link>
```

Why these classes: the nav row uses `items-baseline`, which an image can't
sensibly baseline-align to, so the logo link self-centers; `h-7` (28px)
matches the old text-line presence with width following the SVG's 550:420
ratio (~37px); `-my-1` cancels the extra height so `py-3` on the nav keeps
the header exactly as tall as before. The link carries the accessible name;
the img stays `alt=""` so screen readers don't announce it twice.

- [ ] **Step 2: Verify header height and rendering**

Run: `vp check`, then in the dev server confirm: the icon renders in the
nav at every route, the header is not visibly taller than before (compare
the hairline rule position), it looks right in light and dark themes, and
keyboard-tabbing to the logo announces "DPRR — home".

- [ ] **Step 3: Commit**

```bash
git add src/components/site-header.tsx
git commit -m "feat: site icon replaces DPRR wordmark in nav"
```

---

### Task 6: Final verification and build

**Files:** none new.

- [ ] **Step 1: Full check, test, and production build**

Run (from `site/`): `vp check && vp test && vp run build`
Expected: all PASS; the build (which includes prerendering and the sitemap
normalize script) completes without errors.

- [ ] **Step 2: Spot-check the built output**

```bash
grep -o 'icon[^"]*\.\(svg\|png\|ico\)' dist/client/index.html | sort -u
```

Expected: the icon link tags still reference `icon.svg`, `favicon.ico`,
`apple-touch-icon.png` under the base path, and the prerendered search page
HTML contains the new filter trigger labels ("More filters").

- [ ] **Step 3: Report**

Summarize for the user: what changed, the manual-QA items already exercised
in Tasks 3 and 5, and anything observed that needs their eye (e.g. icon
legibility at 16px in a real browser tab, dark-theme contrast of the new
artwork). Do not claim visual QA the executor could not actually perform.
