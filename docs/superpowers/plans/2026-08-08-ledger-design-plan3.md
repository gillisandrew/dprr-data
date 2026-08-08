# Ledger Design Pass (Plan 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restyle the entire site into the Editorial Ledger system — rules and typographic hierarchy instead of cards — including the registry-header person page (deleting the rail), with zero behavior change.

**Architecture:** One token/utility layer in `styles.css` (two rule weights, micro-labels, tabular numerals, warm paper + dark derivation), then per-surface restyles that only touch className/markup, never logic. The person page absorbs the rail into a registry strip. All 101 existing tests pass unmodified (except deleting any test covering removed rail code — none exist).

**Tech Stack:** Tailwind CSS 4 (@theme tokens + utilities), existing Lora/Inter fonts, TanStack Start routes. No new dependencies.

## Global Constraints

- Zero behavior change: search semantics, URL state, data pipeline, routes, progressive-disclosure interactions untouched. `vp test src` passes with NO test-file edits.
- Exactly two rule weights: `--rule-lead` (2px near-foreground) and `--rule-hair` (1px low-contrast). No card borders/rounded panels/fills except interactive surfaces (inputs, selects, popovers/suggestion lists) and note prose blocks.
- Type roles: Lora (`font-heading`) ONLY for person/entity names and page titles; Inter for all else. Micro-labels: uppercase, tracking `.1em`, ~11px, accent color. `tabular-nums` site-wide.
- `--primary` is the only non-neutral hue. Status markers = small-caps text, never Badge pills.
- Every restyled surface verified in light AND dark themes.
- All site commands from `site/` via `vp`; style: no semicolons, double quotes, 2-space indent; `vp check` fully green before every commit; stage explicit paths.
- Build gates unchanged: ~6,130 pages, landing < 100 KB.

---

### Task 1: Token + utility layer

**Files:**
- Modify: `site/src/styles.css`

**Interfaces:**
- Produces CSS custom properties `--rule-lead`, `--rule-hair` (both themes) and utility classes consumed by every later task: `.rule-lead`, `.rule-hair`, `.micro-label`, `.ledger-row`, `.year-col`, `.small-caps`.

- [ ] **Step 1: Add tokens and utilities**

In `site/src/styles.css`:

1. Warm the paper — in `:root`, change `--background` to `oklch(0.993 0.003 83)` and `--card`/`--popover` to match. Reduce `--radius` to `0.25rem`. Add:

```css
  --rule-lead: oklch(0.22 0.008 49.3);
  --rule-hair: oklch(0.9 0.008 60);
```

2. In `.dark`, add (background stays as-is — already warm near-black):

```css
  --rule-lead: oklch(0.85 0.005 67.8);
  --rule-hair: oklch(1 0 0 / 12%);
```

3. In the `@theme inline` block, register them as border colors so `border-rule-lead`/`border-rule-hair` utilities exist:

```css
  --color-rule-lead: var(--rule-lead);
  --color-rule-hair: var(--rule-hair);
```

4. After the theme blocks, add base + component classes:

```css
body {
  font-variant-numeric: tabular-nums;
}

@layer components {
  .rule-lead {
    border-bottom: 2px solid var(--rule-lead);
  }
  .rule-hair {
    border-bottom: 1px solid var(--rule-hair);
  }
  .micro-label {
    font-size: 0.6875rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--primary);
    font-weight: 500;
  }
  .micro-label-muted {
    font-size: 0.6875rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--muted-foreground);
    font-weight: 500;
  }
  .ledger-row {
    border-bottom: 1px solid var(--rule-hair);
    padding-block: 0.375rem;
  }
  .ledger-row:hover {
    background: color-mix(in oklch, var(--foreground) 3%, transparent);
  }
  .year-col {
    min-width: 3.25rem;
    text-align: right;
    color: var(--primary);
    flex-shrink: 0;
  }
  .small-caps {
    font-variant-caps: all-small-caps;
    letter-spacing: 0.06em;
  }
}
```

- [ ] **Step 2: Verify nothing broke**

Run: `cd site && vp check && vp test src` — green, no test edits. Dev server: site renders as before apart from the slightly warmer background and smaller radii; check one page in dark mode.

- [ ] **Step 3: Commit**

```bash
git add site/src/styles.css
git commit -m "feat: add ledger design tokens and utility classes"
```

---

### Task 2: Search control band + fasti result rows

**Files:**
- Modify: `site/src/routes/index.tsx` (full-layout chrome + loading-state chrome)
- Modify: `site/src/components/results-list.tsx`
- Modify: `site/src/components/fasti-row.tsx`
- Modify: `site/src/components/active-filter-chips.tsx`

**Interfaces:**
- Consumes Task 1 utilities. All props/handlers unchanged everywhere.

- [ ] **Step 1: Control band in `index.tsx`**

In the full-layout render (`SearchResults`), replace the stacked SearchInput / chips / (ResultsList carries count+sort) intro with one band. Structure (adapt surrounding JSX minimally — handlers unchanged):

```tsx
      <div className="rule-lead pb-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="min-w-64 flex-1">
            <SearchInput value={state.q} onChange={(q) => updateState({ q })} />
          </div>
          <ResultsHeader
            count={results.length}
            sort={state.sort}
            hasQuery={state.q.trim().length > 0}
            onSortChange={(sort) => updateState({ sort })}
          />
        </div>
        <div className="mt-2 empty:hidden">
          <ActiveFilterChips state={state} onRemove={updateState} onClearAll={clearAll} />
        </div>
      </div>
```

`ResultsHeader` is the count + sort control extracted from `results-list.tsx` (move the existing JSX — count text + `<select>` — into a named export `ResultsHeader` in `results-list.tsx`; `ResultsList` keeps only the row list + empty state). Apply the same band styling to the loading-state chrome so the transition doesn't jump.

- [ ] **Step 2: Fasti rows**

`fasti-row.tsx` — same data, ledger dress:

```tsx
  return (
    <Link
      to="/persons/$id"
      params={{ id: person.id }}
      className="ledger-row block px-1 transition-colors"
    >
      <p className="font-heading text-[0.95rem] leading-snug">
        {name}
        {person.highestOffice && (
          <span className="ml-2 font-sans text-sm text-primary">
            — {person.highestOffice}
          </span>
        )}
      </p>
      <p className="text-xs leading-snug text-muted-foreground">
        {person.filiation && <span>{person.filiation} · </span>}
        <EraRange from={person.eraFrom} to={person.eraTo} />
        {person.nomen && (
          <>
            {" · "}
            {gensSlug ? (
              <Link
                to="/gentes/$slug"
                params={{ slug: gensSlug }}
                className="relative z-10 hover:text-primary hover:underline"
              >
                gens {person.nomen}
              </Link>
            ) : (
              <span>gens {person.nomen}</span>
            )}
          </>
        )}
      </p>
    </Link>
  )
```

(Keep the existing stretched-link/`displayName`/empty-slug logic exactly; only classNames and element order per this shape.) In `results-list.tsx`, remove any remaining per-row borders/cards so `.ledger-row` provides the only separators; empty state stays.

- [ ] **Step 3: Chips**

`active-filter-chips.tsx`: replace `Badge variant="secondary"` chips with ruled text chips — keep all labels/removal logic:

```tsx
        <button
          key={chip.label}
          onClick={chip.onRemove}
          className="rule-hair inline-flex items-center gap-1 pb-0.5 text-xs text-foreground hover:text-primary"
        >
          {chip.label}
          <X className="h-3 w-3 text-muted-foreground" />
        </button>
```

("Clear all" keeps its current style.)

- [ ] **Step 4: Verify + commit**

`vp check && vp test src` green. Dev server (light+dark): one band closed by the 2px rule, rows separated by hairlines only, no card borders in results.

```bash
git add site/src/routes/index.tsx site/src/components/results-list.tsx site/src/components/fasti-row.tsx site/src/components/active-filter-chips.tsx
git commit -m "feat: ledger control band and ruled fasti rows"
```

---

### Task 3: Sidebar + timeline frame

**Files:**
- Modify: `site/src/components/facet-sidebar.tsx`, `facet-group.tsx`, `facet-hierarchy-group.tsx`, `advanced-search.tsx`, `facet-combobox.tsx`, `era-timeline.tsx`

**Interfaces:** consumes Task 1 utilities; every prop, handler, open/close and auto-open behavior unchanged.

- [ ] **Step 1: Facet groups**

In `facet-group.tsx` and `facet-hierarchy-group.tsx`: the `CollapsibleTrigger` becomes a micro-label row —

```tsx
      <CollapsibleTrigger className="micro-label rule-hair flex w-full items-center justify-between pb-1 pt-3">
        {title}
        <ChevronRight className={cn("h-3 w-3 transition-transform", open && "rotate-90")} />
      </CollapsibleTrigger>
```

Checkbox rows tighten: `py-0.5 text-[0.8125rem] leading-6` (~28px rows). In the hierarchy tree, structural (count-null) node labels use `micro-label-muted` (keep their checkboxes and toggle wiring exactly). Remove any `bg-*`/rounded/card classes from group bodies.

- [ ] **Step 2: Tier triggers + advanced tier**

`facet-sidebar.tsx`: "More filters" / "Advanced search" triggers become ruled rows with +/− (text, not icon):

```tsx
      <CollapsibleTrigger className="rule-hair flex w-full items-center justify-between pb-1 pt-4 text-sm font-medium">
        More filters
        <span className="text-muted-foreground">{tier2Open ? "−" : "+"}</span>
      </CollapsibleTrigger>
```

`advanced-search.tsx` and `facet-combobox.tsx`: labels become `micro-label-muted`; the suggestion list KEEPS its border/background (interactive surface); chips inside the combobox match Task 3 chip style… they use Badge — switch to the Task-2 chip button style.

- [ ] **Step 3: Timeline frame**

`era-timeline.tsx`: the band's container (in `index.tsx` if the wrapper lives there) gets `rule-hair` top and bottom via `border-y` equivalents — concretely wrap: `<div className="border-y border-rule-hair py-2">` and remove any other border/box classes around the SVG; the "Time period" heading becomes `micro-label`. SVG internals and inputs unchanged.

- [ ] **Step 4: Verify + commit**

`vp check && vp test src` green; dev server light+dark: sidebar reads as ruled groups, tiers auto-open behavior intact (deep-link `?officeMode=all&office=consul` still opens Tier 3).

```bash
git add site/src/components/facet-sidebar.tsx site/src/components/facet-group.tsx site/src/components/facet-hierarchy-group.tsx site/src/components/advanced-search.tsx site/src/components/facet-combobox.tsx site/src/components/era-timeline.tsx site/src/routes/index.tsx
git commit -m "feat: ruled facet sidebar and timeline frame"
```

---

### Task 4: Landing restyle

**Files:**
- Modify: `site/src/components/search-landing.tsx`

- [ ] **Step 1: Ruled cards**

Cards lose the border box; each gets a lead top rule:

```tsx
          <button
            key={c.key}
            onClick={() => onBrowse(c.key)}
            className="border-t-2 border-rule-lead pt-3 text-left transition-colors hover:text-primary"
          >
            <p className="font-heading text-base font-semibold">Browse by {c.title}</p>
            <p className="mt-1 text-xs text-muted-foreground">{c.blurb}</p>
          </button>
```

Title stays `font-heading`; subtitle text unchanged; input keeps its interactive border.

- [ ] **Step 2: Verify + commit**

`vp check` green; landing renders correctly both themes; typing/browse behavior unchanged.

```bash
git add site/src/components/search-landing.tsx
git commit -m "feat: ruled landing cards"
```

---

### Task 5: Person page — registry header, rail deletion

**Files:**
- Create: `site/src/components/person-registry.tsx`
- Modify: `site/src/routes/persons.$id.tsx`
- Modify: `site/src/components/section.tsx`
- Delete: `site/src/components/person-rail.tsx`

**Interfaces:**
- Produces `<PersonRegistry person={Person} />` — the wrapping micro-labeled field strip.
- `groupRelationships` currently lives in (and is exported from) `person-rail.tsx` and is consumed by the route — MOVE it (with its display-name sort) into `persons.$id.tsx` as a local function, verbatim.
- `Section` becomes the micro-label + hairline header (all existing consumers keep working: title + optional count + children).

- [ ] **Step 1: Registry strip component**

`site/src/components/person-registry.tsx`:

```tsx
// site/src/components/person-registry.tsx
import { Link } from "@tanstack/react-router"
import { slugify } from "@/lib/slug"
import type { Person } from "@/data/types"

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <p className="micro-label-muted">{label}</p>
      <p className="truncate text-sm">{children}</p>
    </div>
  )
}

/** Wrapping identity strip under the person header — replaces the rail's IdentityCard/LinksCard. */
export function PersonRegistry({ person }: { person: Person }) {
  const nomenSlug = person.nomen ? slugify(person.nomen) : ""
  return (
    <div className="rule-hair flex flex-wrap gap-x-8 gap-y-2 pb-3 pt-2">
      {person.praenomen && <Field label="Praenomen">{person.praenomen}</Field>}
      {person.nomen && (
        <Field label="Nomen">
          {nomenSlug ? (
            <Link to="/gentes/$slug" params={{ slug: nomenSlug }} className="text-primary hover:underline">
              {person.nomen}
            </Link>
          ) : (
            person.nomen
          )}
        </Field>
      )}
      {person.cognomen && <Field label="Cognomen">{person.cognomen}</Field>}
      {person.filiation && <Field label="Filiation">{person.filiation}</Field>}
      {person.reNumber && <Field label="RE">{person.reNumber}</Field>}
      {person.tribes.length > 0 && (
        <Field label="Tribe">
          {person.tribes.map((t, i) => (
            <span key={t}>
              {i > 0 && ", "}
              <Link to="/tribes/$slug" params={{ slug: slugify(t) }} className="text-primary hover:underline">
                {t}
              </Link>
            </span>
          ))}
        </Field>
      )}
      <Field label="DPRR ID">
        <span className="font-mono text-xs">{person.id}</span>
      </Field>
      {person.concordances.length > 0 && (
        <Field label="Links">
          {[...new Map(person.concordances.map((c) => [c.system, c])).values()].map((c, i) => (
            <span key={c.system}>
              {i > 0 && " · "}
              <a href={c.uri} target="_blank" rel="noopener noreferrer" className="text-primary capitalize hover:underline">
                {c.system}
              </a>
            </span>
          ))}
        </Field>
      )}
    </div>
  )
}
```

NOTE: the Links field shows one link per system (first URI); persons can carry multiple URIs per system — the FULL list must stay reachable: keep the existing External Links content as a final body ledger section "Links" listing every URI (move `LinksCard`'s list markup into the route restyled as a ledger section) — the strip is the quick-access row.

- [ ] **Step 2: Section header restyle**

`section.tsx` — the section wrapper becomes:

```tsx
    <section className="mt-7">
      <h2 className="micro-label rule-hair flex items-baseline justify-between pb-1">
        {title}
        {count !== undefined && (
          <span className="micro-label-muted">{count}</span>
        )}
      </h2>
      <div className="mt-2">{children}</div>
    </section>
```

(Keep the component's existing props/signature; drop any collapse behavior only if unused — check consumers first.)

- [ ] **Step 3: Route restructure**

`persons.$id.tsx`:
- Header: keep the CR-era name + strong line; close with `rule-lead` (`<header className="rule-lead pb-3">`); status renders as `<span className="small-caps text-muted-foreground">patrician</span>` (drop the Badge imports if now unused).
- Insert `<PersonRegistry person={person} />` directly under the header; DELETE the `lg:grid` two-column wrapper, the `<aside>`, the `lg:hidden` mobile Identity copy, and the PersonRail import/usage.
- Move `groupRelationships` (from person-rail.tsx) into this file verbatim; move the Dates entries and full Links list from the rail cards into body `Section`s ("Dates": year-col rows `value · type · ? · source`; "Links": every concordance URI grouped by system, alphabetical — reuse the rail's markup restyled with `ledger-row`).
- Career/Relationship entries: swap `border-l-2 pl-4` for ledger rows with year columns:

```tsx
      <div className="ledger-row flex gap-3">
        <span className="year-col text-sm">
          {/* existing DateDisplay/EraRange/"undated" expression unchanged */}
        </span>
        <div className="min-w-0 flex-1">{/* existing office/relationship content unchanged */}</div>
      </div>
```

- Delete `site/src/components/person-rail.tsx` once nothing imports it (`grep -rn "person-rail" site/src` → empty).

- [ ] **Step 4: Verify + commit**

`vp check && vp test src` green with NO test edits. Dev server light+dark, wide+narrow: registry wraps; exactly one identity rendering (`grep -c "Praenomen" on the page HTML` sanity); career notes and relationship references expanders still work (LUCR0010, PROC4180); JSON-LD/meta head unchanged (`git diff` shows no head() hunks).

```bash
git add site/src/components/person-registry.tsx site/src/components/section.tsx 'site/src/routes/persons.$id.tsx'
git rm site/src/components/person-rail.tsx
git commit -m "feat: registry-header person page, delete rail"
```

---

### Task 6: Reference pages, directory, header/footer, person cards

**Files:**
- Modify: `site/src/routes/gentes.index.tsx`, `tribes.index.tsx`, `provinces.index.tsx`, `offices.index.tsx`, `directory.tsx`
- Modify: `site/src/routes/gentes.$slug.tsx`, `tribes.$slug.tsx`, `provinces.$slug.tsx`, `offices.$slug.tsx`
- Modify: `site/src/components/person-card.tsx`, `site-header.tsx`, `site-footer.tsx`

- [ ] **Step 1: Index pages** — every index page title block closes with `rule-lead`; rows become justified ledger rows (keep Link wrappers/params):

```tsx
            <Link … className="ledger-row group flex items-baseline justify-between gap-2 px-1">
              <span className="font-heading group-hover:text-primary">{name}</span>
              <span className="text-sm text-muted-foreground">{count}</span>
            </Link>
```

Offices keeps its category grouping; category `<h2>`s become `micro-label` + hairline.

- [ ] **Step 2: Detail pages** — holder/assertion lists adopt the career-row anatomy (year-col + content + source), replacing `border-l-2 pl-4`. `person-card.tsx` (tribe/gens member lists) restyles from bordered card to a two-line ledger row (same anatomy as fasti-row; keep its distinct props/links).

- [ ] **Step 3: Header/footer** — `site-header.tsx`: `border-b` → `rule-hair` equivalents; nav active state = `text-primary` (no weight jump). `site-footer.tsx`: `border-t` hairline.

- [ ] **Step 4: Verify + commit**

`vp check && vp test src` green; spot-check `/gentes/`, `/offices/`, `/offices/consul`, `/tribes/fabia`, `/directory/` both themes.

```bash
git add site/src/routes site/src/components/person-card.tsx site/src/components/site-header.tsx site/src/components/site-footer.tsx
git commit -m "feat: ledger treatment for reference pages, directory, and chrome"
```

---

### Task 7: Full sweep + build gates

**Files:** none — verification; fix-forward only trivial styling misses.

- [ ] **Step 1:** `cd site && vp check && vp test` — fully green, `git diff --stat` shows zero test-file changes across the plan (except none needed for deleted rail).
- [ ] **Step 2:** `vp build 2>&1 | tail -3`; page count ~6,130; `wc -c dist/client/index.html` < 100 KB; expected 8 province warnings only.
- [ ] **Step 3:** Serve `dist/client` under `/dprr-data/` prefix; visual sweep of landing, search (with filters), person (LUCR0010, PROC4180, CLUE4548), one page per reference family, directory — light + dark (toggle `.dark` class via devtools or the site toggle if present), desktop + ~390px.
- [ ] **Step 4:** Confirm no `Badge` renders remain on restyled surfaces (`grep -rn "Badge" site/src/routes site/src/components` — remaining uses must be justified interactive surfaces or removed imports) and exactly two border weights in the restyled surfaces.
- [ ] **Step 5:** Report with screenshots/curl evidence; commit any straggler fixes as `style:` commits.

---

## Execution Notes

- Order 1→7; Tasks 2/3/4 are independent after 1 (parallel-safe on disjoint files — except both 2 and 3 touch `index.tsx`: run 2 before 3, or merge their index.tsx edits into whichever runs last).
- Task 5 (person page) and Task 6 (reference pages) are independent after Task 1; Task 6's person-card restyle is independent of Task 2's fasti-row.
- The spec (docs/superpowers/specs/2026-08-08-ledger-design-design.md) governs semantics; this plan pins file layout and class names.
