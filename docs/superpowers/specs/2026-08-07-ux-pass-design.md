# DPRR Site UX Pass (Plan 2) — Design Spec

## Overview

The second and final pre-launch plan for the DPRR static site: a UX pass over the complete Plan-1 feature set. It restructures the search experience around progressive disclosure, turns the era filter into an attestation-density timeline, adds researcher-grade office queries, reorganizes the person page's visual hierarchy, and gives every list on the site a stated, deterministic order. Design choices were made interactively against mockups on 2026-08-07 (see `.superpowers/brainstorm/` session).

## Goals

- A first-time visitor sees an inviting, fast landing — not a wall of 190 checkboxes and a 4 MB page
- Researchers can express the queries a prosopography exists for: office held *in* a time range, career conjunctions, subtree selection
- The person page reads as a structured scholarly record, career first
- No list anywhere renders in incidental (parse/insertion) order
- Everything stays URL-addressable and fully static

## Non-Goals

- Print/PDF export of results (deferred)
- Linked Data Fragments / SPARQL (deferred from Plan 1)
- New data-derived features beyond the life-events facet
- Custom domain, analytics, sitemap

## 1. Search experience

### 1.1 Two-state landing (progressive disclosure, level 0)

The search page has two states keyed off the URL:

- **Landing state** (no search/filter params): a hero search box ("Search 4,876 persons…") plus three "Browse by" entry cards — **Office**, **Time**, **Gens**. Selecting a card transitions to the full layout with that facet opened (Office: hierarchy tree expanded; Time: timeline focused; Gens: gens facet open). Typing transitions on first keystroke. The landing needs no search data — it must render from HTML alone.
- **Full layout** (any param present, or after first interaction): results list + tiered sidebar. Deep links land here directly.

### 1.2 Tiered sidebar (progressive disclosure, levels 1–3)

- **Tier 1 (always visible):** Office hierarchy tree, density timeline.
- **Tier 2 — "More filters" (one click):** Gens, Tribe, Location, Sex, Status, Life events.
- **Tier 3 — "Advanced search":** office-in-range toggle, office AND/OR mode, office combobox, name-part fields (praenomen, nomen, cognomen, RE) as comboboxes fed from client-side data.
- A tier auto-opens when the URL carries one of its params — shared links never hide their own filters. Disclosure state is otherwise ephemeral (not in the URL).

### 1.3 Advanced office queries

- **Office held in range** (`officeInRange=true`): with office selections and a time range both active, the toggle switches matching from person-level (lifetime overlaps range AND ever held office) to assertion-level (a dated assertion of a selected office falls inside the range). Requires per-person career tuples in the search data (§1.5).
- **AND mode** (`officeMode=all`): person must hold every selected office; default remains OR.
- **Subtree selection:** selecting a parent node in the office tree stores the parent value as one selection/chip ("any Priesthood"); descendant expansion happens at filter time via the shipped hierarchy map.
- **Office combobox:** type-ahead selection in Tier 3; same selection model as the tree.

### 1.4 Attestation-density timeline

- Replaces the era number-inputs pair: an **area curve with two drag handles** (chosen over a brushable histogram). Handles write the existing `eraFrom`/`eraTo` params.
- Density = count of dated office-assertions per 5-year bin, computed at build time into a small static JSON. With other filters active, the curve recomputes client-side from career tuples to show density within the filtered set.
- Exact-year inputs remain beneath the curve for precision.

### 1.5 Search-data payload diet

Today the search page embeds ~3.6 MB of data plus ~250 KB of hidden crawl links. Changes:

- Search data (summaries, MiniSearch index, hierarchies, career tuples, histogram) is emitted at build time as static JSON assets, fetched on first search interaction and cached. The landing state ships no data.
- Summaries gain `reNumber`, `filiation` (for fasti lines, §3.1), `lifeEvents` (§2), and compact career tuples `[officeId, dateStart, dateEnd][]` with an office-name table (for §1.3/§1.4).
- Hidden crawl links move to a dedicated prerendered **`/directory/`** page linked from the footer — crawler reachability preserved, search page unburdened.

### 1.6 BC-dominant date handling

- Era values are negative (BC) in 92% of cases; assertion dates are BC in all but 7 of 23,330. Year inputs become **unsigned year + BC/AD selector, defaulting BC** ("509 [BC]" → internal −509). URLs keep signed integers, so existing deep links work unchanged.
- Timeline axis is BC-native: spans the actual assertion range (~600 BC → 31 BC), labels without minus signs; the 7 AD outlier assertions fold into the final bin.
- **Year-zero policy: 0 = 1 BC** (astronomical convention) everywhere — the formatter already does this; histogram binning and input conversion follow the same rule.
- Sort controls say "Earliest first / Latest first", never "Date ↑/↓".

## 2. Life-events facet

Summaries gain `lifeEvents: string[]` derived from DateInformation types (the exact value list comes from what `misc.ttl`'s DateTypes actually yield — birth, death, violent death, etc.). Tier-2 checkbox facet with counts; URL param `event=` (comma-separated, encoded like the other multi-value facets).

## 3. Results presentation

### 3.1 Fasti result lines

Two-line entries: **line 1** — display name, RE number in parens, highest office with date ("cos. 508"); **line 2** — filiation (when present), era range, gens.

### 3.2 Sorting

Default **date ascending** (`eraFrom ?? eraTo`, undated last). Sort control: Earliest first / Latest first / Name A–Z; **Relevance** appears (and becomes default) only when a text query is active.

## 4. Person page

Career-main + identity-rail layout (chosen from mockups):

- **Main column:** Career (chronological, uncertainty conventions kept, per-assertion scholarly notes collapsed behind expanders), then person-level Notes as prose.
- **Sticky right rail:** Identity card (name parts, filiation, RE, tribe links, status badges), Family (relationships), Dates, External links — compact and scannable.
- **Header:** name plus one strong line of era range + highest office, replacing the loose badge scatter.
- **Mobile:** Identity card stacks under the header, career next, remaining rail sections after.

## 5. Site-wide ordering rules

Every list gets a deterministic order via a shared comparator helper (single source for "undated last"):

| List | Order |
|------|-------|
| Search results (no query) | Date ascending (`eraFrom ?? eraTo`), undated last, name tiebreak |
| Search results (query) | Relevance |
| Career entries | Chronological (existing, Plan 1) |
| Relationships | Grouped by type (types alphabetical), person name within |
| Date information | Year ascending |
| Person notes | By note type |
| Concordance systems | Alphabetical |
| Facet values | Count descending, name tiebreak (existing) |
| Reference index pages | Alphabetical / grouped (existing, Plan 1) |

## 6. "Location" relabel

The province facet and page headings adopt DPRR's term **"Location"** (the canonical set includes courts, the fleet, and Rome — "Province" is wrong for those). URLs stay `/provinces/…`.

## 7. Testing

New logic lands as pure, unit-tested modules: sort comparators, office-in-range filtering, subtree expansion, histogram binning, BC/AD input conversion (including year zero), fasti line formatting, and URL round-trips extended to `officeMode`, `officeInRange`, `event`. UI wiring is verified against the dev server and the prerendered build (landing state must render without JS-fetched data; `/directory/` must keep the crawl complete — build page count is the regression test).

## 8. Visual execution

New components (landing cards, timeline, identity rail, drawer/tiers) are built with the frontend-design skill against the existing Lora/Inter + token system so the site reads as one designed object. No broad restyle beyond the surfaces this spec touches.

## Deferred

Print/PDF export; structured relationship-context lines for obscure persons in results (original DPRR shows "mother of C. Marcius (50) Coriolanus"); "visible-count cap" is subsumed by the tiered sidebar design.
