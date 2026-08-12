# Semantics & Source-Surfacing Pass — Design

**Date:** 2026-08-12
**Status:** Approved

## Problem

The site strips or ignores predicates that carry real scholarly/display
value, presents domain terms (novus, nobilis, tribe, RE number) with no
explanation, hides the fact that selecting a parent office filters its
whole subtree, and reduces status assertions to bare small-caps labels with
no dates, sources, or notes. Two smaller defects ride along: the SPARQL
editor's syntax highlighting is illegible in dark mode, and scrollbar
appearance/disappearance shifts the centered layout between routes.

## Decisions (approved in brainstorming)

- Re-shard from `~/Downloads/dprr.ttl` (Apr 1 dump), keeping the three
  ordering predicates `shard.py` currently strips.
- Parse + display: career detail (`hasOfficeXref`, `hasDateSourceText`),
  status completeness (`hasNovusNotes`, `hasStatusAssertionNote`), identity
  extras (`hasOrigin`, per-name-part uncertainty flags). Deep cuts
  (`hasReNumberOld`, `hasAltPraenomen`) explicitly skipped.
- Explanations: one glossary module drives ⓘ popovers AND an About-page
  glossary section.
- Tree select-all: implied-check descendants.

## Workstream 1: Data pipeline (re-shard)

`shard.py` `STRIP_PREDICATES` currently drops, as "UI display ordering":

- `hasPosition` — ~9,800 instances on `PostAssertion`: DPRR's canonical
  ordering of a person's career entries.
- `hasOrderNumber` — 44 instances on `Relationship` authority types:
  curated ordering of relationship groups.
- `hasRelationshipNumber` — 351 instances on relationship assertions:
  ordering within a group.

Changes:

1. Remove the three from `STRIP_PREDICATES`; the comment block gains a note
   that these are kept as display-ordering data consumed by the site.
2. Re-run `shard.py ~/Downloads/dprr.ttl .` (canonicalized output — diff
   shows only real changes).
3. If the Apr 1 dump carries upstream content changes beyond the three
   predicates, commit those first as `data: upstream refresh (YYYY-MM
   dump)`, then the predicate-keep regeneration as its own commit — two
   reviewable diffs.
4. README's "changes applied by shard.py" list updated to match.

## Workstream 2: Parser + person page

### Types & parser (`site/src/data/types.ts`, `parse-persons.ts`)

- `PostAssertion` gains `position: number | null` (from `hasPosition`),
  `officeXref: string | null` (from `hasOfficeXref`), `dateSourceText:
  string | null` (from `hasDateSourceText`).
- New `StatusAssertion` shape replacing the current `statuses: string[]`
  reduction — per assertion: status name, `dateStart`, `dateEnd`,
  date-uncertainty flags, `isUncertain`, `secondarySource`, and notes (via
  `hasStatusAssertionNote`, same note shape as post-assertion notes:
  text, type, source). The header summary keeps using the deduplicated
  status names.
- `Person` gains `origin: string | null` (from `hasOrigin`), `novusNotes:
  string | null` (from `hasNovusNotes`), and name-part uncertainty flags:
  `isNomenUncertain`, `isCognomenUncertain`, `isPraenomenUncertain`,
  `isFiliationUncertain`, `isOtherNamesUncertain` (all default false;
  upstream only asserts them when true).
- `Relationship` gains `orderNumber: number | null` (the relationship
  type's `hasOrderNumber`, joined from the reference data) and
  `relationshipNumber: number | null` (from `hasRelationshipNumber`).

### Display (`site/src/routes/persons.$id.tsx` + components)

- **Career sort:** compound key `(position ?? Infinity, dateStart ??
  dateEnd ?? Infinity)` — positioned assertions in DPRR's canonical order
  first, then positionless ones chronologically.
- **Career entry:** `officeXref` renders muted beside the office link
  (replacing nothing — `officeAbbreviation` stays); `dateSourceText`
  renders with the source citation line, italic, prefixed "date:".
- **Status section:** new `Section title="Status"` after Career, one
  ledger row per status assertion: year-col date range (with uncertainty
  `?`), status name (small-caps, italic + `?` when `isUncertain`), note
  text, `SourceCitation`. Header small-caps summary unchanged.
- **novusNotes:** rendered identically to the existing `nobilisNotes`
  italic paragraph (both may appear).
- **Origin:** new "Origin" field in `PersonRegistry`, with ⓘ popover.
- **Name-part uncertainty:** the registry's Praenomen/Nomen/Cognomen/
  Filiation/Other-names fields append the `?` convention when their flag
  is true; `?` gets a glossary popover ("DPRR records this element as
  uncertain").
- **Relationships:** group order = type's `orderNumber` (nulls last, then
  alphabetical); within groups `relationshipNumber` (nulls last), then
  display name.
- **Source audit:** every assertion row (career, status, dates, notes,
  relationships) must render its `SourceCitation` and any typed notes.
  Known gap fixed by the Status section; the audit verifies the rest and
  fixes anything found.

## Workstream 3: Glossary + filter UX

### Glossary module (`site/src/lib/glossary.ts`)

`GLOSSARY: Record<string, { label: string; text: string }>` — text adapted
from the ontology's `rdfs:comment`s (authoritative), 1–2 sentences,
plain-English first, DPRR sourcing second. Terms: patrician, nobilis,
novus, eques Romanus, senator, sex facet note, praenomen, nomen (gens),
cognomen, filiation, other names, RE number, tribe, office, life events,
location/province, era dates (estimates), uncertainty `?`, office AND
mode, held-in-range mode, statuses-as-facet note. A unit test asserts
every term id referenced by components exists (import-level check).

### ⓘ popover component (`site/src/components/info-hint.tsx`)

Accessible trigger (`<button aria-label="What is …?">` with a small ⓘ
glyph) opening a Radix popover with the glossary text. `ui/popover.tsx`
was deleted as dead code this week — it returns as the shadcn primitive
(restored file + `--popover*` theme tokens) since two features now need
it. Placement:

- Filter panel: each section trigger row's open inset gets a header line
  with the ⓘ for that facet (office, name, status, tribe, location,
  events); the office Options reveal gets ⓘ on both toggles.
- Person page: registry field labels (RE number, Filiation, Origin,
  Tribe), status section title, and the `?` uncertainty markers.
- About page: "Glossary" section rendering the whole module as a dl.

### Tree select-all (implied-check)

In `FacetHierarchyGroup`: when an ancestor of a node is selected, the node
renders its checkbox visually checked but muted and non-interactive
(clicking it does nothing while implied; a tooltip/title says "included
via <ancestor>"). The selected ancestor row appends
`— incl. N sub-offices` (N = descendant count present in the current
facet universe; "sub-locations" for the province tree). Chip labels in
`ActiveFilterChips` for hierarchy facets append "+ sub-offices" /
"+ sub-locations" when the value has descendants. Filter semantics
unchanged (`descendantSet` already implements subtree matching).

## Workstream 4: Polish batch

- **SPARQL dark highlighting** (`site/src/routes/sparql.tsx`): replace the
  hardcoded `defaultHighlightStyle` with a theme-aware setup — a custom
  `HighlightStyle` whose token colors read the site's CSS variables (works
  in both themes without recreating the editor), or if CodeMirror requires
  concrete colors, a compartment that swaps light/dark styles on theme
  change. Acceptance: keywords/strings/comments legible in both themes,
  including after toggling while the editor is open.
- **Scrollbar shift** (`site/src/styles.css`): `html { scrollbar-gutter:
  stable; }` so the layout reserves gutter space whether or not the page
  scrolls. Verify landing ↔ results ↔ person navigation no longer shifts
  the nav/content centering (macOS overlay scrollbars mask the bug; test
  with "Show scroll bars: Always" or a non-overlay environment).

## Unchanged

URL params and filter semantics, search index/payload shapes (position and
xref are person-page data, not search facets), SPARQL page features,
deploy pipeline.

## Testing

- Parser units: each new predicate (position, officeXref, dateSourceText,
  status assertions with notes, origin, novusNotes, uncertainty flags,
  relationship ordering) against fixture TTL.
- Career sort unit: positioned before positionless, chronological
  fallback.
- Relationship grouping unit: orderNumber → relationshipNumber → name.
- Glossary completeness test.
- Full `vp check`/`vp test`/`vp run build`; visual pass: person page (a
  praetor peregrinus case, e.g. ABUR1215; a novus; a status-noted person),
  office tree implied-check, chips, SPARQL both themes, scroll-shift
  navigation.
