# Source Completeness, Provincia Terminology, Hover Hints — Design

**Date:** 2026-08-12
**Status:** Approved

## Problem

An audit of the person page found four places where assertion sources or
qualifiers exist in the data but never render: tribe assertions carry
`hasSecondarySource` + `hasNotes` that the parser flattens away;
`RelationshipAssertion.isUncertain` (present in volume) is dropped;
`DateInformation.hasDateInterval` (S/B/A) is parsed but a "before 74" date
renders as a bare year; and `PostAssertion.hasDateSecondarySource` is
parsed but never displayed. Separately: the site labels post provinciae as
"Location", but a provincia is a sphere of responsibility that need not be
a place (Broughton codes like "amb" appear in `hasProvinceOriginal`); and
the ⓘ glossary popovers require a click where hover should suffice.

Verified non-gaps (no action): career/status/date/person-note/relationship
sources and typed notes all render; `PrimarySourceReference` only ever
targets PostAssertions (covered); the structured province-link classes
(`PostAssertionProvince`, `ProvinceNote`) have zero instances, so post
provinciae have no per-link source beyond the assertion's own citation;
`PostAssertionNote.hasExtraInfo` has zero instances (vestigial).

## 1. Tribe sources

- `types.ts`: `Person` gains
  `tribeAssertions: TribeAssertionRecord[]` where
  `interface TribeAssertionRecord { tribeName: string; secondarySource: string; notes: string | null; isUncertain: boolean }`.
  The flat `tribes: string[]` on `PersonSummary` stays (faceting).
- `parse-persons.ts`: `buildTribes` returns both shapes (or a sibling
  builder): name from refs, `resolveSource(first(g, "hasSecondarySource"))`,
  `first(g, "hasNotes")`, `first(g, "isUncertain") === "true"`.
- Registry Tribe field: each tribe link is followed by a `SourceHint`
  popover containing the assertion's `SourceCitation` and note text, and
  the `?` convention when `isUncertain`. Multiple assertions for the same
  tribe name (differing sources) each get their hint content stacked in
  one popover.

## 2. Uncertain relationships

- `Relationship` gains `isUncertain: boolean`
  (`first(g, "isUncertain") === "true"` in `buildRelationships`).
- `RelationshipEntry`: when uncertain, the related-person name is italic
  with a trailing `?` — same convention as career/status rows.

## 3. Date intervals

- `lib/dates.ts`: new `formatYearWithInterval(year, interval, uncertain)`
  → `B` prefixes "before ", `A` prefixes "after ", `S`/null defers to
  `formatYear(year, uncertain)` exactly (the `c.` uncertainty prefix
  composes: "before c. 216 BC" is valid output).
- `DateEntry` uses it with `dateInfo.interval`. Unit tests for B/A/S/null
  × certain/uncertain.

## 4. Date source display

- `OfficeEntry`: when `dateSecondarySource` is non-empty AND differs from
  `secondarySource`, the date line shows it — with source wording:
  `date: {dateSourceText} — {dateSecondarySource}`; without:
  `date per {dateSecondarySource}`. Identical-to-main-citation cases stay
  suppressed.

## 5. Provincia rename (visible labels only; URLs, params, slugs, and data
   field names unchanged)

- Career row label "Location:" → "Provincia:".
- Filter panel: ADVANCED section label "Location" → "Provincia"; chips
  "Location: X" → "Provincia: X"; Location tree `childNoun` →
  "sub-provinciae".
- Nav "Locations" → "Provinciae"; `/provinces` index page title/heading and
  the slug page's noun usages retitle accordingly; directory-page prose
  link text updates if it says "locations".
- Glossary entry id `location` keeps its id; label becomes "Provincia",
  text: "The sphere of responsibility assigned with a post — often a
  territory (Sicilia, Hispania), but equally a task or command: a war, a
  fleet, the courts, the grain supply. DPRR records the provincia as given
  in the sources; the geographic grouping used for browsing is this
  site's curation."
- README: touch up any "location" phrasing about the province mapping.
- Person-page section hints and search meta descriptions that say
  "Location" follow the rename.

## 6. Hover-reveal hints

- New `HoverPopover` wrapper (`site/src/components/hover-popover.tsx`) used by
  `InfoHint` and `SourceHint`: controlled Radix popover that opens on
  pointer-enter of the trigger (~150ms delay) or keyboard focus, stays
  open while the pointer is over trigger or content, closes on
  pointer-leave of both; click still toggles so touch devices keep
  working. No call-site API change.

## Testing

- Parser units: tribe assertion record (source, notes, uncertain) and
  relationship `isUncertain`.
- `dates` unit tests for interval formatting.
- Suite green (`vp check`, `vp test`, `vp run build`).
- Visual pass: AEMI2895 (tribe with source + note "p187."), an uncertain
  relationship, a before/after date, renamed labels across career row /
  filter section / chips / nav / provinces pages, hover-opening ⓘ.
