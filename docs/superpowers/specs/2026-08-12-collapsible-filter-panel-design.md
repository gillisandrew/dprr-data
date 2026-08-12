# Collapsible Filter Panel with Progressive Disclosure — Design

**Date:** 2026-08-12
**Status:** Approved

## Problem

The faceted search UI uses a horizontal band of pill triggers (`FilterBand`),
each opening a floating popover on desktop or a bottom-sheet dialog on mobile
(`FilterPopover`). The dropdown approach is limiting: only one small floating
surface is visible at a time, content is cramped inside a popover width, and
all six groups carry equal visual weight regardless of how often they are
used. This replaces the popovers with in-place collapsible sections and adds
progressive disclosure so basic filters are immediate and advanced ones stay
available without cluttering the default view.

## Decisions (approved in brainstorming)

- **Layout:** stacked panel above the results list (not a sidebar, not a
  drawer). Section content expands in place, full width, pushing results down.
- **Basic tier (always visible):** Office, Name, Status.
- **Advanced tier (behind "More filters"):** Tribe, Location, Events.
- **Expand model:** accordion — one section open at a time.
- **In-group disclosure:** yes — Name and Office tuck their advanced fields.
- **Mobile:** same stacked sections; the bottom-sheet code path is deleted.

## Components

### `FilterPanel` (new, replaces `FilterBand` + `FilterPopover`)

Sits where the band is today: below the era timeline, above the results.

**Header row.** Section triggers styled like the current pills — uppercase
micro label, active-count suffix, chevron (rotates when open). Order: Office,
Name, Status, then a visually quieter "More filters" trigger.

**"More filters" reveal.** Clicking it adds the Tribe, Location, and Events
triggers to the row and toggles its own label to "Fewer filters", which
collapses the tier again (closing any open advanced section). Rules:

- If any advanced-group filter is active on initial render (deep link /
  back-navigation), the advanced tier starts revealed.
- While collapsed, the trigger shows the combined active count of the hidden
  groups (e.g. "More filters (2)") so active state is never invisible.

**Accordion.** Local state holds the single open section key (same pattern as
the band's `openKey`). Clicking an open section's trigger closes it. The open
section renders in a bordered inset panel directly below the header row,
spanning the panel's full width. Content taller than ~45vh scrolls internally
(`max-h-[45vh] overflow-y-auto`) so results never vanish below the fold.

**`initialFocus`.** The landing-page "Browse by" cards keep working: `office`
opens the Office section expanded, `gens` opens Name expanded.

### Section contents

Existing facet components are reused unchanged in `frameless` mode:
`FacetGroup`, `FacetHierarchyGroup`, `FacetCombobox`.

- **Office:** `FacetHierarchyGroup` tree, plus an "Options…" in-group reveal
  containing the two checkboxes (require-every-office AND mode; apply time
  period to offices). Auto-revealed when either is non-default
  (`officeMode === "all"` or `officeInRange`).
- **Name:** Gens (nomen) and Cognomen comboboxes visible up front. A "More
  name fields…" reveal adds Praenomen, Father (praenomen), Grandfather
  (praenomen), and RE number. Auto-revealed when any of those four carries a
  value. Comboboxes sit in a 2-column grid at `md+`, single column below.
- **Status:** status checkboxes + sex checkboxes (same split as today).
- **Tribe:** searchable `FacetGroup`.
- **Location:** `FacetHierarchyGroup` with province hierarchy.
- **Events:** `FacetGroup`.

**Width use.** Plain checkbox lists (Status, Tribe, Events) flow into
responsive CSS columns (e.g. `columns-2 md:columns-3`) instead of one narrow
column. Hierarchy trees (Office, Location) stay single-column with internal
scroll.

### In-group reveal pattern

A small text button ("More name fields…" / "Options…") in the section body.
Once revealed it stays revealed for the session (component state); it is
forced open when any tucked field is active so an active filter is never
hidden. No collapse-again control is required.

## Mobile & accessibility

- One code path at all viewports: triggers wrap onto multiple lines; the
  content inset is full width. `useIsDesktop` and the Radix bottom-sheet
  Dialog branch in `filter-popover.tsx` are deleted along with the file.
- Triggers are `<button>`s with `aria-expanded` and `aria-controls` pointing
  at the content region; the content region has `role="region"` and
  `aria-labelledby` back to its trigger.

## Unchanged

`SearchState`, URL-param serialization (`search-params.ts`), filter logic
(`filter.ts`), facet computation, `ActiveFilterChips`, `EraTimeline`
placement, `ResultsHeader`/`ResultsList`, and the search landing page. This
is a presentation-layer change only.

## Code changes

- Add `site/src/components/filter-panel.tsx` (header row, accordion, tiers,
  in-group reveals — may split internals into small local components).
- Delete `site/src/components/filter-band.tsx` and
  `site/src/components/filter-popover.tsx`.
- Update `site/src/routes/index.tsx` to render `FilterPanel`.
- Check `site/src/styles.css` for popover-specific rules that become dead.

## Batch item: updated site icon

`site/public/icon.svg` was updated on 2026-08-12 but its raster derivatives
(`favicon.ico`, `icon-192.png`, `icon-512.png`, `apple-touch-icon.png`) are
stale (2026-08-09). Regenerate all four from the new SVG and commit them
together with the SVG as a separate commit in the same batch.

## Testing

- Existing unit tests in `site/src/lib` and `site/src/data` are unaffected
  (no logic changes) and must stay green (`vp check`, `vp test`).
- Manual pass, desktop and narrow viewport: accordion open/close, More
  filters reveal + collapsed active count, deep-link auto-reveal of the
  advanced tier and tucked fields, landing-card `initialFocus` paths, tall
  tree internal scroll.
