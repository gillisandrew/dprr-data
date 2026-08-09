# Advanced Search Gap — Design

Date: 2026-08-09. Closes the feature gap against the legacy DPRR person
search (https://romanrepublic.ac.uk/person/) while replacing the tall
sidebar with a progressive-disclosure filter band. Decisions made
interactively (visual companion session `.superpowers/brainstorm/58528-*`;
legacy-site audit summarized below).

## Legacy audit → true gaps

Already at parity: hierarchical office facets with counts and subtree
select, location/province hierarchy, era range, life-event facet (the
legacy's death/birth/proscribed/exiled/etc. are DateInformation types our
event facet derives from), gender, patrician/nobilis, tribe, RE, name
autocompletes, sorting. Legacy "Distinctions" (triumphator, princeps
senatus) are ordinary offices in our office list.

Genuine gaps addressed here: **status facet** (eques Romanus 396
assertions, senator 1,596, novus 115 — all unparsed today),
**father/grandfather search**, **relationship-context result lines**.
Explicitly out of scope: notes full-text search, print/PDF export, the
legacy Fasti and Senate search pages.

## 1. Layout: filter band + popovers

The facet sidebar is deleted; results render full-width. Under the search
box and the always-visible era timeline sits a horizontal band of facet
buttons — **Office · Name · Status · Tribe · Location · Events** — styled
in the ledger idiom (micro-labels, hairlines, accent ink).

- Each button opens one Radix Popover at a time (radix-ui already a dep);
  buttons show active counts, e.g. "Office (2)".
- The active-filter chips row stays directly under the band.
- Popover contents reuse existing internals: office/location hierarchy
  trees (subtree select, any/all + in-range controls stay inside the
  Office popover), checkbox lists with disjunctive counts, and the
  comboboxes currently in the advanced-search tier.
- Name popover: praenomen, nomen, cognomen, father, grandfather
  comboboxes + RE input.
- Status popover: the unified status list (§2) plus the Sex toggle.
- Mobile (below `md`): the band wraps; popovers render as bottom sheets
  (Radix Dialog).
- Deleted: tiered sidebar, "More filters" and "Advanced search"
  expanders — the band is the progressive disclosure.

## 2. Status facet

- Parser: handle `StatusAssertion` entities (`hasStatus` →
  the already-parsed statuses reference map, `isAboutPerson` linkage) and
  the `isNovus` person property.
- Person/summary gain status values; **one Status facet** lists
  Patrician, Nobilis, Eques Romanus, Senator, Novus with counts,
  replacing the two lone Patrician/Nobilis toggles.
- URL: new multi-value `status` param. Old `patrician`/`nobilis` params
  parse as aliases (mapped into `status`) so shipped links keep working;
  serialization emits only `status`.
- Person page: new statuses render in the registry strip in the same
  small-caps style as Patrician/Nobilis today.

## 3. Father / Grandfather (filiation praenomen)

- Build-time parse of the filiation string: "Q. f. Ser. n." → father
  Quintus, grandfather Servius. Abbreviations expand via the praenomina
  reference; unparseable slots ("-. f.") yield null and never match.
- Two nullable summary fields (`father`, `grandfather`); facet values
  computed like praenomen; two comboboxes in the Name popover; two URL
  params.

## 4. Relationship-context result lines

- Build-time: persons with no career entries get a context line derived
  from their relationships — e.g. "father of Ap. Claudius (321),
  cos. 495" — picking the most notable relative (non-null highest
  office; era as tie-break). Null when no such relative exists.
- Stored as a nullable string on the summary; rendered as the secondary
  line of the fasti result row (same slot career summaries use).

## Testing

House pattern — pure modules with unit tests: filiation parser, status
parsing, context-line builder, URL round-trips (including alias params),
facet filtering for the new state fields. Full suite green; prerender
page count as build regression; landing must still render without
JS-fetched data.

## Non-goals

Notes full-text search, print/PDF export of results, Fasti/Senate search
pages, relationship-based (full-name) ancestor matching.
