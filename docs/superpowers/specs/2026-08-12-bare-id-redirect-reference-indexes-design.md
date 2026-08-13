# Bare-ID redirects and reference index pages

Date: 2026-08-12

Two independent additions to the DPRR static site:

1. Short URLs that redirect a bare numeric ID to its canonical person page
   (`/persons/0522` → `/persons/VALE0522`).
2. Index and detail pages for the three reference vocabularies that don't
   have them yet: secondary sources, praenomina, and relationship types.

## Context

The site is prerendered to static files and served from GitHub Pages under
the `/dprr-data/` base path. There is no server, and `public/404.html` is a
static dead-end — it does **not** boot the SPA. Any URL that isn't
prerendered is therefore unreachable by the application. This single fact
drives the redirect design.

Existing reference pages (`/offices`, `/tribes`, `/provinces`, `/gentes`)
follow a settled pattern: aggregate builders in
`site/src/data/aggregate-references.ts`, static JSON emitted per detail page
into `site/public/data/<type>/<slug>.json` by `staticDataPlugin`, and route
components that load it via `createIsomorphicFn`. Both features below follow
that pattern rather than inventing a new one.

## Part 1 — Bare-ID redirects

### Verified premises

- All 4,876 person IDs end in exactly **four digits**.
- Those four-digit suffixes are **globally unique** — no two persons share
  one, across all prefixes.
- Prefixes are not uniformly four letters. Real examples include `VALE`,
  `CAE`, `TE-`, `P-`, `POS-`, `SAE[`, and `PL[A`.
- `site/public/data/person-ids.json` already exists (56 KB): a flat JSON
  array of every canonical ID. It is copied to
  `dist/client/data/person-ids.json` during the build.

### Aliases

For each canonical ID (e.g. `VALE0522`), two aliases redirect to it:

| Alias | Example | Rationale |
| --- | --- | --- |
| Bare four-digit suffix | `0522` | Short citation form; the feature request. |
| All-lowercase full ID | `vale0522` | Common paste/typing error. |

Only the all-lowercase form is generated. Mixed-case variants such as
`Vale0522` are combinatorial and deliberately out of scope — they cannot be
prerendered exhaustively.

Alias URLs live under the existing persons path only: `/persons/<alias>`.
Root-level aliases (`/0522`) are out of scope; they would claim the root
namespace and risk colliding with `/about`, `/sparql`, `/directory`.

### Mechanism

A post-build Node script, `site/scripts/emit-id-redirects.mjs`, wired into
the build after the existing sitemap step:

```
"build": "vp build && node scripts/normalize-sitemap.mjs && node scripts/emit-id-redirects.mjs"
```

It reads `dist/client/data/person-ids.json` and writes one file per alias to
`dist/client/persons/<alias>/index.html`. Because it runs after prerender,
these are plain file writes, not React renders — the cost is negligible
compared to the 126 genuinely prerendered pages added in Part 2.

Each emitted file contains:

- `<link rel="canonical" href="/dprr-data/persons/VALE0522">` so search
  engines consolidate on the real page.
- `<meta http-equiv="refresh" content="0; url=/dprr-data/persons/VALE0522">`
  for the actual redirect, which works with JavaScript disabled.
- A visible fallback paragraph linking to the target, so the page is still
  usable if the refresh is blocked.

Styling reuses the inline-CSS approach already in `public/404.html` — these
files must not depend on the app bundle.

### Collision safety

The script asserts, before writing anything, that every generated alias maps
to exactly one canonical ID. If a future data refresh introduces a
four-digit collision, the build **fails loudly**. A silent wrong-person
redirect would be far worse than a red build, because it would look correct.

The alias derivation and collision check are extracted into a pure,
exported function so they can be unit-tested without running a build.

### Cost

~9,750 additional files (4,876 bare + 4,876 lowercase), roughly 400 bytes
each — about 4 MB of build output. These are written directly, not rendered:
the prerendered page count rises only by the 126 pages from Part 2 (~6,133 →
~6,259), while total files in `dist/client` grows by ~9,750.

## Part 2 — Reference index and detail pages

### Scope

Three types, each getting an index page and per-item detail pages:

| Route | Index content | Detail content |
| --- | --- | --- |
| `/sources`, `/sources/$slug` | 34 works: abbreviation + full bibliography | Persons citing that work |
| `/praenomina`, `/praenomina/$slug` | 45 names + abbreviations | Persons bearing that praenomen |
| `/relationships`, `/relationships/$slug` | 44 types in curated order, with inverses | Person **pairs** in that relation |

`reference/misc.ttl` is **out of scope**. Its contents are note types (~9
internal provenance labels), statuses (2, already exposed as a search
facet), and sex (2, a trivial split). None earn a page.

### Join keys

- **Sources:** `resolveSource` in `parse-persons.ts` resolves a source URI to
  the source's `hasName` value, so `PostAssertion.secondarySource` holds the
  full title (e.g. "The Magistrates of the Roman Republic, Vol. I"), not the
  abbreviation. The reverse index groups on that string and matches back to
  the `sources.ttl` entry by name.
- **Praenomina:** `PersonSummary.praenomen`, a plain name string.
- **Relationships:** `Relationship.relationshipType` plus
  `relatedPersonId` / `relatedPersonName`, which together give both ends of
  each pair.

### Data layer

Extend `site/src/data/aggregate-references.ts` with six builders following
the existing `buildOfficeIndex` / `buildOfficeDetail` shape:

- `buildSourceIndex`, `buildSourceDetail`
- `buildPraenomenIndex`, `buildPraenomenDetail`
- `buildRelationshipIndex`, `buildRelationshipDetail`

Slugs come from the existing `slugify`, guarded by the existing
`assertUniqueSlugs`, which throws at build time on a collision.

Sources whose citation count is zero still appear on the index, with their
detail page rendering an explicit empty state rather than 404ing — the
bibliography entry is itself the useful content.

### Payload shape

Sources and praenomina detail pages carry person lists that can be very
large: Broughton MRR Vol. I is cited by roughly 4,200 of the 4,876 persons.

These payloads therefore store a **trimmed row summary**, not the full
~20-field `PersonSummary`. A new exported type `FastiRowSummary` in
`site/src/data/types.ts` carries exactly the eight fields `FastiRow`
renders:

```
id, name, highestOffice, contextLine, filiation, eraFrom, eraTo, nomen
```

`FastiRowSummary` is defined as a `Pick<PersonSummary, …>` of those fields,
so `FastiRow` can be widened to accept it and every existing caller passing
a full `PersonSummary` keeps working unchanged.

This keeps the worst case near 700 KB instead of ~2 MB, stays self-contained
like the existing offices and gentes payloads, and avoids introducing a
dependency on the shared 2.2 MB `search-data.json`. It also partially
addresses the previously deferred concern about `contextLine` bytes bloating
gens and tribe payloads.

Relationship payloads need no such treatment. Measured across a 1,500-person
sample scaled to the full dataset, there are roughly 6,800 pairs in total and
the largest single type ("brother of") has about 1,550 — around 120 KB.

### Components

- Sources and praenomina detail pages render the existing `ResultsList`,
  which paginates at 50 rows. `ResultsList` and `FastiRow` take a plain
  array and hold no search context, so they are reusable as-is.
- Relationship detail pages need a new row component. They list pairs
  (`A — brother of — B`), not members, so `FastiRow` does not fit. The index
  page surfaces `owl:inverseOf` where present (brother of ↔ sister of),
  which `relationships.ttl` already carries.

### Discovery

All three index pages are linked from `/directory`, whose header currently
links offices, tribes, and provinciae. The prerender crawler follows links,
so linking the index pages is what makes the detail pages discoverable and
prerendered — this is load-bearing, not just navigation.

## Error handling

| Case | Behaviour |
| --- | --- |
| Unknown reference slug | Route loader returns null; existing not-found handling applies, matching `offices.$slug` |
| Reference slug collision | `assertUniqueSlugs` throws during build |
| Bare-ID alias collision | `emit-id-redirects.mjs` throws before writing; build fails |
| Source with zero citing persons | Index lists it; detail page shows an empty state |
| Redirect page with JS disabled | `meta refresh` still fires; visible fallback link as backstop |

## Testing

Unit tests in the existing files, following existing style:

- `aggregate-references.test.ts`: index counts and ordering, detail
  membership, curated relationship ordering, trimmed-row field selection,
  and slug-collision failure for each of the three new types.
- A new test for the extracted alias function: bare and lowercase alias
  derivation across the awkward prefixes (`PL[A3544`, `TE-3084`, `P-3093`),
  and that a synthetic four-digit collision throws.

The existing 174 tests must stay green. `vp build` must exit 0 — this is
non-negotiable verification, since a prior change in this repo passed both
typecheck and the full test suite while still breaking the prerender step.

## Out of scope

- Root-level aliases (`/0522`).
- Mixed-case ID variants.
- Index or detail pages for `reference/misc.ttl`.
- Search facets for source, praenomen, or relationship type.
- Backfilling trimmed payloads into the existing gentes and tribes pages.
