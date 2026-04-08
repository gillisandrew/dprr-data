# DPRR Static Site — Design Spec

## Overview

A statically rendered site publishing the DPRR prosopographical dataset as HTML documents with faceted search, hostable on GitHub Pages. Built on the existing TanStack Start scaffold in `site/`.

## Goals

- Publish ~4,900 person records as individual HTML pages at stable URLs
- Provide MiniSearch-powered full-text search with faceted filtering
- Generate good meta tags for SEO and link previews
- Produce a fully static build deployable to GitHub Pages
- Favor reusable components + composition over bespoke one-off components

## Non-Goals

- JSON-LD or structured data markup (canonical RDF is published separately via OCI)
- Dedicated reference pages for offices/provinces/tribes (search with URL-addressable filters covers this)
- Content negotiation or dynamic server behavior
- Province facet (derived indirectly through PostAssertion chains — deferred)

---

## Architecture

### Build Pipeline

```
TTL files (persons/, reference/, concordances/, ontology.ttl)
  │
  ▼
Build-time data loader (TypeScript, runs at SSG time)
  │  - Parses TTL using a JS RDF library (e.g., n3 or rdfxml-streaming-parser)
  │  - Resolves references (offices, provinces, praenomina, relationships, sources)
  │  - Produces typed JSON structures per person
  │  - Builds MiniSearch index + facet value lists
  │
  ▼
TanStack Start SSG
  │  - Static rendering via Nitro static preset
  │  - File-based routing generates /persons/IUNI0001/, /search/, etc.
  │  - Each person page gets full scholarly content + meta tags
  │  - Search page ships MiniSearch index as a static JSON asset
  │
  ▼
Static output → GitHub Pages
```

### Data Loading

A build-time data loader reads the TTL files and produces JSON data that the site consumes. This runs once at build time, not in the browser.

**Input:** All `.ttl` files from `persons/`, `reference/`, `concordances/`, `ontology.ttl`

**Output:**
- `persons-summary.json` — compact array of all persons with only the fields needed for search results and faceting (~500 KB). Does NOT include notes, full post assertions, or scholarly text.
- `search-index.json` — pre-built MiniSearch index (~1-2 MB)
- Per-person data is embedded directly in each pre-rendered HTML page at build time (no client-side fetch for detail pages)

**Person record shape (after resolution):**

```typescript
interface Person {
  id: string                    // "IUNI0001"
  uri: string                   // full RDF URI
  name: string                  // hasPersonName
  praenomen: string             // resolved from Praenomen entity
  nomen: string                 // hasNomen
  cognomen: string | null       // hasCognomen
  otherNames: string | null     // hasOtherNames
  filiation: string | null      // hasFiliation
  reNumber: string | null       // hasReNumber (RE reference)
  sex: "Male" | "Female"        // resolved from Sex entity
  isPatrician: boolean
  isNobilis: boolean
  nobilisNotes: string | null
  highestOffice: string | null  // hasHighestOffice (display string)
  eraFrom: number | null        // hasEraFrom (negative = BC)
  eraTo: number | null          // hasEraTo
  tribe: string | null          // resolved from Tribe entity
  postAssertions: PostAssertion[]
  relationships: Relationship[]
  dateInformation: DateInfo[]
  personNotes: Note[]
  concordances: Concordance[]
}

interface PostAssertion {
  id: string
  officeName: string            // resolved from Office entity
  officeAbbreviation: string | null
  dateStart: number | null
  dateEnd: number | null
  dateSecondarySource: string | null
  originalText: string | null
  secondarySource: string       // resolved source name
  notes: PostAssertionNote[]
  primarySourceRefs: string[]
}

interface Relationship {
  id: string
  relationshipType: string      // resolved from Relationship entity (e.g., "father of")
  relatedPersonId: string       // DPRR ID of related person
  relatedPersonName: string     // resolved name for display
  secondarySource: string
  references: RelationshipReference[]
}

interface DateInfo {
  type: string                  // resolved DateType name (birth, death, etc.)
  value: number
  interval: string
  isUncertain: boolean
  notes: string | null
  secondarySource: string
}

interface Note {
  type: string                  // resolved NoteType name
  text: string
  secondarySource: string
}

interface Concordance {
  system: string                // "wikidata", "viaf", "gnd", etc.
  uri: string                   // external URI
  predicate: "owl:sameAs" | "skos:exactMatch"
}
```

### RDF Parsing Strategy

Use the `n3` library (N3.js) — it's the most mature JS/TS RDF parser, handles Turtle natively, and works in Node. The data loader will:

1. Parse all reference files first to build lookup maps (office ID → name, source ID → title, etc.)
2. Parse concordance files to build a person ID → concordance links map
3. Parse each person file, resolving references against the lookup maps
4. Emit the flattened JSON structures

---

## URL Structure

| Route | Purpose |
|-------|---------|
| `/` | Search/browse page (landing page) |
| `/persons/$id` | Person detail page (e.g., `/persons/IUNI0001`) |

### URL-Addressable Filter State

Search/filter state is encoded in URL query parameters:

```
/?q=brutus&office=consul&nomen=Iunius&eraFrom=-200&eraTo=-100&sex=Male&patrician=true
```

All params are optional. Multiple values for the same facet use comma separation:
```
/?office=consul,praetor
```

This enables bookmarking and sharing filtered views.

---

## Pages

### Search Page (`/`)

The landing page is the search interface. No separate "home" page.

**Layout:** Hybrid sidebar facets + filter chips (Option C from brainstorming).

**Components:**

```
SearchPage
├── SearchInput              # Full-text search, drives MiniSearch
├── ActiveFilterChips        # Shows active filters as removable chips
├── SearchLayout
│   ├── FacetSidebar
│   │   ├── FacetGroup (Office)      # Primary, expanded by default
│   │   │   └── FacetSearch          # Type-to-filter within facet
│   │   │   └── FacetCheckboxList
│   │   ├── FacetGroup (Nomen)       # Primary, expanded by default
│   │   │   └── FacetSearch
│   │   │   └── FacetCheckboxList
│   │   ├── FacetRangeGroup (Era)    # Primary, dual number inputs
│   │   ├── FacetGroup (Sex)         # Secondary, collapsed by default
│   │   ├── FacetGroup (Status)      # Secondary, collapsed (patrician + nobilis)
│   │   └── FacetGroup (Tribe)       # Secondary, collapsed
│   └── ResultsList
│       ├── ResultCount
│       └── PersonCard[]             # Compact result cards, link to detail page
└── Pagination / infinite scroll
```

**Reusable components from this decomposition:**
- `FacetGroup` — generic collapsible facet with checkbox list + optional search-within
- `FacetRangeGroup` — numeric range variant of FacetGroup
- `FacetSearch` — search input that filters a FacetCheckboxList
- `FacetCheckboxList` — list of checkboxes with counts
- `ActiveFilterChips` — horizontal list of removable chip badges
- `PersonCard` — compact person summary (reusable on search results, relationship links, etc.)
- `SearchInput` — debounced text input wired to MiniSearch

### Person Detail Page (`/persons/$id`)

Full scholarly view. Everything rendered.

**Layout:**

```
PersonPage
├── PersonHeader
│   ├── Name (full hasPersonName)
│   ├── Era dates (eraFrom–eraTo, displayed as "540–509 BC")
│   ├── Meta badges (Patrician, Nobilis, Sex)
│   └── Praenomen, Nomen, Cognomen, Filiation, RE number
├── Section: Offices (PostAssertions)
│   └── OfficeEntry[]
│       ├── Office name + abbreviation
│       ├── Date range
│       ├── Original text
│       ├── Secondary source
│       ├── Primary source references
│       └── Notes (scholarly footnotes, rendered in full)
├── Section: Relationships
│   └── RelationshipEntry[]
│       ├── Relationship type
│       ├── Related person (linked to their page)
│       ├── Secondary source
│       └── References
├── Section: Dates
│   └── DateEntry[]
│       ├── Date type + value
│       ├── Uncertainty marker
│       ├── Notes + source
├── Section: Notes
│   └── NoteEntry[]
│       ├── Note type
│       ├── Full text (can be very long — render as prose)
│       └── Source
├── Section: Concordances
│   └── External links grouped by system (Wikidata, VIAF, etc.)
└── Meta tags (<title>, Open Graph, description)
```

**Reusable components from this decomposition:**
- `Section` — titled content section with optional collapse
- `Badge` — small label (shadcn/ui already has this)
- `PersonCard` — reused from search for relationship links
- `SourceCitation` — renders a secondary source reference
- `DateDisplay` — formats negative integers as "509 BC" with uncertainty markers

---

## Search Implementation

### MiniSearch

**Indexed fields:** `name`, `nomen`, `cognomen`, `otherNames`, `highestOffice`

**Stored fields:** `id`, `name`, `nomen`, `eraFrom`, `eraTo`, `highestOffice`, `sex`, `isPatrician`, `isNobilis`

**Facet filtering:** Not done by MiniSearch. After MiniSearch returns text-match results, apply facet filters as array intersection in JS:

```
results = miniSearch.search(query)
results = results.filter(matchesFacets(activeFacets))
```

When no text query is entered, start from the full person list and apply facet filters only.

**Index size:** The pre-built MiniSearch index JSON will be ~1-2 MB for 4,900 documents with the fields above. Loaded once on search page mount.

### Facet Counts

Facet counts update dynamically as filters change. When a facet is active, counts for other facets reflect the intersection (i.e., selecting "consul" updates nomen counts to show only nomina of consuls).

Computed client-side from the full person list — 4,900 records is small enough for in-browser filtering.

---

## Static Generation

### TanStack Start SSG Configuration

TanStack Start with Nitro's `static` preset generates static HTML at build time.

**Key configuration:**
- Nitro preset: `static` (or `github-pages`)
- Pre-render all person routes by enumerating person IDs at build time
- The search page (`/`) ships the MiniSearch index as a static JSON asset loaded client-side

### Build Output

```
dist/
├── index.html                    # Search page
├── persons/
│   ├── IUNI0001/index.html       # Person detail pages
│   ├── IUNI0040/index.html
│   └── ... (~4,900 pages)
├── assets/
│   ├── search-index.json         # Pre-built MiniSearch index
│   ├── persons-summary.json      # Compact person data (for client-side faceting)
│   └── [JS/CSS bundles]
└── 404.html
```

### GitHub Pages Deployment

A GitHub Actions workflow:
1. Runs the build-time data loader (parse TTL → JSON)
2. Runs `vp build` with static preset
3. Deploys `dist/` to GitHub Pages

---

## Component Design Principles

Per user preference: **reusable components + composition over bespoke**.

**Composition patterns:**
- `FacetGroup` composes `FacetSearch` + `FacetCheckboxList` + collapsible wrapper — not a monolithic facet widget
- `PersonPage` composes `Section` + domain-specific entries — `Section` is generic (title, children, collapsible)
- `PersonCard` is the same component in search results and relationship links
- `DateDisplay` is a pure formatting component used across offices, dates, and era display
- `SourceCitation` renders any secondary source reference consistently
- `Badge` (from shadcn/ui) used for status markers, filter chips, and metadata labels

**What NOT to abstract:**
- Don't create a generic "entity page" component — person pages are the only entity type
- Don't abstract the search state management — one hook (`useSearchState`) manages query params, MiniSearch, and facets together
- Don't create wrapper components around shadcn/ui primitives unless adding real behavior

---

## Meta Tags

Each person page gets:

```html
<title>L. Iunius Brutus (IUNI0001) — DPRR</title>
<meta name="description" content="cos. 509 · Patrician · 540–509 BC" />
<meta property="og:title" content="L. Iunius Brutus — DPRR" />
<meta property="og:description" content="cos. 509 · Patrician · 540–509 BC" />
<meta property="og:type" content="profile" />
```

The search page gets:

```html
<title>DPRR — Digital Prosopography of the Roman Republic</title>
<meta name="description" content="Search and browse 4,876 persons from the Roman Republic (509–31 BC)" />
```

---

## Technology Stack

| Layer | Choice | Notes |
|-------|--------|-------|
| Framework | TanStack Start | Already scaffolded in `site/` |
| Routing | TanStack Router (file-based) | Already configured |
| Styling | Tailwind CSS 4.2 + shadcn/ui | Existing theme with Lora (serif headings) + Inter (body) |
| Search | MiniSearch | Client-side full-text search, pre-built index |
| RDF Parsing | N3.js | Build-time only, parses TTL to JSON |
| SSG | Nitro static preset | Generates static HTML for all routes |
| Hosting | GitHub Pages | Static files served from `dist/` |
| Build tool | Vite+ (`vp`) | Existing project toolchain |

---

## Open Questions (resolved)

- ~~JSON-LD~~ → Skipped. Good meta tags instead.
- ~~Reference entity pages~~ → Skipped. URL-addressable search filters cover this.
- ~~Province facet~~ → Deferred. Requires resolving PostAssertion → Office → Province chains at build time.
