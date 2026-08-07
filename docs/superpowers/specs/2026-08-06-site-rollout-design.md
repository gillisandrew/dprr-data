# DPRR Site Rollout — Design Spec

## Overview

A rollout plan taking the completed static site feature (`feature/static-site`, all 19 tasks of the 2026-04-08 plan implemented) to a polished public launch on GitHub Pages. The rollout adds the features deferred by the original spec, fixes deployment blockers, and finishes with a structured UX pass — all held on the feature branch until a single launch merge.

## Goals

- Deploy to the GitHub project page at `https://gillisandrew.github.io/dprr-data/`
- Add the originally deferred features: province facet, reference pages (offices, tribes, provinces), JSON-LD structured data
- Refine search & discovery UX, person detail pages, and visual identity before launch
- Launch once, polished — no incremental public releases before the site is ready

## Non-Goals (deferred to later plans)

- Linked Data Fragments / in-browser SPARQL querying and published RDF dump artifacts (canonical RDF is already published via OCI)
- Custom domain, analytics, sitemap generation, PR deploy previews
- Any dynamic server behavior

## Rollout Structure

Two implementation plans executed in sequence on `feature/static-site`:

| Plan | Content | Planned when |
|------|---------|--------------|
| Plan 1 — Features & deploy readiness | Deployment foundation, province pipeline, reference pages, province facet, JSON-LD | Now |
| Plan 2 — UX pass | Structured review + polish of search, person pages, visual identity | After Plan 1 is built, via its own short brainstorm against the running site |

Launch is a single merge of `feature/static-site` → `main`, which triggers the existing `deploy-site.yml` workflow.

## Plan 1: Features & Deploy Readiness

### Deployment foundation

- Commit the pending formatting-only working-tree changes (`vp fmt` output, no behavior change).
- Configure the base path for a GitHub project page: `base: "/dprr-data/"` in `vite.config.ts` and the matching `basepath` on the TanStack Router configuration, so assets, routes, and the prerender crawler all resolve under `/dprr-data/`.
- Verification: build and serve `dist/client` locally under a `/dprr-data/` path prefix; check the search page, a person page, the 404 page, and client-side navigation between them.

### Province pipeline + facet

**Data reality (discovered during planning):** the TTL export contains no structured province links — the `PostAssertionProvince` entities described by the ontology are absent. Post assertions carry only free-text `hasProvinceOriginal` strings (~952 occurrences, 185 distinct values with spelling variants). The structured provinces in `reference/provinces.ttl` are orphaned.

- Resolve provinces via a **curated mapping** checked into the repo: each distinct `hasProvinceOriginal` string maps to one or more canonical province names from `reference/provinces.ttl`. The mapping is a reviewable source file; a test validates every mapping target against the reference data. Unmapped strings produce build-time warnings (not failures) and are excluded from the facet; the raw text is still displayed on person pages.
- The loader produces `provinces: string[]` (canonical names) on each person summary plus a facet value list.
- The province facet groups values by the `hasParent` hierarchy in `reference/provinces.ttl` (12 roots — Italia, Africa, Asia, Mediterranean, … — max depth 2), mirroring the original DPRR "Location" facet.

### Scholarly conventions (from review of romanrepublic.ac.uk, 2026-08-06)

A review of the original DPRR faceted search surfaced conventions researchers expect that our build was missing:

- **Uncertainty markers:** the TTL export carries `isUncertain` (6,748 occurrences on post assertions), `isDateStartUncertain` (2,596), and `isDateEndUncertain` (2,018) flags that the parser previously dropped. Parse them and render DPRR's convention — italic office name with a trailing "?" (e.g. *Tribunus Militum?* 508), and "?" after uncertain dates — on person pages and reference-page holder lists.
- **Chronological careers:** post assertions sort by earliest known date (undated last), matching DPRR's career listing.
- **Office hierarchy:** `reference/offices.ttl` organizes 204 offices under 8 roots (Magisterial Posts, Promagisterial Posts, Priesthoods, Non-magisterial Posts, Equestrian Functions, Distinctions, plus two standalone entries; max depth 3). The office facet and the `/offices` index group by this hierarchy instead of one flat alphabetical list.
- Add a `FacetGroup (Province)` to the sidebar as a collapsed secondary facet, a `province=` URL query param in the search state hook (comma-separated multi-value, like `office=`), and removable-chip support.
- Parser changes get unit tests alongside the existing parser test suites.

### Reference pages

New prerendered routes following the person-page composition patterns (`Section`, `PersonCard`, `DateDisplay`, `SourceCitation`):

- `/offices/` and `/offices/$id` — index of all offices; each office page lists holders chronologically (name, dates, source) aggregated from post assertions. Office names on person pages link here.
- `/tribes/` and `/tribes/$id` — index and per-tribe member list; tribe display on person pages links here.
- `/provinces/` and `/provinces/$id` — same shape, built on the province pipeline output.

Index pages join the prerender crawl (via links reachable from the crawler entry points). Every page gets `<title>` and meta description tags consistent with the person pages.

### JSON-LD

- Person pages embed a conservative schema.org `Person` block: `name`, `alternateName` (other names, when present), `gender`, `description` (reusing the existing meta-description string), and `sameAs` populated from Wikidata/VIAF/GND concordance URIs.
- The search page embeds a `Dataset`/`CollectionPage` block describing the collection.
- Era/birth/death years are intentionally excluded — schema.org date handling for BC years is unreliable, and the canonical dates live in the RDF.

## Plan 2: UX Pass

Structure committed now; outcomes decided later against the real site.

- **Method:** run the complete site locally, perform a structured review of three areas — search & discovery, person detail pages, visual identity — and turn findings into a concrete polish plan. Implementation uses the frontend-design skill so visual identity is deliberate rather than default-shadcn.
- **Seed candidates** (inputs to the review, not commitments): landing-page framing so the site reads as a scholarly resource rather than a bare search box; empty/zero-result states; result density and 4,900-result browsing; person-page content density and in-page navigation for long records; typographic hierarchy with the existing Lora/Inter pairing; mobile layout for the facet sidebar.
- **Seed candidates from the romanrepublic.ac.uk review:** fasti-style result lines (RE number, filiation, relationship context for otherwise-obscure persons — e.g. "VETU5025 Veturia (24) (mother of C. Marcius (50) Coriolanus)"); date-ascending default result sort; structured name-part search with autocomplete (praenomen/nomen/cognomen/father/RE); a life-events facet (birth, death, violent death, proscribed, exiled, adopted, expelled from senate, restored — derivable from DateInformation types); "Time frame: 509 B.C. – 31 B.C." hints on date inputs; collapsed-by-default career notes with expand; print/PDF export of results; "Add all" subtree selection in hierarchical facets; reconsider labeling the province facet "Location" as DPRR does; a visible-count cap for hierarchical facets; a look at the search page's ~4 MB prerendered HTML payload (3.6 MB embedded data + hidden crawl links).
- **Boundary:** polish of existing and Plan-1 features only; no new features enter through this phase.

## Launch Process

1. Full static build on the branch; verify page count (~4,900 persons + reference pages), spot-check representative pages, run all test suites.
2. Merge `feature/static-site` → `main`; `deploy-site.yml` builds and deploys.
3. One-time repo setting: GitHub Pages enabled with "GitHub Actions" as the source.
4. Post-deploy smoke check of live URLs: search page, a person page, a reference page, a deep link with filter params, 404 behavior.

Post-launch data updates need no extra work: the workflow already redeploys when TTL files change on `main`.

## Error Handling & Testing

- Pipeline additions (province resolution, office-holder aggregation) follow the existing pattern: unit tests per parser; the loader fails the build loudly on unresolvable references rather than emitting partial pages.
- UI correctness is verified through the prerendered build — the crawler visits every internal link, so broken routes fail the build.
