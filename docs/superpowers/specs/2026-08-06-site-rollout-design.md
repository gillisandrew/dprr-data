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

- Extend the build-time data loader to resolve provinces via the PostAssertion→Office→Province chain, producing `provinces: string[]` on each person summary plus a facet value list.
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
