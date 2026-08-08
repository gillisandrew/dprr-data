# DPRR Ledger Design Pass (Plan 3) — Design Spec

## Overview

A presentation-layer redesign tightening the whole site around one visual system — the **Editorial Ledger**: structure from typographic hierarchy, alignment, and hairline rules rather than cards and boxes. Direction chosen interactively from mockups on 2026-08-08 (session in `.superpowers/brainstorm/`), grounded in dense-interface research (hierarchy over minimalism; density is a feature when type scale, alignment, and consistent row patterns carry it).

Chosen variants: **Editorial Ledger** overall; **registry-header single-column** person page; **two-line fasti, ruled** search results.

## Goals

- The search and person pages read as one deliberately designed scholarly instrument
- Density increases (more rows per screen, less chrome) while scannability improves
- The rail/mobile-duplication machinery on person pages is eliminated structurally
- Dark mode remains first-class

## Non-Goals

- Any behavior change: search semantics, URL state, data pipeline, routes, and progressive-disclosure interactions are untouched
- New features; the post-launch fix list (conjunctive counts, etc.) stays separate
- Custom fonts beyond the existing Lora/Inter pair

## 1. Token layer (the system)

- **Rules, not boxes.** Exactly two rule weights: `--rule-lead` (2px, near-foreground) under page titles, column headers, and the control band; `--rule-hair` (1px, warm low-contrast) between entries and under section labels. Card borders, rounded panels, and fill backgrounds are removed everywhere except interactive surfaces (inputs, selects, open popovers/suggestion lists) and code/note blocks.
- **Type roles.** Lora: person/entity names and page titles only. Inter: everything else. Section labels: 10–11px uppercase, `letter-spacing: .1em`, accent color ("micro-labels"). `font-variant-numeric: tabular-nums` site-wide via a base rule; era/year values right-align within their columns.
- **Accent.** The existing `--primary` token is the only non-neutral hue: ledger year columns, links, micro-labels, active/selected states. Status markers (Patrician, Nobilis) render as small-caps text, not badge pills.
- **Dark mode.** Tokens re-derived: paper → warm near-black, hairlines reduced contrast, accent lightened one step. Every restyled surface verified in both themes.
- Implemented as CSS variables + a small set of shared utility classes in `styles.css` (e.g. `ledger-row`, `micro-label`, `rule-lead`), consumed by components — no per-page copies of the values.

## 2. Search page

- **Control band:** search input, active filter chips, result count, and sort control merge into one band closed by a lead rule (input left; chips wrap; count + sort right). The timeline band sits directly beneath, framed by hairlines only (no border/box).
- **Sidebar:** facet groups become micro-labeled ruled groups (label + hairline; no collapsible-card chrome). Checkbox rows tighten to ~28px. The hierarchy tree keeps indentation; its structural nodes use the micro-label style with their checkboxes. Tier triggers ("More filters", "Advanced search") are ruled rows with +/− affordances. All existing open/close/auto-open behavior unchanged.
- **Result rows (two-line fasti, ruled):** line 1 — serif name, accent `— {highestOffice}`; line 2 — muted `filiation · era · gens` (tabular numerals, gens keeps its link). Hairline separators; hover is a subtle paper tint. Row anatomy identical on all viewports.
- **Landing:** hero and three browse-by cards remain; cards restyle to ruled entries (lead top rule, no border box), same type roles.

## 3. Person page (registry header, single column)

- **Header:** serif name; strong line `{accent office} · {era} · {small-caps status}`; lead rule.
- **Registry strip:** a wrapping micro-labeled field row directly under the header: Praenomen, Nomen (→ gens page), Cognomen, Filiation, RE, Tribe(s) (→ tribe pages), DPRR ID, Links (concordances, system names as links). Fields with no value are omitted. On narrow screens the strip wraps 2–3 fields per line.
- **The right rail is deleted**: `person-rail.tsx` and the `lg:` grid/`hidden lg:block` mobile-duplication machinery are removed; the identity/family/dates/links cards' content is fully absorbed by the registry strip and the body ledgers.
- **Body ledgers, in order:** Career (right-aligned accent year column; office + location; source line with the collapsed "{n} notes ▸" expander), Relationships (existing grouping; collapsed "{n} references ▸" kept), Dates (year column, type, uncertainty "?", source), Notes (prose under micro-labels). Each section: micro-label + hairline; no boxes. All content and interactions carry over exactly.

## 4. Reference pages, directory, header/footer

Light-touch: page titles get lead rules; index pages (gentes, tribes, offices grouped categories, locations, directory) become justified ledger rows — name left, count right, hairline separators; detail-page holder/assertion lists adopt the career-row anatomy (year column, name, source). Header/footer: same nav/links, restyled to the token set (hairline instead of border).

## 5. Implementation shape

1. **Task 1 — tokens:** CSS variables + shared classes in `styles.css`; base `tabular-nums`; both theme derivations. Nothing visible changes until surfaces adopt them.
2. **Per-surface restyles**, each independently verifiable: control band + results; sidebar + timeline frame; landing; person page (incl. rail deletion); reference/index pages + directory; header/footer.
3. **Zero logic changes.** The search hook, filter/order/histogram modules, URL state, server functions, and data pipeline are untouched. All 101 existing tests pass unmodified — any test edit is a red flag except deletions that covered the removed rail component.

## 6. Verification

- Full suite + `vp check` green after every task; `vp build` page count (~6,130) and landing-size (<100 KB) gates unchanged.
- Visual sweep per surface: dev server at desktop and ~390px widths, light and dark themes.
- The person-page DOM must contain exactly one identity rendering (the registry strip) — the duplication class of bug becomes impossible, verified by grep on built HTML.
