# DPRR Favicon / Site Icon — Design

Date: 2026-08-09. Decided interactively (visual companion session; screens in
`.superpowers/brainstorm/53563-1786312654/content/`).

## The mark

A stroke-drawn **tabula ansata** — the dovetail-handled inscription plaque of
Roman epigraphy, the shape the fasti were carved on. Geometry (the approved
"B1" variant, 32×32 viewBox, stroke width 3, round joins/caps):

- plaque: `rect x=9 y=9 w=14 h=14`
- left wing: `M9 12.5 L4 10 V22 L9 19.5`
- right wing: `M23 12.5 L28 10 V22 L23 19.5`
- two inscribed lines: `M13 14.5 H19` and `M13 17.5 H19`

Treatment: bare glyph in the site accent red on transparent background
("Editorial Ledger" accent ink). No tile for the browser favicon.

## Assets (all in `site/public/`)

| File | Content | Notes |
|------|---------|-------|
| `icon.svg` | bare glyph, transparent | primary favicon; `@media (prefers-color-scheme: dark)` swaps stroke `oklch(0.505 0.213 27.5)` → `oklch(0.68 0.177 26.9)` |
| `favicon.ico` | bare glyph, 32+16 px | legacy fallback; light-mode red baked in |
| `apple-touch-icon.png` | 180 px, glyph on warm-paper tile | touch icons can't be transparent |
| `icon-192.png`, `icon-512.png` | glyph on warm-paper tile | referenced from manifest |
| `manifest.json` | rewritten | name "DPRR — Digital Prosopography of the Roman Republic", short_name "DPRR", correct icon paths, paper background color; replaces stock "TanStack App" content referencing nonexistent logos |

Paper tile: rounded square (radius ≈ 7/32 of the canvas) filled with the
light-theme background `oklch(0.993 0.003 83)`, glyph in light-mode accent red.

## Wiring

`head()` in `src/routes/__root.tsx` gains links, hrefs prefixed with
`import.meta.env.BASE_URL` so they resolve under `/dprr-data/`:

- `<link rel="icon" type="image/svg+xml" href=…/icon.svg>`
- `<link rel="icon" href=…/favicon.ico sizes="any">`
- `<link rel="apple-touch-icon" href=…/apple-touch-icon.png>`
- `<link rel="manifest" href=…/manifest.json>`

This is required regardless of design: with no explicit link, browsers request
`/favicon.ico` from the origin root, which the `/dprr-data/` project page never
serves.

## Generation

`icon.svg` is hand-authored and committed (browsers support `oklch()` in SVG
CSS). PNGs and ICO are rasterized once from hex-color variants of the SVG
(rasterizers don't reliably support `oklch()`) via one-off `vp dlx` tooling —
not a build step, no new dependencies — and committed as static files.

## Verification

`vp build`; confirm the link tags appear in prerendered HTML with the base
path; view the dev server tab icon in light and dark themes; confirm deployed
tab icon and valid manifest.
