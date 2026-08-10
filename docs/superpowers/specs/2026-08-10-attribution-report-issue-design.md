# Attribution, unofficial-site disclaimer, and per-record issue reporting

Date: 2026-08-10
Status: approved

## Goal

Give the DPRR data proper attribution, make clear the site is an unofficial
rendering (not the DPRR project or King's College London), and let readers
report data problems against the exact source TTL file in this repo.

## Components

### 1. `/about` page (`site/src/routes/about.tsx`)

Prose page, three sections:

- **What this is** — a friendlier read-only interface to the DPRR dataset,
  statically built from the TTL files in this repository.
- **Not the official project** — this site is not affiliated with or endorsed
  by the DPRR project or King's College London. The official site is
  <https://romanrepublic.ac.uk>. Errors introduced by this rendering are ours,
  not theirs.
- **Attribution & license** — data from the Digital Prosopography of the Roman
  Republic (King's College London), CC BY-NC 4.0 (link to license), changes
  were made (link to the README's "Changes from upstream" section on GitHub),
  links to this repo and the official site.

### 2. Footer (`site/src/components/site-footer.tsx`)

Replace the current credit line with:
"Unofficial interface to data from the Digital Prosopography of the Roman
Republic (KCL) · CC BY-NC 4.0 · About" — license text links to the CC deed,
About links to `/about`. Existing Directory link stays.

### 3. GitHub issue form (`.github/ISSUE_TEMPLATE/data-issue.yml`)

Issue form (YAML), auto-label `data`, fields:

| id | type | notes |
|----|------|-------|
| `entity` | input | prefilled entity label/ID, required |
| `file` | input | prefilled repo-relative source path, required |
| `type` | dropdown | wrong date / wrong office or post / missing or wrong relationship / mapping or curation error / rendering bug (site, not data) / other |
| `description` | textarea | required |
| `sources` | textarea | optional citations supporting the correction |

`config.yml` adds a contact link pointing to the official DPRR site for
questions about the underlying scholarship. Blank issues stay enabled.

### 4. Report-issue link

- `site/src/lib/report-issue.ts` — pure builder: given
  `{ entityLabel, ttlPath, dprrUri? }`, returns the
  `https://github.com/gillisandrew/dprr-data/issues/new` URL with
  `template=data-issue.yml`, `title=[data] <entityLabel>`, and field params
  `entity`, `file`. Also exports per-entity path helpers:
  - person: `persons/${id.slice(0, 4)}/${id}.ttl` (shard.py rule: dir = id[:4])
  - office: `reference/offices.ttl`
  - province: `reference/provinces.ttl`
  - tribe: `reference/tribes.ttl`
  - gens: `persons/<code>/` (directory; code from first member's id)
- `site/src/components/report-issue-link.tsx` — muted line at the bottom of
  each detail page: "Spotted an error? Report an issue with this record."
- Wired into all five detail routes: `persons.$id`, `offices.$slug`,
  `provinces.$slug`, `tribes.$slug`, `gentes.$slug`.

## Testing

- Unit tests (`report-issue.test.ts`): URL encoding of title/fields, correct
  template param, per-entity-type paths (including the gens directory form).
- `vp check` and `vp test` pass; manual dev-server smoke of `/about` and one
  page per entity type.

## Out of scope

- Per-assertion granularity (links target the whole record).
- Disabling blank issues.
