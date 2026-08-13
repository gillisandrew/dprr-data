# Source and relationship search facets

Date: 2026-08-13

Add two facets to the filter panel: the secondary source a person is
documented by, and the kinds of relationship recorded for them.

## Scope correction

The original request named three facets — source, praenomen and
relationship. **Praenomen already exists**: `SearchState.praenomen`,
`facets.praenomen`, and a working control in the Name section. Nothing to
build. This spec covers the two that genuinely don't exist.

## Definitions

**Source.** A person is documented by a source if that source appears
anywhere in their record — on posts, statuses, relationships, dates, notes,
or tribes. This is deliberately the same definition the `/sources` pages
use, so the facet count and the "N persons cite this work" heading can never
disagree.

**Relationship.** A person matches a relationship type if they have at least
one relationship of that type. The facet answers "who has a recorded
adoption", not "who is adopted by whom" — the paired view is what
`/relationships/$slug` is for.

## Measured shape

| | value |
| --- | --- |
| Distinct sources per person | mean 2.28, max 11 |
| Source facet cardinality | 32 in use (34 exist) |
| Distinct relationship types per person | mean 0.97, max 9 |
| Relationship facet cardinality | 36 |
| Persons with any relationship | 2,137 of 4,876 |

## Storage: abbreviations, not titles

Sources are stored on the summary as their **abbreviation**
(`"Broughton MRR I"`), not the full title (`"The Magistrates of the Roman
Republic, Vol. I"`). All 34 sources have one, so there is no fallback case.

This is both the size and the usability decision. Full titles reach 108
characters and would break a filter list; abbreviations cap at 27. Measured
against the current 2.25 MB `search-data.json`:

| Addition | Size | Growth |
| --- | --- | --- |
| Sources as abbreviations | 184 KB | +8.2% |
| *(Sources as full titles)* | *450 KB* | *+20.0%* |
| Relationship types | 68 KB | +3.0% |
| **Total** | **252 KB** | **+11.2%** |

Accepted consequence: the facet label and the `/sources` page heading differ
(abbreviation vs full title). The detail page shows both, so the connection
stays discoverable.

## Architecture

### Shared "cited by" logic

`citedSources(person)` — which walks all six places a source appears —
currently lives in `aggregate-references.ts` and serves the `/sources`
pages. It moves to `src/data/cited-sources.ts` so the facet and the source
pages share one implementation. Two copies would silently disagree the first
time the data grows a new source-bearing field, and the whole point of
choosing "cited anywhere" was that the two agree.

### A distinct summary type for search

`PersonSummary` is **not** extended. It is reused verbatim by the gens and
tribe detail payloads, which have no use for these fields, and adding two
always-empty arrays across ~5,000 records is pure waste.

Instead `types.ts` gains:

```ts
export interface SearchSummary extends PersonSummary {
  /** Source abbreviations, from every part of the record. */
  sources: string[]
  relationshipTypes: string[]
}
```

`SearchPayload.summaries` becomes `SearchSummary[]`. `buildSearchPayload`
already receives `refs`, so it can map source names to abbreviations without
changing `toSummaries`, whose other callers stay untouched. `filter.ts` and
`search.ts` widen from `PersonSummary` to `SearchSummary`; since
`SearchSummary` extends it, `FastiRow`/`ResultsList` consumers are
unaffected.

### Filter semantics

OR-within-facet through the existing `matchesSelection`, matching tribe,
provincia and events. Not the AND semantics `status` uses: "cited by
Broughton **or** Zmeskal" is the natural reading, and AND would return
almost nothing. Facet counts stay disjunctive, consistent with the existing
deliberate behaviour.

### UI

Both go in the **"More filters"** tier beside Tribe, Provincia and Events:

- `{ key: "source", label: "Source" }` — `FacetCombobox`, since 32 values is
  too many for a flat list.
- `{ key: "relationship", label: "Relationship" }` — `FacetGroup`; 36 values
  but short labels.

Both get a glossary `InfoHint`, because "source" and "relationship" each
mean something specific here — `relationship` in particular sits beside the
existing Father/Grandfather filiation filters, which are relationships in
the ordinary sense but not in this one.

### URL parameters

`source=` and `relationship=`, via the existing `search-params.ts`
serializer, including its whitespace-trimming behaviour.

## Error handling

| Case | Behaviour |
| --- | --- |
| Source with no abbreviation | Cannot occur — all 34 have one. If the data changes, fall back to the full name rather than dropping the person from the facet. |
| Person with no sources | Empty array; matches no source selection. |
| Unknown value in a URL param | Existing behaviour: matches nothing, chip still renders so it can be removed. |

## Testing

- `citedSources` after extraction: covers all six source-bearing fields, and
  deduplicates a source cited twice.
- `matchesFacets` for each new facet, including OR-within-facet.
- Param round-tripping for `source=` and `relationship=`.
- A real-browser check that selecting a source narrows the result count. The
  last round demonstrated that a fully green suite can coexist with a broken
  UI, so this is not optional.

## Out of scope

- Praenomen (already shipped).
- AND semantics for either facet.
- Filtering by the *other end* of a relationship — `/relationships/$slug`
  covers that.
- Interning the source vocabulary as integer indices; abbreviations already
  bring the growth to an acceptable +11.2%.
