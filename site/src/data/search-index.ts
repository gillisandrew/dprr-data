// site/src/data/search-index.ts
import MiniSearch, { type Options } from "minisearch"
import type { PersonSummary } from "./types"

export const MINISEARCH_OPTIONS: Options<PersonSummary> = {
  fields: ["name", "nomen", "cognomen", "otherNames", "highestOffice"],
  storeFields: [
    "id",
    "name",
    "nomen",
    "cognomen",
    "sex",
    "statuses",
    "highestOffice",
    "eraFrom",
    "eraTo",
    "tribes",
    "offices",
  ],
  idField: "id",
  searchOptions: {
    prefix: true,
    fuzzy: 0.2,
    boost: { name: 2, cognomen: 1.5, nomen: 1.5 },
  },
}

/**
 * Build a MiniSearch index and return its JSON-serializable form.
 * The returned object can be passed to MiniSearch.loadJSON() on the client.
 */
export function buildSearchIndex(
  summaries: PersonSummary[]
): ReturnType<MiniSearch<PersonSummary>["toJSON"]> {
  const ms = new MiniSearch<PersonSummary>(MINISEARCH_OPTIONS)
  ms.addAll(summaries)
  return ms.toJSON()
}
