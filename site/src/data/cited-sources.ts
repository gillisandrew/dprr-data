// site/src/data/cited-sources.ts
import type { Person } from "./types"

/**
 * Every secondary source a person cites, anywhere in their record. Sources
 * appear on posts, statuses, relationships, dates, notes and tribes, so a
 * "cited by" list that only looked at careers would undercount badly.
 *
 * Shared deliberately: the /sources pages and the source search facet must
 * agree on what "cited by" means, or the facet count and the page's
 * "N persons cite this work" heading drift apart.
 */
export function citedSources(p: Person): Set<string> {
  const cited = new Set<string>()
  const add = (s: string | null | undefined) => {
    if (s) cited.add(s)
  }
  for (const pa of p.postAssertions) add(pa.secondarySource)
  for (const sa of p.statusAssertions) add(sa.secondarySource)
  for (const rel of p.relationships) add(rel.secondarySource)
  for (const d of p.dateInformation) add(d.secondarySource)
  for (const n of p.personNotes) add(n.secondarySource)
  for (const t of p.tribeAssertions) add(t.secondarySource)
  return cited
}
