// site/src/data/context-line.ts
import { displayName } from "../lib/order"
import type { Person } from "./types"

/**
 * A one-line relationship anchor for persons with no career of their own,
 * mirroring the original DPRR results ("father of Ap. Claudius (321),
 * cos. 495"). Chooses the relative with a recorded highest office,
 * earliest era first (nulls last), so the anchor is the most historically
 * locatable member of the family.
 */
export function buildContextLine(
  person: Person,
  byId: Map<string, Person>
): string | null {
  if (person.postAssertions.length > 0) return null
  const candidates = person.relationships
    .map((rel) => ({ rel, related: byId.get(rel.relatedPersonId) }))
    .filter(
      (
        c
      ): c is { rel: (typeof person.relationships)[number]; related: Person } =>
        c.related !== undefined && c.related.highestOffice !== null
    )
  if (candidates.length === 0) return null
  candidates.sort(
    (a, b) =>
      (a.related.eraFrom ?? Number.MAX_SAFE_INTEGER) -
      (b.related.eraFrom ?? Number.MAX_SAFE_INTEGER)
  )
  const { rel, related } = candidates[0]
  // Relationship names already carry the preposition ("father of",
  // "married to"), so the line is "<type> <name>, <office>".
  return `${rel.relationshipType} ${displayName(related.name)}, ${related.highestOffice}`
}
