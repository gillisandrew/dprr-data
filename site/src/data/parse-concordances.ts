// site/src/data/parse-concordances.ts
import { Parser } from "n3"
import type { Concordance } from "./types"

const OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
const SKOS_EXACT_MATCH = "http://www.w3.org/2004/02/skos/core#exactMatch"
const PERSON_PREFIX = "http://romanrepublic.ac.uk/rdf/entity/Person/"

/**
 * Parse a concordance TTL file and return a map from person numeric ID
 * to concordance entries.
 */
export function parseConcordanceTtl(
  system: string,
  ttl: string
): Map<string, Concordance[]> {
  const parser = new Parser()
  const quads = parser.parse(ttl)
  const result = new Map<string, Concordance[]>()

  for (const q of quads) {
    const subjectUri = q.subject.value
    if (!subjectUri.startsWith(PERSON_PREFIX)) continue

    const personNumericId = subjectUri.slice(PERSON_PREFIX.length)
    let predicate: Concordance["predicate"]

    if (q.predicate.value === OWL_SAME_AS) {
      predicate = "owl:sameAs"
    } else if (q.predicate.value === SKOS_EXACT_MATCH) {
      predicate = "skos:exactMatch"
    } else {
      continue
    }

    const entry: Concordance = {
      system,
      uri: q.object.value,
      predicate,
    }

    const existing = result.get(personNumericId) ?? []
    existing.push(entry)
    result.set(personNumericId, existing)
  }

  return result
}
