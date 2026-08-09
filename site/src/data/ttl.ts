// site/src/data/ttl.ts
// Shared TTL plumbing: parse a file and group its quads by subject, with
// rdf:type split out from the ordinary predicates.
import { Parser } from "n3"

export const DPRR = "http://romanrepublic.ac.uk/rdf/ontology#"
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

export interface QuadGroup {
  type: string | null
  props: Map<string, string[]>
}

export function groupSubjects(ttl: string): Map<string, QuadGroup> {
  const map = new Map<string, QuadGroup>()
  if (!ttl) return map
  for (const q of new Parser().parse(ttl)) {
    const subj = q.subject.value
    if (!map.has(subj)) map.set(subj, { type: null, props: new Map() })
    const entry = map.get(subj)!
    if (q.predicate.value === RDF_TYPE) {
      entry.type = q.object.value
    } else {
      const pred = q.predicate.value
      if (!entry.props.has(pred)) entry.props.set(pred, [])
      entry.props.get(pred)!.push(q.object.value)
    }
  }
  return map
}

/** First value of a DPRR-namespaced predicate, or null. */
export function first(g: QuadGroup, pred: string): string | null {
  return g.props.get(`${DPRR}${pred}`)?.[0] ?? null
}

export function firstNum(g: QuadGroup, pred: string): number | null {
  const v = first(g, pred)
  if (v === null) return null
  const n = Number(v)
  return Number.isNaN(n) ? null : n
}

/** All values of a DPRR-namespaced predicate. */
export function all(g: QuadGroup, pred: string): string[] {
  return g.props.get(`${DPRR}${pred}`) ?? []
}
