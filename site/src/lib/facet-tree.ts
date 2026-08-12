// site/src/lib/facet-tree.ts
// Pure tree math for hierarchical facets (offices, locations): building the
// display tree from the current facet universe, resolving implied selection
// through ancestors, and deciding which selected values actually cover
// descendants in the universe. Extracted from facet-hierarchy-group.tsx so
// the logic is unit-testable and shared with the active-filter chips.
import type { FacetValue } from "@/data/types"

export interface TreeNode {
  name: string
  count: number | null // null → structural label only, not selectable
  children: TreeNode[]
  /** Count of selectable (non-structural) descendants, for the "included
   * via this selection" annotation. */
  selectableDescendants: number
}

export function buildTree(
  items: FacetValue[],
  parentOf: Record<string, string | null>
): TreeNode[] {
  const countByName = new Map(items.map((i) => [i.value, i.count]))
  // Universe: item names plus all their ancestors
  const keep = new Set<string>()
  for (const i of items) {
    let current: string | null = i.value
    while (current && !keep.has(current)) {
      keep.add(current)
      current = parentOf[current] ?? null
    }
  }
  const childrenOf = new Map<string, string[]>()
  const roots: string[] = []
  for (const name of keep) {
    const parent = parentOf[name] ?? null
    if (parent && keep.has(parent)) {
      const list = childrenOf.get(parent) ?? []
      list.push(name)
      childrenOf.set(parent, list)
    } else {
      roots.push(name)
    }
  }
  function toNode(name: string): TreeNode {
    const children = (childrenOf.get(name) ?? []).map(toNode)
    children.sort((a, b) => (b.count ?? 0) - (a.count ?? 0))
    const selectableDescendants = children.reduce(
      (sum, c) => sum + c.selectableDescendants + (c.count !== null ? 1 : 0),
      0
    )
    return {
      name,
      count: countByName.get(name) ?? null,
      children,
      selectableDescendants,
    }
  }
  return roots.map(toNode).sort((a, b) => a.name.localeCompare(b.name))
}

/** Nearest selected ancestor of `name`, or null. */
export function selectedAncestor(
  name: string,
  parentOf: Record<string, string | null>,
  selected: string[]
): string | null {
  let cur = parentOf[name] ?? null
  while (cur) {
    if (selected.includes(cur)) return cur
    cur = parentOf[cur] ?? null
  }
  return null
}

/** Subset of `values` that have at least one strict descendant among the
 * current facet universe — the same universe the tree renders — so chip
 * "+ sub-offices" wording never disagrees with the tree's annotations. */
export function valuesWithDescendantsInUniverse(
  values: string[],
  parentOf: Record<string, string | null>,
  universe: FacetValue[]
): Set<string> {
  const selected = new Set(values)
  const result = new Set<string>()
  for (const item of universe) {
    let cur = parentOf[item.value] ?? null
    while (cur) {
      if (selected.has(cur)) result.add(cur)
      cur = parentOf[cur] ?? null
    }
  }
  return result
}
