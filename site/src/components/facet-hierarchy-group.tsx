// site/src/components/facet-hierarchy-group.tsx
import { useMemo, useState } from "react"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"
import type { FacetValue } from "@/data/types"

interface FacetHierarchyGroupProps {
  title: string
  items: FacetValue[]
  /** child name → parent name (null/absent for roots) */
  parentOf: Record<string, string | null>
  selected: string[]
  onChange: (values: string[]) => void
  defaultOpen?: boolean
}

interface TreeNode {
  name: string
  count: number | null // null → structural label only, not selectable
  children: TreeNode[]
}

function buildTree(
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
    return { name, count: countByName.get(name) ?? null, children }
  }
  return roots.map(toNode).sort((a, b) => a.name.localeCompare(b.name))
}

export function FacetHierarchyGroup({
  title,
  items,
  parentOf,
  selected,
  onChange,
  defaultOpen = true,
}: FacetHierarchyGroupProps) {
  const [open, setOpen] = useState(defaultOpen)
  const [filter, setFilter] = useState("")
  const tree = useMemo(() => buildTree(items, parentOf), [items, parentOf])

  function toggle(value: string) {
    onChange(
      selected.includes(value)
        ? selected.filter((v) => v !== value)
        : [...selected, value]
    )
  }

  const filtered = filter.trim()
    ? items.filter((i) =>
        i.value.toLowerCase().includes(filter.trim().toLowerCase())
      )
    : null

  function renderNode(node: TreeNode, depth: number) {
    return (
      <div key={node.name} style={{ paddingLeft: depth * 12 }}>
        {node.count !== null ? (
          <label className="flex cursor-pointer items-center gap-2 py-0.5 text-sm">
            <Checkbox
              checked={selected.includes(node.name)}
              onCheckedChange={() => toggle(node.name)}
            />
            <span className="min-w-0 truncate">{node.name}</span>
            <span className="ml-auto text-xs text-muted-foreground">
              {node.count}
            </span>
          </label>
        ) : (
          <label className="flex cursor-pointer items-center gap-2 pt-2 pb-0.5 text-sm">
            <Checkbox
              checked={selected.includes(node.name)}
              onCheckedChange={() => toggle(node.name)}
            />
            <span className="min-w-0 truncate text-xs font-semibold text-muted-foreground uppercase">
              {node.name}
            </span>
          </label>
        )}
        {node.children.map((c) =>
          renderNode(c, node.count === null ? depth : depth + 1)
        )}
      </div>
    )
  }

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex w-full items-center gap-1.5 py-2 text-sm font-semibold">
        <ChevronRight
          className={cn(
            "h-3.5 w-3.5 shrink-0 transition-transform",
            open && "rotate-90"
          )}
        />
        {title}
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-3 pl-5">
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={`Filter ${title.toLowerCase()}...`}
          className="mb-1 h-7 text-xs"
        />
        {filtered
          ? filtered.map((i) => (
              <label
                key={i.value}
                className="flex cursor-pointer items-center gap-2 py-0.5 text-sm"
              >
                <Checkbox
                  checked={selected.includes(i.value)}
                  onCheckedChange={() => toggle(i.value)}
                />
                <span className="min-w-0 truncate">{i.value}</span>
                <span className="ml-auto text-xs text-muted-foreground">
                  {i.count}
                </span>
              </label>
            ))
          : tree.map((n) => renderNode(n, 0))}
      </CollapsibleContent>
    </Collapsible>
  )
}
