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
import { buildTree, selectedAncestor, type TreeNode } from "@/lib/facet-tree"
import type { FacetValue } from "@/data/types"

interface FacetHierarchyGroupProps {
  title: string
  items: FacetValue[]
  /** child name → parent name (null/absent for roots) */
  parentOf: Record<string, string | null>
  selected: string[]
  onChange: (values: string[]) => void
  defaultOpen?: boolean
  /** Suppress the per-value result counts — used when the counts wouldn't
   * predict what clicking a value actually delivers (e.g. AND-mode or
   * in-range office filtering, where counts are computed disjunctively but
   * the applied filter is conjunctive). */
  hideCounts?: boolean
  /** Body only — no collapsible header or rule. For the filter panel's
   * sections, where the section trigger already names the facet. */
  frameless?: boolean
  /** Noun used in the "— incl. N {childNoun}" annotation for a selected
   * parent with selectable descendants. */
  childNoun?: string
}

export function FacetHierarchyGroup({
  title,
  items,
  parentOf,
  selected,
  onChange,
  defaultOpen = true,
  hideCounts = false,
  frameless = false,
  childNoun = "sub-offices",
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
        {node.count !== null
          ? (() => {
              const ancestor = selectedAncestor(node.name, parentOf, selected)
              const isSelected = selected.includes(node.name)
              if (ancestor && !isSelected) {
                return (
                  <label
                    className="flex items-center gap-2 py-0.5 text-[0.8125rem] leading-6 opacity-55"
                    title={`Included via ${ancestor}`}
                  >
                    <Checkbox
                      checked
                      disabled
                      aria-label={`${node.name} — included via ${ancestor}`}
                    />
                    <span className="min-w-0 truncate">{node.name}</span>
                    {!hideCounts && (
                      <span className="ml-auto text-xs text-muted-foreground">
                        {node.count}
                      </span>
                    )}
                  </label>
                )
              }
              return (
                <label className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6">
                  <Checkbox
                    checked={isSelected}
                    onCheckedChange={() => toggle(node.name)}
                  />
                  <span className="min-w-0 truncate">{node.name}</span>
                  {isSelected && node.selectableDescendants > 0 && (
                    <span className="text-xs text-muted-foreground italic">
                      — incl. {node.selectableDescendants} {childNoun}
                    </span>
                  )}
                  {!hideCounts && (
                    <span className="ml-auto text-xs text-muted-foreground">
                      {node.count}
                    </span>
                  )}
                </label>
              )
            })()
          : (() => {
              const ancestor = selectedAncestor(node.name, parentOf, selected)
              if (ancestor) {
                return (
                  <label
                    className="flex items-center gap-2 pt-2 pb-0.5 opacity-55"
                    title={`Included via ${ancestor}`}
                  >
                    <Checkbox
                      checked
                      disabled
                      aria-label={`${node.name} — included via ${ancestor}`}
                    />
                    <span className="micro-label-muted min-w-0 truncate">
                      {node.name}
                    </span>
                  </label>
                )
              }
              const isSelected = selected.includes(node.name)
              return (
                <label className="flex cursor-pointer items-center gap-2 pt-2 pb-0.5">
                  <Checkbox
                    checked={isSelected}
                    onCheckedChange={() => toggle(node.name)}
                  />
                  <span className="micro-label-muted min-w-0 truncate">
                    {node.name}
                  </span>
                  {isSelected && node.selectableDescendants > 0 && (
                    <span className="text-xs text-muted-foreground italic">
                      — incl. {node.selectableDescendants} {childNoun}
                    </span>
                  )}
                </label>
              )
            })()}
        {node.children.map((c) =>
          renderNode(c, node.count === null ? depth : depth + 1)
        )}
      </div>
    )
  }

  const body = (
    <>
      <Input
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        placeholder={`Filter ${title.toLowerCase()}...`}
        className={cn("mb-1 h-7 text-xs", !frameless && "mt-2")}
      />
      {filtered
        ? filtered.map((i) => (
            <label
              key={i.value}
              className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6"
            >
              <Checkbox
                checked={selected.includes(i.value)}
                onCheckedChange={() => toggle(i.value)}
              />
              <span className="min-w-0 truncate">{i.value}</span>
              {!hideCounts && (
                <span className="ml-auto text-xs text-muted-foreground">
                  {i.count}
                </span>
              )}
            </label>
          ))
        : tree.map((n) => renderNode(n, 0))}
    </>
  )

  if (frameless) return <div>{body}</div>

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="micro-label rule-hair flex w-full items-center justify-between pt-3 pb-1">
        {title}
        <ChevronRight
          className={cn("h-3 w-3 transition-transform", open && "rotate-90")}
        />
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-3 pl-1">{body}</CollapsibleContent>
    </Collapsible>
  )
}
