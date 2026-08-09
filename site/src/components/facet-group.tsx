// site/src/components/facet-group.tsx
import { useState } from "react"
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

const DEFAULT_VISIBLE = 10

export function FacetGroup({
  title,
  items,
  selected,
  onChange,
  defaultOpen = true,
  searchable = false,
  frameless = false,
}: {
  title: string
  items: FacetValue[]
  selected: string[]
  onChange: (selected: string[]) => void
  defaultOpen?: boolean
  searchable?: boolean
  /** Body only — no collapsible header or rule. For popovers, where the
   * trigger pill already names the facet. */
  frameless?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  const [filter, setFilter] = useState("")
  const [showAll, setShowAll] = useState(false)

  const filtered = filter
    ? items.filter((item) =>
        item.value.toLowerCase().includes(filter.toLowerCase())
      )
    : items

  const visible = showAll ? filtered : filtered.slice(0, DEFAULT_VISIBLE)
  const hasMore = filtered.length > DEFAULT_VISIBLE

  function toggle(value: string) {
    if (selected.includes(value)) {
      onChange(selected.filter((v) => v !== value))
    } else {
      onChange([...selected, value])
    }
  }

  const body = (
    <>
      {searchable && (
        <Input
          type="search"
          placeholder={`Filter ${title.toLowerCase()}...`}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className={cn("mb-1 h-7 text-xs", !frameless && "mt-2")}
        />
      )}
      <div>
        {visible.map((item) => (
          <label
            key={item.value}
            className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6"
          >
            <Checkbox
              checked={selected.includes(item.value)}
              onCheckedChange={() => toggle(item.value)}
            />
            <span className="min-w-0 truncate">{item.value}</span>
            <span className="ml-auto text-xs text-muted-foreground">
              {item.count}
            </span>
          </label>
        ))}
      </div>
      {hasMore && !showAll && (
        <button
          onClick={() => setShowAll(true)}
          className="mt-1 text-xs text-muted-foreground hover:underline"
        >
          + {filtered.length - DEFAULT_VISIBLE} more...
        </button>
      )}
      {showAll && hasMore && (
        <button
          onClick={() => setShowAll(false)}
          className="mt-1 text-xs text-muted-foreground hover:underline"
        >
          Show less
        </button>
      )}
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
