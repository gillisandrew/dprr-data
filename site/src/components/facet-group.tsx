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
}: {
  title: string
  items: FacetValue[]
  selected: string[]
  onChange: (selected: string[]) => void
  defaultOpen?: boolean
  searchable?: boolean
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
        {searchable && (
          <Input
            type="search"
            placeholder={`Filter ${title.toLowerCase()}...`}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="mb-2 h-7 text-xs"
          />
        )}
        <div className="space-y-1">
          {visible.map((item) => (
            <label
              key={item.value}
              className="flex cursor-pointer items-center gap-2 text-sm"
            >
              <Checkbox
                checked={selected.includes(item.value)}
                onCheckedChange={() => toggle(item.value)}
              />
              <span className="min-w-0 truncate">{item.value}</span>
              <span className="text-muted-foreground ml-auto text-xs">
                {item.count}
              </span>
            </label>
          ))}
        </div>
        {hasMore && !showAll && (
          <button
            onClick={() => setShowAll(true)}
            className="text-muted-foreground mt-1 text-xs hover:underline"
          >
            + {filtered.length - DEFAULT_VISIBLE} more...
          </button>
        )}
        {showAll && hasMore && (
          <button
            onClick={() => setShowAll(false)}
            className="text-muted-foreground mt-1 text-xs hover:underline"
          >
            Show less
          </button>
        )}
      </CollapsibleContent>
    </Collapsible>
  )
}
