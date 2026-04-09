// site/src/components/facet-range-group.tsx
import { useEffect, useRef, useState } from "react"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Input } from "@/components/ui/input"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"

function useDebouncedNumber(
  value: number | null,
  onChange: (v: number | null) => void,
  delay = 400
) {
  const [local, setLocal] = useState(value?.toString() ?? "")
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>(undefined)

  useEffect(() => {
    setLocal(value?.toString() ?? "")
  }, [value])

  // Clear pending debounce on unmount
  useEffect(() => () => clearTimeout(timeoutRef.current), [])

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    setLocal(v)
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => {
      onChange(v === "" ? null : Number(v))
    }, delay)
  }

  return { local, handleChange }
}

export function FacetRangeGroup({
  title,
  fromValue,
  toValue,
  onFromChange,
  onToChange,
  fromPlaceholder = "From",
  toPlaceholder = "To",
  defaultOpen = true,
}: {
  title: string
  fromValue: number | null
  toValue: number | null
  onFromChange: (value: number | null) => void
  onToChange: (value: number | null) => void
  fromPlaceholder?: string
  toPlaceholder?: string
  defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  const from = useDebouncedNumber(fromValue, onFromChange)
  const to = useDebouncedNumber(toValue, onToChange)

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
        <p className="text-muted-foreground mb-2 text-xs">
          Use negative numbers for BC (e.g., -509)
        </p>
        <div className="flex items-center gap-2">
          <Input
            type="number"
            placeholder={fromPlaceholder}
            value={from.local}
            onChange={from.handleChange}
            className="h-7 text-xs"
          />
          <span className="text-muted-foreground text-xs">to</span>
          <Input
            type="number"
            placeholder={toPlaceholder}
            value={to.local}
            onChange={to.handleChange}
            className="h-7 text-xs"
          />
        </div>
      </CollapsibleContent>
    </Collapsible>
  )
}
