// site/src/components/section.tsx
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { ChevronRight } from "lucide-react"
import { useState } from "react"
import { cn } from "@/lib/utils"

export function Section({
  title,
  children,
  defaultOpen = true,
  count,
}: {
  title: string
  children: React.ReactNode
  defaultOpen?: boolean
  count?: number
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex w-full items-center gap-2 py-3">
        <ChevronRight
          className={cn(
            "h-4 w-4 shrink-0 transition-transform",
            open && "rotate-90"
          )}
        />
        <h2 className="font-heading text-lg font-semibold">{title}</h2>
        {count !== undefined && (
          <span className="text-sm text-muted-foreground">({count})</span>
        )}
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-4 pl-6">{children}</CollapsibleContent>
    </Collapsible>
  )
}
