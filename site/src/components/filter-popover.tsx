import { useEffect, useState } from "react"
import { Dialog as DialogPrimitive } from "radix-ui"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"

function useIsDesktop(): boolean {
  const [isDesktop, setIsDesktop] = useState(true)
  useEffect(() => {
    const mq = window.matchMedia("(min-width: 768px)")
    setIsDesktop(mq.matches)
    const onChange = (e: MediaQueryListEvent) => setIsDesktop(e.matches)
    mq.addEventListener("change", onChange)
    return () => mq.removeEventListener("change", onChange)
  }, [])
  return isDesktop
}

interface FilterPopoverProps {
  label: string
  activeCount: number
  open: boolean
  onOpenChange: (open: boolean) => void
  children: React.ReactNode
}

/**
 * One facet group behind a pill trigger: Radix Popover on desktop, a
 * bottom-sheet Dialog under the md breakpoint. The parent owns open
 * state so only one group is open at a time.
 */
export function FilterPopover({
  label,
  activeCount,
  open,
  onOpenChange,
  children,
}: FilterPopoverProps) {
  const isDesktop = useIsDesktop()

  const trigger = (
    <button
      type="button"
      className={
        activeCount > 0
          ? "micro-label rule-hair pb-0.5"
          : "micro-label-muted rule-hair pb-0.5 hover:text-foreground"
      }
    >
      {label}
      {activeCount > 0 && ` (${activeCount})`}
      <span aria-hidden="true"> ▾</span>
    </button>
  )

  if (isDesktop) {
    return (
      <Popover open={open} onOpenChange={onOpenChange}>
        <PopoverTrigger asChild>{trigger}</PopoverTrigger>
        <PopoverContent className="max-h-[70vh] overflow-y-auto">
          {children}
        </PopoverContent>
      </Popover>
    )
  }

  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Trigger asChild>{trigger}</DialogPrimitive.Trigger>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-black/30" />
        <DialogPrimitive.Content className="fixed inset-x-0 bottom-0 z-50 max-h-[75vh] overflow-y-auto border-t border-border bg-background p-4">
          <DialogPrimitive.Title className="micro-label pb-2">
            {label}
          </DialogPrimitive.Title>
          {children}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  )
}
