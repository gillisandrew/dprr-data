// site/src/components/hover-popover.tsx
// Popover that opens on hover (with a short delay) or keyboard focus, and
// still toggles on click/tap so touch devices work. Content stays open
// while the pointer is over the trigger or the content.
import { useEffect, useRef, useState } from "react"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"

export function HoverPopover({
  trigger,
  contentClassName,
  children,
}: {
  /** The trigger element (a button); handlers are merged onto it. */
  trigger: React.ReactElement
  contentClassName?: string
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined)

  // A pending open/close firing after unmount would set state on a gone
  // component; navigating away mid-hover is enough to hit it.
  useEffect(() => () => clearTimeout(timer.current), [])

  const openSoon = () => {
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setOpen(true), 150)
  }
  const closeSoon = () => {
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setOpen(false), 100)
  }
  const cancelClose = () => clearTimeout(timer.current)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        asChild
        onMouseEnter={openSoon}
        onMouseLeave={closeSoon}
        onFocus={(e) => {
          // Keyboard focus only — a tap/click focuses too, and letting it
          // open here would make the subsequent click-toggle close it.
          if (e.target.matches(":focus-visible")) setOpen(true)
        }}
        onBlur={() => setOpen(false)}
      >
        {trigger}
      </PopoverTrigger>
      <PopoverContent
        onMouseEnter={cancelClose}
        onMouseLeave={closeSoon}
        onOpenAutoFocus={(e) => e.preventDefault()}
        className={contentClassName}
      >
        {children}
      </PopoverContent>
    </Popover>
  )
}
