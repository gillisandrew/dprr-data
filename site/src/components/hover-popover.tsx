// site/src/components/hover-popover.tsx
// Popover that opens on hover (with a short delay) or keyboard focus, and
// toggles on click/tap so touch devices work.
//
// The model, in one line: hover to peek, click to keep.
//
//   - Hovering opens it; leaving closes it again.
//   - Clicking toggles, so every click has a visible effect.
//   - A popover opened by a click is *kept*: it ignores the pointer leaving,
//     so moving across to read or select its text no longer dismisses it.
//     Click again, press Escape, or click outside to close.
//
// Radix only calls onOpenChange from its own interactions — a trigger
// click/tap, Escape, or an outside click — never in response to the hover
// timers setting `open` here. Verified by instrumenting every handler: one
// onOpenChange per click, alternating true/false, with no duplicate from the
// dismissable layer. That is what makes "onOpenChange(true) means the user
// clicked it open" a safe inference.
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
  // Read inside timer callbacks and blur, which can fire after `kept`
  // changes; a state value captured at schedule time would be stale.
  const kept = useRef(false)

  // A pending open/close firing after unmount would set state on a gone
  // component; navigating away mid-hover is enough to hit it.
  useEffect(() => () => clearTimeout(timer.current), [])

  const openSoon = () => {
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setOpen(true), 150)
  }
  const closeSoon = () => {
    if (kept.current) return
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setOpen(false), 100)
  }
  const cancelClose = () => clearTimeout(timer.current)

  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        // Cancel any hover timer first: moving the pointer back to the
        // trigger in order to click it also schedules an open, which would
        // otherwise re-open the popover the click just closed.
        clearTimeout(timer.current)
        kept.current = next
        setOpen(next)
      }}
    >
      <PopoverTrigger
        asChild
        onMouseEnter={openSoon}
        onMouseLeave={closeSoon}
        onFocus={(e) => {
          // Keyboard focus only — a tap/click focuses too, and letting it
          // open here would make the subsequent click-toggle close it.
          if (e.target.matches(":focus-visible")) setOpen(true)
        }}
        onBlur={() => {
          // A kept popover survives losing focus, so clicking a link inside
          // its content doesn't dismiss it mid-interaction.
          if (kept.current) return
          setOpen(false)
        }}
      >
        {trigger}
      </PopoverTrigger>
      <PopoverContent
        onMouseEnter={cancelClose}
        onMouseLeave={closeSoon}
        onOpenAutoFocus={(e) => e.preventDefault()}
        // Radix restores focus to the trigger on close. Chrome reports that
        // programmatic focus as :focus-visible, so the onFocus handler above
        // fired and immediately re-opened the popover — a hover-opened one
        // could never be dismissed by moving away. The content never takes
        // focus (onOpenAutoFocus is prevented), so there is nothing to
        // restore and suppressing it is safe.
        onCloseAutoFocus={(e) => e.preventDefault()}
        className={contentClassName}
      >
        {children}
      </PopoverContent>
    </Popover>
  )
}
