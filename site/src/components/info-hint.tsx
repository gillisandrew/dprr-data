// site/src/components/info-hint.tsx
// Small ⓘ (or custom mark) trigger opening a glossary explanation.
import { Info } from "lucide-react"
import { HoverPopover } from "./hover-popover"
import { GLOSSARY, type GlossaryTermId } from "@/lib/glossary"

export function InfoHint({
  term,
  mark,
}: {
  term: GlossaryTermId
  /** Custom trigger text (e.g. "?" for uncertainty markers); ⓘ otherwise. */
  mark?: string
}) {
  const entry = GLOSSARY[term]
  return (
    <HoverPopover
      contentClassName="max-w-72 text-xs"
      trigger={
        <button
          type="button"
          aria-label={`What does ${entry.label} mean?`}
          // The icon is 12px square; the ::after overlay widens the hit area
          // to ~24px (the WCAG 2.5.8 minimum) without occupying any layout
          // space, so surrounding text is unmoved.
          className="relative inline-flex cursor-help items-center align-baseline text-muted-foreground after:absolute after:-inset-1.5 after:content-[''] hover:text-foreground"
        >
          {mark ?? <Info aria-hidden="true" className="h-3 w-3" />}
        </button>
      }
    >
      <p className="micro-label mb-1">{entry.label}</p>
      <p>{entry.text}</p>
    </HoverPopover>
  )
}
