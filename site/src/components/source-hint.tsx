// site/src/components/source-hint.tsx
// Small book icon revealing the source citation(s) and note(s) behind a
// compact value (e.g. a tribe name in the registry strip).
import { BookOpen } from "lucide-react"
import { HoverPopover } from "./hover-popover"
import { SourceCitation } from "./source-citation"

export function SourceHint({
  sources,
  label,
}: {
  sources: { secondarySource: string; notes: string | null }[]
  label: string
}) {
  if (sources.length === 0) return null
  return (
    <HoverPopover
      contentClassName="max-w-72 text-xs"
      trigger={
        <button
          type="button"
          aria-label={label}
          className="ml-0.5 inline-flex cursor-help items-center align-baseline text-muted-foreground hover:text-foreground"
        >
          <BookOpen aria-hidden="true" className="h-3 w-3" />
        </button>
      }
    >
      <div className="space-y-2">
        {sources.map((s) => (
          <div key={`${s.secondarySource}|${s.notes ?? ""}`}>
            <SourceCitation name={s.secondarySource} className="text-xs" />
            {s.notes && <p className="text-muted-foreground">{s.notes}</p>}
          </div>
        ))}
      </div>
    </HoverPopover>
  )
}
