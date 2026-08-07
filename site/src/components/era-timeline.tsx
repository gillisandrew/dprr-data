// site/src/components/era-timeline.tsx
import { useRef, useState } from "react"
import type { Histogram } from "@/lib/histogram"
import { fromSignedYear } from "@/lib/dates"
import { YearInput } from "@/components/year-input"

interface EraTimelineProps {
  histogram: Histogram
  from: number | null
  to: number | null
  onChange: (from: number | null, to: number | null) => void
}

interface Draft {
  from: number | null
  to: number | null
}

const W = 800
const H = 80

/** Axis/label text for a signed year; never a minus sign. */
function axisYear(signed: number): string {
  const { year, era } = fromSignedYear(signed)
  return era === "BC" ? `${year} BC` : `AD ${year}`
}

export function EraTimeline({
  histogram,
  from,
  to,
  onChange,
}: EraTimelineProps) {
  const svgRef = useRef<SVGSVGElement>(null)
  // Non-null while dragging; the visual selection renders from this instead
  // of props so pointer moves don't trigger onChange (and therefore no
  // result reload / scroll jump) until the drag is released.
  const [draft, setDraft] = useState<Draft | null>(null)
  const { start, binSize, counts } = histogram
  const end = start + counts.length * binSize
  const span = end - start

  const yearToX = (y: number) =>
    ((Math.min(Math.max(y, start), end) - start) / span) * W
  const xToYear = (x: number) =>
    Math.round(start + (Math.min(Math.max(x, 0), W) / W) * span)

  const max = Math.max(...counts, 1)
  const points = counts.map((c, i) => {
    const x = ((i + 0.5) / counts.length) * W
    const y = H - (c / max) * (H - 4)
    return `${x},${y}`
  })
  const areaPath = `M0,${H} L${points.join(" L")} L${W},${H} Z`

  const activeFrom = draft ? draft.from : from
  const activeTo = draft ? draft.to : to
  const selFrom = activeFrom ?? start
  const selTo = activeTo ?? end
  const dragging = useRef<"from" | "to" | null>(null)

  function pointerYear(e: React.PointerEvent): number {
    const rect = svgRef.current!.getBoundingClientRect()
    return xToYear(((e.clientX - rect.left) / rect.width) * W)
  }

  function onPointerDown(e: React.PointerEvent) {
    const y = pointerYear(e)
    const dir = Math.abs(y - selFrom) <= Math.abs(y - selTo) ? "from" : "to"
    dragging.current = dir
    ;(e.target as Element).setPointerCapture(e.pointerId)
    setDraft(
      dir === "from"
        ? { from: Math.min(y, selTo), to }
        : { from, to: Math.max(y, selFrom) }
    )
  }

  function onPointerMove(e: React.PointerEvent) {
    if (!dragging.current) return
    const y = pointerYear(e)
    setDraft((prev) => {
      const base = prev ?? { from, to }
      if (dragging.current === "from") {
        return { from: Math.min(y, base.to ?? end), to: base.to }
      }
      return { from: base.from, to: Math.max(y, base.from ?? start) }
    })
  }

  function commitDraft() {
    dragging.current = null
    if (draft) onChange(draft.from, draft.to)
    setDraft(null)
  }

  const labelCount = 5
  const labels = Array.from({ length: labelCount + 1 }, (_, i) =>
    Math.round(start + (i / labelCount) * span)
  )

  return (
    <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-4">
      <div className="min-w-0 flex-1">
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          className="h-20 w-full cursor-col-resize touch-none select-none"
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={commitDraft}
          onPointerCancel={commitDraft}
          onLostPointerCapture={commitDraft}
          role="slider"
          aria-label="Time period"
          aria-valuetext={`${axisYear(selFrom)} to ${axisYear(selTo)}`}
        >
          <path d={areaPath} className="fill-muted-foreground/25" />
          <rect
            x={yearToX(selFrom)}
            y={0}
            width={Math.max(yearToX(selTo) - yearToX(selFrom), 0)}
            height={H}
            className="fill-primary/15"
          />
          <line
            x1={yearToX(selFrom)}
            x2={yearToX(selFrom)}
            y1={0}
            y2={H}
            className="stroke-primary"
            strokeWidth={2}
          />
          <line
            x1={yearToX(selTo)}
            x2={yearToX(selTo)}
            y1={0}
            y2={H}
            className="stroke-primary"
            strokeWidth={2}
          />
        </svg>
        <div className="flex justify-between text-[10px] text-muted-foreground">
          {labels.map((y, i) => {
            const isEdge = i === 0 || i === labels.length - 1
            return (
              <span key={i}>
                {isEdge ? axisYear(y) : fromSignedYear(y).year}
              </span>
            )
          })}
        </div>
      </div>
      <div className="flex shrink-0 items-center gap-2 text-xs">
        <YearInput
          value={from}
          onChange={(v) => onChange(v, to)}
          placeholder="509"
          aria-label="from year"
        />
        <span className="text-muted-foreground">to</span>
        <YearInput
          value={to}
          onChange={(v) => onChange(from, v)}
          placeholder="31"
          aria-label="to year"
        />
        {(from !== null || to !== null) && (
          <button
            onClick={() => onChange(null, null)}
            className="text-xs text-muted-foreground underline hover:text-foreground"
          >
            clear
          </button>
        )}
      </div>
    </div>
  )
}
