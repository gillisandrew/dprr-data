// site/src/components/era-timeline.tsx
import { useRef } from "react"
import type { Histogram } from "@/lib/histogram"
import { fromSignedYear } from "@/lib/dates"
import { YearInput } from "@/components/year-input"

interface EraTimelineProps {
  histogram: Histogram
  from: number | null
  to: number | null
  onChange: (from: number | null, to: number | null) => void
}

const W = 280
const H = 56

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

  const selFrom = from ?? start
  const selTo = to ?? end
  const dragging = useRef<"from" | "to" | null>(null)

  function pointerYear(e: React.PointerEvent): number {
    const rect = svgRef.current!.getBoundingClientRect()
    return xToYear(((e.clientX - rect.left) / rect.width) * W)
  }

  function onPointerDown(e: React.PointerEvent) {
    const y = pointerYear(e)
    dragging.current =
      Math.abs(y - selFrom) <= Math.abs(y - selTo) ? "from" : "to"
    ;(e.target as Element).setPointerCapture(e.pointerId)
    onPointerMove(e)
  }

  function onPointerMove(e: React.PointerEvent) {
    if (!dragging.current) return
    const y = pointerYear(e)
    if (dragging.current === "from") onChange(Math.min(y, selTo), to ?? null)
    else onChange(from ?? null, Math.max(y, selFrom))
  }

  function onPointerUp() {
    dragging.current = null
  }

  const labelCount = 5
  const labels = Array.from({ length: labelCount + 1 }, (_, i) =>
    Math.round(start + (i / labelCount) * span)
  )

  return (
    <div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        className="h-16 w-full cursor-col-resize touch-none select-none"
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
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
            <span key={i}>{isEdge ? axisYear(y) : fromSignedYear(y).year}</span>
          )
        })}
      </div>
      <div className="mt-2 flex items-center gap-2 text-xs">
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
