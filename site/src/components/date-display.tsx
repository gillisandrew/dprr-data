// site/src/components/date-display.tsx
import { formatYear, formatEraRange } from "@/lib/dates"

export function DateDisplay({
  year,
  uncertain = false,
}: {
  year: number
  uncertain?: boolean
}) {
  return <span>{formatYear(year, uncertain)}</span>
}

export function EraRange({
  from,
  to,
}: {
  from: number | null
  to: number | null
}) {
  const text = formatEraRange(from, to)
  if (!text) return null
  return <span>{text}</span>
}
