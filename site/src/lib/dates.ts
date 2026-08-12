/**
 * Format a year integer for display.
 * Negative values are BC, positive are AD. Zero = 1 BC (no year zero).
 */
export function formatYear(year: number, uncertain: boolean = false): string {
  const prefix = uncertain ? "c. " : ""
  if (year <= 0) {
    return `${prefix}${Math.abs(year) || 1} BC`
  }
  return `${prefix}AD ${year}`
}

/**
 * Format an era range like "540–509 BC" or "63 BC–AD 14".
 * Returns null if both values are null.
 */
export function formatEraRange(
  from: number | null,
  to: number | null
): string | null {
  if (from === null && to === null) return null
  const fromStr = from !== null ? formatYear(from) : "?"
  const toStr = to !== null ? formatYear(to) : "?"
  if (from !== null && from <= 0 && to !== null && to <= 0) {
    const fromNum = Math.abs(from) || 1
    const toNum = Math.abs(to) || 1
    return `${fromNum}\u2013${toNum} BC`
  }
  return `${fromStr}\u2013${toStr}`
}

export type EraLabel = "BC" | "AD"

/** "509 BC" \u2192 -509; "14 AD" \u2192 14. Years < 1 clamp to 1. */
export function toSignedYear(year: number, era: EraLabel): number {
  const y = Math.max(1, Math.floor(year))
  return era === "BC" ? -y : y
}

/** -509 \u2192 {year: 509, era: "BC"}; 0 \u2192 {year: 1, era: "BC"}; 14 \u2192 {year: 14, era: "AD"}. */
export function fromSignedYear(signed: number): {
  year: number
  era: EraLabel
} {
  if (signed <= 0) return { year: Math.abs(signed) || 1, era: "BC" }
  return { year: signed, era: "AD" }
}

/**
 * Format a year with its DateInformation interval qualifier:
 * "B" = before, "A" = after, "S"/null = a single year (plain formatYear).
 * The uncertainty "c." prefix composes inside: "before c. 216 BC".
 */
export function formatYearWithInterval(
  year: number,
  interval: string | null,
  uncertain: boolean = false
): string {
  const base = formatYear(year, uncertain)
  if (interval === "B") return `before ${base}`
  if (interval === "A") return `after ${base}`
  return base
}
