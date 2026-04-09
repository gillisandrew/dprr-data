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
