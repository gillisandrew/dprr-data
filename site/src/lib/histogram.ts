// site/src/lib/histogram.ts

export interface Histogram {
  /** First bin's starting year (signed, multiple of binSize). */
  start: number
  binSize: number
  counts: number[]
}

const DEFAULT_BIN_SIZE = 5
/** Last year covered by its own bin; later years fold into the final bin. */
const AXIS_END = -31

function representativeYear(
  range: readonly [number | null, number | null]
): number | null {
  const year = range[0] ?? range[1]
  if (year === null) return null
  return year === 0 ? -1 : year
}

/**
 * Bin dated ranges by representative year (start ?? end). Undated dropped.
 * Year 0 clamps to -1. Years after -31 fold into the final bin.
 */
export function buildHistogram(
  ranges: ReadonlyArray<readonly [number | null, number | null]>,
  binSize: number = DEFAULT_BIN_SIZE
): Histogram {
  const years = ranges
    .map(representativeYear)
    .filter((y): y is number => y !== null)

  if (years.length === 0) {
    return { start: AXIS_END - binSize + 1, binSize, counts: [0] }
  }

  const minYear = Math.min(...years, AXIS_END)
  const start = Math.floor(minYear / binSize) * binSize
  const binCount = Math.max(1, Math.ceil((AXIS_END + 1 - start) / binSize))
  const counts = Array.from<number>({ length: binCount }).fill(0)
  const h = { start, binSize, counts }
  for (const y of years) {
    counts[binIndexFor(y, h)] += 1
  }
  return h
}

/** Year → bin index for an existing histogram (same clamp rules). */
export function binIndexFor(year: number, h: Histogram): number {
  const y = year === 0 ? -1 : year
  const idx = Math.floor((y - h.start) / h.binSize)
  return Math.min(Math.max(idx, 0), h.counts.length - 1)
}
