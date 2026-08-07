import { expect, test, describe } from "vite-plus/test"
import { buildHistogram, binIndexFor } from "./histogram"

describe("buildHistogram", () => {
  test("bins representative years into 5-year bins", () => {
    const h = buildHistogram([
      [-509, -509],
      [-508, null],
      [null, -507],
      [-31, -31],
    ])
    expect(h.binSize).toBe(5)
    expect(h.start).toBe(-510)
    // bins: [-510..-506), [-505..-501), ... final bin contains -31
    expect(h.counts[0]).toBe(3)
    expect(h.counts[h.counts.length - 1]).toBe(1)
    expect(h.counts.reduce((a, b) => a + b, 0)).toBe(4)
  })

  test("drops undated, clamps year 0 to 1 BC, folds AD into the final bin", () => {
    const h = buildHistogram([
      [null, null],
      [-100, -99],
      [0, null],
      [14, null],
    ])
    expect(h.counts.reduce((a, b) => a + b, 0)).toBe(3)
    // 0 → -1 and 14 (AD) both land in the final bin
    expect(h.counts[h.counts.length - 1]).toBe(2)
  })

  test("binIndexFor matches the builder's placement", () => {
    const h = buildHistogram([
      [-509, null],
      [-31, null],
    ])
    expect(binIndexFor(-509, h)).toBe(0)
    expect(binIndexFor(-31, h)).toBe(h.counts.length - 1)
    expect(binIndexFor(100, h)).toBe(h.counts.length - 1)
    expect(binIndexFor(-9999, h)).toBe(0)
  })

  test("empty input yields a single empty bin", () => {
    const h = buildHistogram([])
    expect(h.counts).toEqual([0])
  })
})
