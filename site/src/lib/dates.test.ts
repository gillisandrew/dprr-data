import { expect, test, describe } from "vite-plus/test"
import { formatYear, formatEraRange } from "./dates"

describe("formatYear", () => {
  test("negative year displays as BC", () => {
    expect(formatYear(-509)).toBe("509 BC")
  })
  test("positive year displays as AD", () => {
    expect(formatYear(14)).toBe("AD 14")
  })
  test("zero displays as 1 BC", () => {
    expect(formatYear(0)).toBe("1 BC")
  })
  test("with uncertainty marker", () => {
    expect(formatYear(-540, true)).toBe("c. 540 BC")
  })
})

describe("formatEraRange", () => {
  test("both BC dates", () => {
    expect(formatEraRange(-540, -509)).toBe("540\u2013509 BC")
  })
  test("null from", () => {
    expect(formatEraRange(null, -509)).toBe("?\u2013509 BC")
  })
  test("null to", () => {
    expect(formatEraRange(-540, null)).toBe("540 BC\u2013?")
  })
  test("both null", () => {
    expect(formatEraRange(null, null)).toBe(null)
  })
  test("cross BC/AD boundary", () => {
    expect(formatEraRange(-63, 14)).toBe("63 BC\u2013AD 14")
  })
})
