import { expect, test, describe } from "vite-plus/test"
import {
  formatYear,
  formatEraRange,
  toSignedYear,
  fromSignedYear,
  formatYearWithInterval,
} from "./dates"

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

describe("signed year conversion", () => {
  test("BC years negate, AD years pass through", () => {
    expect(toSignedYear(509, "BC")).toBe(-509)
    expect(toSignedYear(14, "AD")).toBe(14)
  })
  test("year zero policy: 0 = 1 BC, inputs below 1 clamp", () => {
    expect(fromSignedYear(0)).toEqual({ year: 1, era: "BC" })
    expect(toSignedYear(0, "BC")).toBe(-1)
    expect(toSignedYear(-5, "AD")).toBe(1)
  })
  test("round-trips", () => {
    expect(
      toSignedYear(fromSignedYear(-509).year, fromSignedYear(-509).era)
    ).toBe(-509)
    expect(fromSignedYear(14)).toEqual({ year: 14, era: "AD" })
  })
})

describe("formatYearWithInterval", () => {
  test("B prefixes before, A prefixes after", () => {
    expect(formatYearWithInterval(-216, "B")).toBe("before 216 BC")
    expect(formatYearWithInterval(-216, "A")).toBe("after 216 BC")
  })

  test("S and null defer to formatYear exactly", () => {
    expect(formatYearWithInterval(-216, "S")).toBe(formatYear(-216))
    expect(formatYearWithInterval(-216, null)).toBe(formatYear(-216))
  })

  test("uncertainty composes with the interval", () => {
    expect(formatYearWithInterval(-216, "B", true)).toBe("before c. 216 BC")
    expect(formatYearWithInterval(14, "A", true)).toBe("after c. AD 14")
  })
})
