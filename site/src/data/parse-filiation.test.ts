// site/src/data/parse-filiation.test.ts
import { expect, test, describe } from "vite-plus/test"
import { parseFiliation } from "./parse-filiation"

describe("parseFiliation", () => {
  test("expands father and grandfather praenomina", () => {
    expect(parseFiliation("M. f. M. n.")).toEqual({
      father: "Marcus",
      grandfather: "Marcus",
    })
    expect(parseFiliation("Q. f. Ser. n.")).toEqual({
      father: "Quintus",
      grandfather: "Servius",
    })
  })

  test("father only", () => {
    expect(parseFiliation("L. f.")).toEqual({
      father: "Lucius",
      grandfather: null,
    })
  })

  test("unknown slots yield null", () => {
    expect(parseFiliation("- f. - n.")).toEqual({
      father: null,
      grandfather: null,
    })
    expect(parseFiliation(null)).toEqual({ father: null, grandfather: null })
    expect(parseFiliation("")).toEqual({ father: null, grandfather: null })
  })

  test("strips uncertainty markers and parentheses", () => {
    expect(parseFiliation("L.? f. C. n.")).toEqual({
      father: "Lucius",
      grandfather: "Gaius",
    })
    expect(parseFiliation("Sex. f. (Sex. n.)")).toEqual({
      father: "Sextus",
      grandfather: "Sextus",
    })
    expect(parseFiliation("Ser. ? f. - n.")).toEqual({
      father: "Servius",
      grandfather: null,
    })
  })

  test("ambiguous 'or' slots yield null", () => {
    expect(parseFiliation("Q. f. Q. or L.? n.")).toEqual({
      father: "Quintus",
      grandfather: null,
    })
  })

  test("longer abbreviations", () => {
    expect(parseFiliation("Volus. f. Volus. n.")).toEqual({
      father: "Volusus",
      grandfather: "Volusus",
    })
    expect(parseFiliation("Mam. f.")).toEqual({
      father: "Mamercus",
      grandfather: null,
    })
    expect(parseFiliation("M'. f.")).toEqual({
      father: "Manius",
      grandfather: null,
    })
  })
})
