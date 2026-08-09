import { expect, test, describe } from "vite-plus/test"
import { gensDisplayName } from "./gens-name"

describe("gensDisplayName", () => {
  test("feminizes -ius nomina", () => {
    expect(gensDisplayName("Cornelius")).toBe("Cornelia")
    expect(gensDisplayName("Iulius")).toBe("Iulia")
    expect(gensDisplayName("Ancharius")).toBe("Ancharia")
  })

  test("feminizes other -us nomina", () => {
    expect(gensDisplayName("Norbanus")).toBe("Norbana")
  })

  test("preserves uncertain-attribution punctuation around the name", () => {
    expect(gensDisplayName("(Cornelius)")).toBe("(Cornelia)")
    expect(gensDisplayName("(Cornelius?)")).toBe("(Cornelia?)")
  })

  test("leaves nomina without a masculine suffix unchanged", () => {
    expect(gensDisplayName("Maecenas")).toBe("Maecenas")
    expect(gensDisplayName("Cinna")).toBe("Cinna")
  })
})
