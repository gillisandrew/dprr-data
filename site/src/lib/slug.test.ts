import { expect, test, describe } from "vite-plus/test"
import { slugify } from "./slug"

describe("slugify", () => {
  test("lowercases and hyphenates", () => {
    expect(slugify("triumvir capitalis")).toBe("triumvir-capitalis")
  })
  test("strips punctuation and collapses runs", () => {
    expect(slugify("quaestio (de veneficiis)")).toBe("quaestio-de-veneficiis")
  })
  test("trims leading/trailing separators", () => {
    expect(slugify("  consul  ")).toBe("consul")
  })
})
