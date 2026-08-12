import { expect, test, describe } from "vite-plus/test"
import {
  buildTree,
  selectedAncestor,
  valuesWithDescendantsInUniverse,
} from "./facet-tree"
import type { FacetValue } from "@/data/types"

// Universe: consul + praetor under "Magisterial Posts"; pontifex under
// "Priesthoods" via the structural intermediate "Major Colleges" (which has
// no facet count of its own).
const parentOf: Record<string, string | null> = {
  consul: "Magisterial Posts",
  praetor: "Magisterial Posts",
  "Magisterial Posts": null,
  pontifex: "Major Colleges",
  "Major Colleges": "Priesthoods",
  Priesthoods: null,
}

const items: FacetValue[] = [
  { value: "consul", count: 678 },
  { value: "praetor", count: 345 },
  { value: "pontifex", count: 80 },
  { value: "Magisterial Posts", count: 12 },
]

describe("buildTree", () => {
  test("counts selectable descendants through structural intermediates", () => {
    const tree = buildTree(items, parentOf)
    const magisterial = tree.find((n) => n.name === "Magisterial Posts")!
    expect(magisterial.count).toBe(12)
    expect(magisterial.selectableDescendants).toBe(2) // consul + praetor

    const priesthoods = tree.find((n) => n.name === "Priesthoods")!
    expect(priesthoods.count).toBeNull() // structural: no facet count
    // pontifex counts; the structural "Major Colleges" itself does not
    expect(priesthoods.selectableDescendants).toBe(1)
    const majorColleges = priesthoods.children.find(
      (n) => n.name === "Major Colleges"
    )!
    expect(majorColleges.count).toBeNull()
    expect(majorColleges.selectableDescendants).toBe(1)
  })

  test("leaves have zero selectable descendants", () => {
    const tree = buildTree(items, parentOf)
    const magisterial = tree.find((n) => n.name === "Magisterial Posts")!
    const consul = magisterial.children.find((n) => n.name === "consul")!
    expect(consul.selectableDescendants).toBe(0)
  })
})

describe("selectedAncestor", () => {
  test("finds the nearest selected ancestor across structural levels", () => {
    expect(selectedAncestor("pontifex", parentOf, ["Priesthoods"])).toBe(
      "Priesthoods"
    )
    expect(selectedAncestor("consul", parentOf, ["Magisterial Posts"])).toBe(
      "Magisterial Posts"
    )
  })

  test("null when no ancestor is selected or for the node itself", () => {
    expect(selectedAncestor("consul", parentOf, ["consul"])).toBeNull()
    expect(selectedAncestor("consul", parentOf, ["Priesthoods"])).toBeNull()
  })
})

describe("valuesWithDescendantsInUniverse", () => {
  test("marks a selected parent only when the universe holds a descendant", () => {
    const result = valuesWithDescendantsInUniverse(
      ["Magisterial Posts", "Priesthoods"],
      parentOf,
      items
    )
    expect(result.has("Magisterial Posts")).toBe(true)
    // pontifex is in the universe → Priesthoods covers a descendant
    expect(result.has("Priesthoods")).toBe(true)
  })

  test("selected value with no descendants in the universe is excluded", () => {
    const result = valuesWithDescendantsInUniverse(
      ["Priesthoods"],
      parentOf,
      items.filter((i) => i.value !== "pontifex")
    )
    expect(result.has("Priesthoods")).toBe(false)
  })

  test("leaf selections never gain a suffix", () => {
    const result = valuesWithDescendantsInUniverse(["consul"], parentOf, items)
    expect(result.has("consul")).toBe(false)
  })
})
