import { expect, test, describe } from "vite-plus/test"
import {
  sectionCount,
  advancedActiveCount,
  nameExtrasActive,
  officeOptionsActive,
} from "./filter-panel"
import { parseSearchParams } from "./search-params"

const blank = () => parseSearchParams({})

describe("sectionCount", () => {
  test("all zero on a blank state", () => {
    for (const key of [
      "office",
      "name",
      "status",
      "tribe",
      "location",
      "events",
    ] as const) {
      expect(sectionCount(blank(), key)).toBe(0)
    }
  })

  test("office counts selections plus non-default options", () => {
    const state = {
      ...blank(),
      office: ["consul", "praetor"],
      officeMode: "all" as const,
      officeInRange: true,
    }
    expect(sectionCount(state, "office")).toBe(4)
  })

  test("name counts all six name fields", () => {
    const state = {
      ...blank(),
      praenomen: ["Gaius"],
      nomen: ["Iulius"],
      cognomen: ["Caesar"],
      father: ["Gaius"],
      grandfather: ["Gaius"],
      re: "131",
    }
    expect(sectionCount(state, "name")).toBe(6)
  })

  test("status counts status and sex together", () => {
    const state = { ...blank(), status: ["Patrician"], sex: ["Female"] }
    expect(sectionCount(state, "status")).toBe(2)
  })

  test("tribe, location, events count their facet arrays", () => {
    const state = {
      ...blank(),
      tribe: ["Fabia"],
      province: ["Hispania", "Gallia"],
      event: ["triumph"],
    }
    expect(sectionCount(state, "tribe")).toBe(1)
    expect(sectionCount(state, "location")).toBe(2)
    expect(sectionCount(state, "events")).toBe(1)
  })
})

describe("advancedActiveCount", () => {
  test("zero on blank, sums tribe + location + events", () => {
    expect(advancedActiveCount(blank())).toBe(0)
    const state = {
      ...blank(),
      tribe: ["Fabia"],
      province: ["Hispania"],
      event: ["triumph", "augur"],
    }
    expect(advancedActiveCount(state)).toBe(4)
  })
})

describe("nameExtrasActive", () => {
  test("false on blank and when only gens/cognomen set", () => {
    expect(nameExtrasActive(blank())).toBe(false)
    expect(
      nameExtrasActive({ ...blank(), nomen: ["Iulius"], cognomen: ["Caesar"] })
    ).toBe(false)
  })

  test.each([
    ["praenomen", { praenomen: ["Gaius"] }],
    ["father", { father: ["Gaius"] }],
    ["grandfather", { grandfather: ["Gaius"] }],
    ["re", { re: "46a" }],
  ])("true when %s is set", (_label, patch) => {
    expect(nameExtrasActive({ ...blank(), ...patch })).toBe(true)
  })

  test("whitespace-only RE number does not count", () => {
    expect(nameExtrasActive({ ...blank(), re: "  " })).toBe(false)
  })
})

describe("officeOptionsActive", () => {
  test("false on defaults, true when either option is non-default", () => {
    expect(officeOptionsActive(blank())).toBe(false)
    expect(officeOptionsActive({ ...blank(), officeMode: "all" })).toBe(true)
    expect(officeOptionsActive({ ...blank(), officeInRange: true })).toBe(true)
  })
})
