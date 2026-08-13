import { expect, test, describe } from "vite-plus/test"
import { hasRenderableSources } from "./source-hint"

describe("hasRenderableSources", () => {
  test("false when there are no sources at all", () => {
    expect(hasRenderableSources([])).toBe(false)
  })

  test("false when every entry would render nothing", () => {
    // SourceCitation renders null for an empty name, so this set would
    // produce a book icon opening onto an empty popover.
    expect(
      hasRenderableSources([
        { secondarySource: "", notes: null },
        { secondarySource: "", notes: null },
      ])
    ).toBe(false)
  })

  test("true when any entry names a source", () => {
    expect(
      hasRenderableSources([
        { secondarySource: "", notes: null },
        { secondarySource: "Broughton MRR", notes: null },
      ])
    ).toBe(true)
  })

  test("true when an entry has only a note", () => {
    expect(
      hasRenderableSources([{ secondarySource: "", notes: "see Appendix 3" }])
    ).toBe(true)
  })

  test("treats whitespace-only values as empty", () => {
    expect(
      hasRenderableSources([{ secondarySource: "  ", notes: "\n " }])
    ).toBe(false)
  })
})
