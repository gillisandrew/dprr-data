import { expect, test, describe } from "vite-plus/test"
import { GLOSSARY } from "./glossary"

describe("glossary", () => {
  test("every entry has a label and non-trivial text", () => {
    for (const [id, entry] of Object.entries(GLOSSARY)) {
      expect(entry.label.length, id).toBeGreaterThan(0)
      expect(entry.text.length, id).toBeGreaterThan(40)
    }
  })
})
