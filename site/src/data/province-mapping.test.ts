// site/src/data/province-mapping.test.ts
import { readFileSync } from "node:fs"
import { join } from "node:path"
import { expect, test, describe } from "vite-plus/test"
import {
  PROVINCE_MAPPING,
  mapProvinceText,
  collectUnmappedProvinces,
} from "./province-mapping"

function canonicalNames(): Set<string> {
  const ttl = readFileSync(
    join(process.cwd(), "..", "reference", "provinces.ttl"),
    "utf-8"
  )
  const names = new Set<string>()
  for (const m of ttl.matchAll(/dprr:hasName "([^"]+)"/g)) {
    names.add(m[1])
  }
  return names
}

describe("province mapping", () => {
  test("every mapping target is a canonical province name", () => {
    const canonical = canonicalNames()
    for (const [raw, targets] of Object.entries(PROVINCE_MAPPING)) {
      expect(targets.length, `"${raw}" maps to nothing`).toBeGreaterThan(0)
      for (const t of targets) {
        expect(canonical.has(t), `"${raw}" → "${t}" not canonical`).toBe(true)
      }
    }
  })

  test("known variants resolve", () => {
    expect(mapProvinceText("Sicily")).toEqual(["Sicilia"])
    expect(mapProvinceText("Sicilia")).toEqual(["Sicilia"])
    // "Greece" has no canonical entry of its own; Achaea is the canonical
    // province covering Greece proper.
    expect(mapProvinceText("Greece and Asia")).toEqual(["Achaea", "Asia"])
  })

  test("unmapped strings return null and are collected", () => {
    expect(mapProvinceText("definitely-not-a-province")).toBeNull()
    expect(
      collectUnmappedProvinces(["Sicilia", "definitely-not-a-province"])
    ).toEqual(["definitely-not-a-province"])
  })

  test("high-frequency strings are covered", () => {
    for (const raw of [
      "Rome",
      "Asia",
      "Macedonia",
      "Sicilia",
      "Africa",
      "Syria",
      "Cilicia",
      "Hispania Ulterior",
      "Hispania Citerior",
    ]) {
      expect(mapProvinceText(raw), `"${raw}" should be mapped`).not.toBeNull()
    }
  })
})
