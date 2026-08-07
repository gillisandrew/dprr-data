import { expect, test, describe } from "vite-plus/test"
import { eraKey, compareByName, sortResults, UNDATED } from "./order"

const p = (
  id: string,
  name: string,
  eraFrom: number | null,
  eraTo: number | null
) => ({ id, name, eraFrom, eraTo }) as never

describe("ordering", () => {
  test("eraKey uses eraFrom, falls back to eraTo, undated last", () => {
    expect(eraKey({ eraFrom: -509, eraTo: -31 })).toBe(-509)
    expect(eraKey({ eraFrom: null, eraTo: -100 })).toBe(-100)
    expect(eraKey({ eraFrom: null, eraTo: null })).toBe(UNDATED)
  })

  test("compareByName strips the DPRR ID prefix", () => {
    expect(
      compareByName(
        { name: "IUNI0001 L. Iunius" },
        { name: "AEMI0002 M. Aemilius" }
      )
    ).toBeGreaterThan(0)
  })

  test("compareByName normalizes uncertain praenomen markers, apostrophes, empty praenomen, and hyphenated IDs", () => {
    const list = [
      { name: "ACIL5133 -. Acilius (2)" }, // unknown-praenomen marker "-."
      { name: "ACIL0968 M'. Acilius (10)" }, // apostrophe praenomen (Manius)
      { name: "AEBU5164  Aebutius (1)" }, // empty praenomen, leading space
      { name: "AN-3071 -. An- (not in RE)" }, // hyphenated short ID
    ]
    const sorted = [...list].sort(compareByName)
    expect(sorted.map((x) => x.name)).toEqual([
      "ACIL0968 M'. Acilius (10)",
      "ACIL5133 -. Acilius (2)",
      "AEBU5164  Aebutius (1)",
      "AN-3071 -. An- (not in RE)",
    ])
  })

  test("sortResults: earliest default, latest, name, relevance passthrough", () => {
    const list = [
      p("B", "BBBB0001 B", -100, -90),
      p("A", "AAAA0001 A", -200, -150),
      p("U", "UUUU0001 U", null, null),
    ]
    expect(
      sortResults(list, null, false).map((x: { id: string }) => x.id)
    ).toEqual(["A", "B", "U"])
    expect(
      sortResults(list, "latest", false).map((x: { id: string }) => x.id)
    ).toEqual(["B", "A", "U"])
    expect(sortResults(list, "name", false)[0].id).toBe("A")
    expect(sortResults(list, null, true)).toEqual(list) // relevance = passthrough
    expect(sortResults(list, "earliest", true)[0].id).toBe("A")
  })
})
