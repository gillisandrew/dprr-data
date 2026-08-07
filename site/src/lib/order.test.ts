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
