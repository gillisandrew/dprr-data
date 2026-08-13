import { expect, test, describe } from "vite-plus/test"
import { citedSources } from "./cited-sources"
import type { Person } from "./types"

function personWith(over: Partial<Person>): Person {
  return {
    postAssertions: [],
    statusAssertions: [],
    relationships: [],
    dateInformation: [],
    personNotes: [],
    tribeAssertions: [],
    ...over,
  } as Person
}

describe("citedSources", () => {
  test("collects sources from all six source-bearing fields", () => {
    const p = personWith({
      postAssertions: [{ secondarySource: "post" }],
      statusAssertions: [{ secondarySource: "status" }],
      relationships: [{ secondarySource: "rel" }],
      dateInformation: [{ secondarySource: "date" }],
      personNotes: [{ secondarySource: "note" }],
      tribeAssertions: [{ secondarySource: "tribe" }],
    } as Partial<Person>)
    expect([...citedSources(p)].sort()).toEqual([
      "date",
      "note",
      "post",
      "rel",
      "status",
      "tribe",
    ])
  })

  test("counts a source once however often it is cited", () => {
    const p = personWith({
      postAssertions: [
        { secondarySource: "Broughton MRR I" },
        { secondarySource: "Broughton MRR I" },
      ],
      personNotes: [{ secondarySource: "Broughton MRR I" }],
    } as Partial<Person>)
    expect([...citedSources(p)]).toEqual(["Broughton MRR I"])
  })

  test("ignores blank source names", () => {
    const p = personWith({
      postAssertions: [{ secondarySource: "" }],
    } as Partial<Person>)
    expect([...citedSources(p)]).toEqual([])
  })
})
