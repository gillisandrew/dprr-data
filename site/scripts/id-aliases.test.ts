import { expect, test, describe } from "vite-plus/test"
import { bareIdAliases, redirectHtml } from "./id-aliases.mjs"

describe("bareIdAliases", () => {
  test("maps a four-digit suffix to its canonical id", () => {
    expect(bareIdAliases(["VALE0522"])).toEqual(new Map([["0522", "VALE0522"]]))
  })

  test("throws when two ids share a four-digit suffix", () => {
    expect(() => bareIdAliases(["VALE0522", "CORN0522"])).toThrow(
      /0522.*VALE0522.*CORN0522/
    )
  })

  test("throws on an id that does not end in four digits", () => {
    expect(() => bareIdAliases(["VALE522"])).toThrow(/VALE522/)
  })

  test("throws when an alias would shadow a canonical id", () => {
    expect(() => bareIdAliases(["0522", "VALE1234"])).toThrow(/0522/)
  })

  // Passed on first run: documents the real prefix shapes rather than
  // driving new behaviour. Guards against tightening the pattern to
  // /^[A-Z]{4}\d{4}$/, which would drop 30-odd persons.
  test("handles non-alphabetic prefixes present in the data", () => {
    expect(bareIdAliases(["PL[A3544", "TE-3084", "P-3093"])).toEqual(
      new Map([
        ["3544", "PL[A3544"],
        ["3084", "TE-3084"],
        ["3093", "P-3093"],
      ])
    )
  })
})

describe("redirectHtml", () => {
  test("points canonical and refresh at the base-prefixed person page", () => {
    const html = redirectHtml("VALE0522", "/dprr-data")
    expect(html).toContain(
      '<link rel="canonical" href="/dprr-data/persons/VALE0522">'
    )
    expect(html).toContain(
      '<meta http-equiv="refresh" content="0; url=/dprr-data/persons/VALE0522">'
    )
  })

  test("keeps a working link for readers whose refresh is blocked", () => {
    expect(redirectHtml("VALE0522", "/dprr-data")).toContain(
      '<a href="/dprr-data/persons/VALE0522">'
    )
  })
})
