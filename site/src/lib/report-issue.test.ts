import { expect, test, describe } from "vite-plus/test"
import {
  reportIssueUrl,
  personTtlPath,
  gensTtlPath,
  REFERENCE_TTL,
} from "./report-issue"

describe("reportIssueUrl", () => {
  test("targets the data-issue form with prefilled fields", () => {
    const url = new URL(
      reportIssueUrl({
        entityLabel: "CORN0017 — C. Cornelius (17) Cinna",
        ttlPath: "persons/CORN/CORN0017.ttl",
      })
    )
    expect(url.origin + url.pathname).toBe(
      "https://github.com/gillisandrew/dprr-data/issues/new"
    )
    expect(url.searchParams.get("template")).toBe("data-issue.yml")
    expect(url.searchParams.get("title")).toBe(
      "[data] CORN0017 — C. Cornelius (17) Cinna"
    )
    expect(url.searchParams.get("entity")).toBe(
      "CORN0017 — C. Cornelius (17) Cinna"
    )
    expect(url.searchParams.get("file")).toBe("persons/CORN/CORN0017.ttl")
  })

  test("encodes reserved characters in labels", () => {
    const url = new URL(
      reportIssueUrl({
        entityLabel: "Macedonia/Achaea & friends?",
        ttlPath: "reference/provinces.ttl",
      })
    )
    expect(url.searchParams.get("entity")).toBe("Macedonia/Achaea & friends?")
    expect(url.searchParams.get("file")).toBe("reference/provinces.ttl")
  })
})

describe("ttl paths", () => {
  test("person files shard by the first four characters of the id", () => {
    expect(personTtlPath("CORN0017")).toBe("persons/CORN/CORN0017.ttl")
    expect(personTtlPath("IUNI0001")).toBe("persons/IUNI/IUNI0001.ttl")
  })

  test("gens links point at the shard directory of its members", () => {
    expect(gensTtlPath("CORN0017")).toBe("persons/CORN/")
  })

  test("reference entities share their vocabulary file", () => {
    expect(REFERENCE_TTL.office).toBe("reference/offices.ttl")
    expect(REFERENCE_TTL.province).toBe("reference/provinces.ttl")
    expect(REFERENCE_TTL.tribe).toBe("reference/tribes.ttl")
  })
})
