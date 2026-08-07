import { createServerFn } from "@tanstack/react-start"
import { loadAllData, toSummaries } from "../data/loader"
import { buildSearchIndex, MINISEARCH_OPTIONS } from "../data/search-index"
import {
  buildOfficeIndex,
  buildOfficeDetail,
  buildTribeIndex,
  buildTribeDetail,
  buildProvinceIndex,
  buildProvinceDetail,
  buildNameHierarchy,
} from "../data/aggregate-references"
import type { PersonSummary } from "../data/types"

/** Serializable subset of MiniSearch options returned to the client. */
interface SerializableOptions {
  fields: string[]
  storeFields: string[]
  idField: string
  searchOptions: {
    prefix: boolean
    fuzzy: number
    boost: Record<string, number>
  }
}

// Pre-compute serializable options at module load (no function-valued fields).
const serializableOptions: SerializableOptions = {
  fields: MINISEARCH_OPTIONS.fields as string[],
  storeFields: MINISEARCH_OPTIONS.storeFields as string[],
  idField: MINISEARCH_OPTIONS.idField as string,
  searchOptions: {
    prefix: MINISEARCH_OPTIONS.searchOptions?.prefix as boolean,
    fuzzy: MINISEARCH_OPTIONS.searchOptions?.fuzzy as number,
    boost: MINISEARCH_OPTIONS.searchOptions?.boost as Record<string, number>,
  },
}

export const getPersonById = createServerFn({ method: "GET" })
  .inputValidator((id: string) => id)
  .handler(async ({ data: id }) => {
    const { persons } = await loadAllData()
    const person = persons.find((p) => p.id === id)
    if (!person) {
      throw new Error(`Person not found: ${id}`)
    }
    return person
  })

export const getAllPersonIds = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return persons.map((p) => p.id)
  }
)

export const getSearchData = createServerFn({ method: "GET" }).handler(
  async (): Promise<{
    summaries: PersonSummary[]
    searchIndex: object
    options: SerializableOptions
    officeHierarchy: Record<string, string | null>
    provinceHierarchy: Record<string, string | null>
  }> => {
    const { persons, refs } = await loadAllData()
    const summaries = toSummaries(persons)
    const searchIndex = buildSearchIndex(summaries)
    return {
      summaries,
      searchIndex,
      options: serializableOptions,
      officeHierarchy: buildNameHierarchy(refs.offices),
      provinceHierarchy: buildNameHierarchy(refs.provinces),
    }
  }
)

export const getOfficeIndex = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons, refs } = await loadAllData()
    return buildOfficeIndex(persons, buildNameHierarchy(refs.offices))
  }
)

export const getOfficeDetail = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const { persons } = await loadAllData()
    const detail = buildOfficeDetail(persons, slug)
    if (!detail) throw new Error(`Office not found: ${slug}`)
    return detail
  })

export const getTribeIndex = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return buildTribeIndex(persons)
  }
)

export const getTribeDetail = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const { persons } = await loadAllData()
    const detail = buildTribeDetail(persons, slug)
    if (!detail) throw new Error(`Tribe not found: ${slug}`)
    return detail
  })

export const getProvinceIndex = createServerFn({ method: "GET" }).handler(
  async () => {
    const { persons } = await loadAllData()
    return buildProvinceIndex(persons)
  }
)

export const getProvinceDetail = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const { persons } = await loadAllData()
    const detail = buildProvinceDetail(persons, slug)
    if (!detail) throw new Error(`Province not found: ${slug}`)
    return detail
  })
