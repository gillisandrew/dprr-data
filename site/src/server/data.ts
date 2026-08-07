import { createServerFn } from "@tanstack/react-start"
import { loadAllData } from "../data/loader"
import {
  buildOfficeIndex,
  buildOfficeDetail,
  buildTribeIndex,
  buildTribeDetail,
  buildProvinceIndex,
  buildProvinceDetail,
  buildNameHierarchy,
} from "../data/aggregate-references"

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
