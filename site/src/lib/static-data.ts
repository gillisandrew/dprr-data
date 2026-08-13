// site/src/lib/static-data.ts
import { createIsomorphicFn } from "@tanstack/react-start"
import type { Person } from "@/data/types"
import type {
  GensDetail,
  GensIndexEntry,
  OfficeDetail,
  OfficeIndexEntry,
  ProvinceDetail,
  ProvinceIndexEntry,
  TribeDetail,
  TribeIndexEntry,
  SourceDetail,
  SourceIndexEntry,
  PraenomenDetail,
  PraenomenIndexEntry,
  RelationshipDetail,
  RelationshipIndexEntry,
} from "@/data/aggregate-references"

/**
 * Route data source for a statically hosted site.
 *
 * The site ships as plain files on GitHub Pages, so there is no backend to
 * answer a server function. Loaders run on the server during prerender and
 * in the browser on every client-side navigation, so they read the JSON
 * artifacts that src/build/static-data-plugin.ts writes into public/data/:
 * over the network in the browser, straight off disk during prerender.
 * Both branches parse identical bytes, which keeps hydration consistent.
 *
 * createIsomorphicFn is compiled away per environment, so node:fs never
 * reaches the client bundle.
 */
/** Thrown by readStaticJson when the requested data file is missing/unreachable. */
export class StaticDataError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.status = status
  }
}

const readStaticJson = createIsomorphicFn()
  .server(async (path: string): Promise<unknown> => {
    const [{ readFile }, { join }] = await Promise.all([
      import("node:fs/promises"),
      import("node:path"),
    ])
    // Only ever runs under the prerenderer, whose cwd is the site package.
    const file = join(process.cwd(), "public", "data", path)
    try {
      return JSON.parse(await readFile(file, "utf-8")) as unknown
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        throw new StaticDataError(`Failed to load data/${path}: 404`, 404)
      }
      throw err
    }
  })
  .client(async (path: string): Promise<unknown> => {
    // Encode each path segment (not the slashes) — some DPRR ids/slugs
    // contain URL-unsafe characters (e.g. "PL[A3544").
    const encodedPath = path.split("/").map(encodeURIComponent).join("/")
    const res = await fetch(`${import.meta.env.BASE_URL}data/${encodedPath}`)
    if (!res.ok) {
      throw new StaticDataError(
        `Failed to load data/${path}: ${res.status}`,
        res.status
      )
    }
    return res.json() as unknown
  })

async function readJson<T>(path: string): Promise<T> {
  return (await readStaticJson(path)) as T
}

export function fetchPerson(id: string): Promise<Person> {
  return readJson<Person>(`persons/${id}.json`)
}

export function fetchPersonIds(): Promise<string[]> {
  return readJson<string[]>("person-ids.json")
}

export function fetchOfficeIndex(): Promise<OfficeIndexEntry[]> {
  return readJson<OfficeIndexEntry[]>("offices.json")
}

export function fetchOfficeDetail(slug: string): Promise<OfficeDetail> {
  return readJson<OfficeDetail>(`offices/${slug}.json`)
}

export function fetchTribeIndex(): Promise<TribeIndexEntry[]> {
  return readJson<TribeIndexEntry[]>("tribes.json")
}

export function fetchTribeDetail(slug: string): Promise<TribeDetail> {
  return readJson<TribeDetail>(`tribes/${slug}.json`)
}

export function fetchGensIndex(): Promise<GensIndexEntry[]> {
  return readJson<GensIndexEntry[]>("gentes.json")
}

export function fetchGensDetail(slug: string): Promise<GensDetail> {
  return readJson<GensDetail>(`gentes/${slug}.json`)
}

export function fetchProvinceIndex(): Promise<ProvinceIndexEntry[]> {
  return readJson<ProvinceIndexEntry[]>("provinces.json")
}

export function fetchProvinceDetail(slug: string): Promise<ProvinceDetail> {
  return readJson<ProvinceDetail>(`provinces/${slug}.json`)
}

export function fetchSourceIndex(): Promise<SourceIndexEntry[]> {
  return readJson<SourceIndexEntry[]>("sources.json")
}

export function fetchSourceDetail(slug: string): Promise<SourceDetail> {
  return readJson<SourceDetail>(`sources/${slug}.json`)
}

export function fetchPraenomenIndex(): Promise<PraenomenIndexEntry[]> {
  return readJson<PraenomenIndexEntry[]>("praenomina.json")
}

export function fetchPraenomenDetail(slug: string): Promise<PraenomenDetail> {
  return readJson<PraenomenDetail>(`praenomina/${slug}.json`)
}

export function fetchRelationshipIndex(): Promise<RelationshipIndexEntry[]> {
  return readJson<RelationshipIndexEntry[]>("relationships.json")
}

export function fetchRelationshipDetail(
  slug: string
): Promise<RelationshipDetail> {
  return readJson<RelationshipDetail>(`relationships/${slug}.json`)
}
