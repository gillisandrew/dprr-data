// site/src/build/static-data-plugin.ts
import { mkdir, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
// If vite-plus doesn't re-export the Plugin type, fall back to a structural
// type: `type Plugin = { name: string; configResolved?: (c: { root: string }) => void; buildStart?: () => Promise<void> | void }`
import type { Plugin } from "vite-plus"

/**
 * Generates every JSON asset the app reads at runtime into public/data/ so
 * both the dev server and the production build serve identical files. The
 * site deploys as pure static files, so route loaders cannot call server
 * functions -- they read these artifacts instead (see src/lib/static-data.ts).
 *
 * Runs the TTL pipeline once per vite startup (buildStart), guarded so
 * parallel environments (client/ssr) don't regenerate concurrently.
 */
export function staticDataPlugin(): Plugin {
  let generated: Promise<void> | null = null

  async function generate(root: string): Promise<void> {
    const { loadAllData } = await import("../data/loader")
    const { buildSearchPayload, buildSearchIndexPayload } =
      await import("../data/search-payload")
    const {
      buildOfficeIndex,
      buildOfficeDetail,
      buildTribeIndex,
      buildTribeDetail,
      buildGensIndex,
      buildGensDetail,
      buildProvinceIndex,
      buildProvinceDetail,
      buildNameHierarchy,
    } = await import("../data/aggregate-references")

    const { persons, refs } = await loadAllData()
    const outDir = join(root, "public", "data")

    // Wipe first so slugs/ids removed from the source data don't linger as
    // stale files that would keep resolving after a rebuild.
    await rm(outDir, { recursive: true, force: true })
    await mkdir(outDir, { recursive: true })

    const write = async (name: string, value: unknown): Promise<void> => {
      await writeFile(join(outDir, name), JSON.stringify(value))
    }

    /** Writes one file per entry into `dir`, keyed by `slug`. */
    const writeDetails = async <T extends { slug: string }>(
      dir: string,
      index: Array<{ slug: string }>,
      build: (slug: string) => T | null
    ): Promise<void> => {
      await mkdir(join(outDir, dir), { recursive: true })
      await Promise.all(
        index.map(async ({ slug }) => {
          const detail = build(slug)
          if (!detail) throw new Error(`${dir}: no detail for slug ${slug}`)
          await write(join(dir, `${slug}.json`), detail)
        })
      )
    }

    // Search (client-side faceted search reads these directly).
    const payload = buildSearchPayload(persons, refs)
    await write("search-data.json", payload)
    await write("search-index.json", buildSearchIndexPayload(payload.summaries))

    // Person detail + the directory's id list.
    await mkdir(join(outDir, "persons"), { recursive: true })
    await write(
      "person-ids.json",
      persons.map((p) => p.id)
    )
    await Promise.all(
      persons.map((p) => write(join("persons", `${p.id}.json`), p))
    )

    // Reference indexes and their per-slug details.
    const offices = buildOfficeIndex(persons, buildNameHierarchy(refs.offices))
    const tribes = buildTribeIndex(persons)
    const gentes = buildGensIndex(persons)
    const provinces = buildProvinceIndex(persons)
    await Promise.all([
      write("offices.json", offices),
      write("tribes.json", tribes),
      write("gentes.json", gentes),
      write("provinces.json", provinces),
      writeDetails("offices", offices, (s) => buildOfficeDetail(persons, s)),
      writeDetails("tribes", tribes, (s) => buildTribeDetail(persons, s)),
      writeDetails("gentes", gentes, (s) => buildGensDetail(persons, s)),
      writeDetails("provinces", provinces, (s) =>
        buildProvinceDetail(persons, s)
      ),
    ])

    console.log(
      `[static-data] wrote public/data/ (${persons.length} persons, ` +
        `${offices.length} offices, ${tribes.length} tribes, ` +
        `${gentes.length} gentes, ${provinces.length} provinces)`
    )
  }

  let root = process.cwd()
  return {
    name: "dprr-static-data",
    configResolved(config) {
      root = config.root
    },
    buildStart() {
      generated ??= generate(root)
      return generated
    },
  }
}
