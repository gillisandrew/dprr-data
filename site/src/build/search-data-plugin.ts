// site/src/build/search-data-plugin.ts
import { mkdir, writeFile } from "node:fs/promises"
import { join } from "node:path"
// If vite-plus doesn't re-export the Plugin type, fall back to a structural
// type: `type Plugin = { name: string; configResolved?: (c: { root: string }) => void; buildStart?: () => Promise<void> | void }`
import type { Plugin } from "vite-plus"

/**
 * Generates the static search-data JSON assets into public/data/ so both
 * the dev server and the production build serve identical files. Runs the
 * TTL pipeline once per vite startup (buildStart), guarded so parallel
 * environments (client/ssr) don't regenerate concurrently.
 */
export function searchDataPlugin(): Plugin {
  let generated: Promise<void> | null = null

  async function generate(root: string): Promise<void> {
    const { loadAllData } = await import("../data/loader")
    const { buildSearchPayload, buildSearchIndexPayload } =
      await import("../data/search-payload")
    const { persons, refs } = await loadAllData()
    const payload = buildSearchPayload(persons, refs)
    const indexPayload = buildSearchIndexPayload(payload.summaries)
    const outDir = join(root, "public", "data")
    await mkdir(outDir, { recursive: true })
    await writeFile(join(outDir, "search-data.json"), JSON.stringify(payload))
    await writeFile(
      join(outDir, "search-index.json"),
      JSON.stringify(indexPayload)
    )
    console.log(
      `[search-data] wrote public/data/*.json (${payload.summaries.length} summaries)`
    )
  }

  let root = process.cwd()
  return {
    name: "dprr-search-data",
    configResolved(config) {
      root = config.root
    },
    buildStart() {
      generated ??= generate(root)
      return generated
    },
  }
}
