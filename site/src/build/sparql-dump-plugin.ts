// site/src/build/sparql-dump-plugin.ts
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { gzipSync } from "node:zlib"
import type { Plugin } from "vite-plus"

/**
 * Writes the whole corpus as one deduplicated, gzipped N-Triples dump at
 * public/dump/dprr.nt.gz. The /sparql page's worker loads it into Oxigraph
 * in the browser, and the file doubles as the public bulk-download URL.
 *
 * Lives outside public/data/ because static-data-plugin wipes that
 * directory on every startup; the dump instead regenerates only when a
 * source TTL file is newer than the existing output (person files
 * duplicate related-person triples, so the store's set semantics shrink
 * ~482k raw triples on load).
 */
export function sparqlDumpPlugin(): Plugin {
  let generated: Promise<void> | null = null

  async function collectTtlFiles(repoRoot: string): Promise<string[]> {
    const files = [join(repoRoot, "ontology.ttl")]
    for (const dir of ["reference", "concordances"]) {
      for (const f of await readdir(join(repoRoot, dir))) {
        if (f.endsWith(".ttl")) files.push(join(repoRoot, dir, f))
      }
    }
    const personsDir = join(repoRoot, "persons")
    for (const gens of await readdir(personsDir)) {
      for (const f of await readdir(join(personsDir, gens))) {
        if (f.endsWith(".ttl")) files.push(join(personsDir, gens, f))
      }
    }
    return files
  }

  async function generate(root: string): Promise<void> {
    const repoRoot = join(root, "..")
    const outPath = join(root, "public", "dump", "dprr.nt.gz")
    const files = await collectTtlFiles(repoRoot)

    const [outStat, ...srcStats] = await Promise.allSettled([
      stat(outPath),
      ...files.map((f) => stat(f)),
    ])
    if (outStat.status === "fulfilled") {
      const newestSrc = Math.max(
        ...srcStats.map((s) =>
          s.status === "fulfilled" ? s.value.mtimeMs : Infinity
        )
      )
      if (outStat.value.mtimeMs > newestSrc) return
    }

    const { Store, defaultGraph } = await import("oxigraph")
    const store = new Store()
    for (const file of files) {
      store.load(await readFile(file, "utf-8"), { format: "text/turtle" })
    }
    // All triples live in the default graph; naming it selects a
    // triples-format dump (dataset formats like N-Quads would otherwise be
    // required).
    const nt = store.dump({
      format: "application/n-triples",
      from_graph_name: defaultGraph(),
    })
    const gz = gzipSync(nt, { level: 9 })
    await mkdir(join(root, "public", "dump"), { recursive: true })
    await writeFile(outPath, gz)
    console.log(
      `[sparql-dump] wrote public/dump/dprr.nt.gz ` +
        `(${store.size} triples, ${(gz.byteLength / 1024 / 1024).toFixed(1)}MB gzipped)`
    )
  }

  let root = process.cwd()
  return {
    name: "dprr-sparql-dump",
    configResolved(config) {
      root = config.root
    },
    buildStart() {
      generated ??= generate(root)
      return generated
    },
  }
}
