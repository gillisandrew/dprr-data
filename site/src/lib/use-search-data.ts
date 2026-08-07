// site/src/lib/use-search-data.ts
import { useEffect, useState } from "react"
import MiniSearch from "minisearch"
import type { PersonSummary } from "@/data/types"
import type { SearchPayload, SearchIndexPayload } from "@/data/search-payload"

export interface SearchDataBundle {
  payload: SearchPayload
  miniSearch: MiniSearch<PersonSummary>
}

let cache: Promise<SearchDataBundle> | null = null

function load(): Promise<SearchDataBundle> {
  cache ??= (async () => {
    const base = import.meta.env.BASE_URL
    const [payloadRes, indexRes] = await Promise.all([
      fetch(`${base}data/search-data.json`),
      fetch(`${base}data/search-index.json`),
    ])
    if (!payloadRes.ok || !indexRes.ok) {
      throw new Error(
        `search data fetch failed: ${payloadRes.status}/${indexRes.status}`
      )
    }
    const payload = (await payloadRes.json()) as SearchPayload
    const indexPayload = (await indexRes.json()) as SearchIndexPayload
    const miniSearch = MiniSearch.loadJSON<PersonSummary>(
      JSON.stringify(indexPayload.index),
      indexPayload.options
    )
    return { payload, miniSearch }
  })()
  return cache
}

export function useSearchData(enabled: boolean): {
  bundle: SearchDataBundle | null
  error: string | null
} {
  const [bundle, setBundle] = useState<SearchDataBundle | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!enabled) return
    let alive = true
    load().then(
      (b) => alive && setBundle(b),
      (e: unknown) => {
        if (alive) {
          setError(e instanceof Error ? e.message : String(e))
          cache = null
        }
      }
    )
    return () => {
      alive = false
    }
  }, [enabled])

  return { bundle, error }
}
