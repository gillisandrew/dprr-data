// site/src/lib/sparql-worker.ts
//
// Module worker hosting the Oxigraph WASM engine so multi-second dump
// parsing and heavy queries never block the UI thread. Messages in:
//   { type: "init", dumpUrl }   — fetch + gunzip + load the corpus
//   { type: "query", sparql }   — run a query against the loaded store
// Messages out (all payloads structured-clone-safe):
//   { type: "ready", triples }
//   { type: "result", kind: "select", json } | { kind: "graph", turtle }
//     | { kind: "boolean", value }
//   { type: "error", stage: "init" | "query", message }
import init, { Store } from "oxigraph/web.js"
import wasmUrl from "oxigraph/web_bg.wasm?url"

export type WorkerRequest =
  | { type: "init"; dumpUrl: string }
  | { type: "query"; sparql: string }

export type WorkerResponse =
  | { type: "ready"; triples: number }
  | { type: "result"; kind: "select"; json: string }
  | { type: "result"; kind: "graph"; turtle: string }
  | { type: "result"; kind: "boolean"; value: boolean }
  | { type: "error"; stage: "init" | "query"; message: string }

let store: Store | null = null

const GZIP_MAGIC = 0x1f8b

async function fetchDump(dumpUrl: string): Promise<string> {
  const response = await fetch(dumpUrl)
  if (!response.ok || !response.body) {
    throw new Error(`dump fetch failed: HTTP ${response.status}`)
  }
  const buffer = await response.arrayBuffer()
  const view = new DataView(buffer)
  // Servers differ on whether .gz files arrive transport-decoded; sniff the
  // magic number instead of trusting headers.
  const isGzip = buffer.byteLength >= 2 && view.getUint16(0) === GZIP_MAGIC
  if (!isGzip) return new TextDecoder().decode(buffer)
  const decompressed = new Response(
    new Blob([buffer]).stream().pipeThrough(new DecompressionStream("gzip"))
  )
  return decompressed.text()
}

async function handleInit(dumpUrl: string): Promise<WorkerResponse> {
  await init({ module_or_path: wasmUrl })
  const nt = await fetchDump(dumpUrl)
  store = new Store()
  store.load(nt, { format: "application/n-triples" })
  return { type: "ready", triples: store.size }
}

function handleQuery(sparql: string): WorkerResponse {
  if (!store) {
    return { type: "error", stage: "query", message: "store not loaded yet" }
  }
  const form = sparql
    .replace(/#[^\n]*/g, " ")
    .match(/\b(SELECT|CONSTRUCT|DESCRIBE|ASK)\b/i)?.[1]
    ?.toUpperCase()
  if (form === "SELECT") {
    const json = store.query(sparql, {
      results_format: "application/sparql-results+json",
    }) as string
    return { type: "result", kind: "select", json }
  }
  if (form === "CONSTRUCT" || form === "DESCRIBE") {
    const turtle = store.query(sparql, {
      results_format: "text/turtle",
    }) as string
    return { type: "result", kind: "graph", turtle }
  }
  // ASK (and anything the regex missed — let the engine decide validity).
  const value = store.query(sparql)
  if (typeof value === "boolean") {
    return { type: "result", kind: "boolean", value }
  }
  throw new Error("unsupported query form (updates are disabled)")
}

self.onmessage = async (event: MessageEvent<WorkerRequest>) => {
  const request = event.data
  try {
    if (request.type === "init") {
      self.postMessage(await handleInit(request.dumpUrl))
    } else {
      self.postMessage(handleQuery(request.sparql))
    }
  } catch (err) {
    self.postMessage({
      type: "error",
      stage: request.type === "init" ? "init" : "query",
      message: err instanceof Error ? err.message : String(err),
    } satisfies WorkerResponse)
  }
}
