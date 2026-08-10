// site/src/routes/sparql.tsx
import { useEffect, useRef, useState } from "react"
import { createFileRoute, Link } from "@tanstack/react-router"
import { EditorView, keymap, lineNumbers } from "@codemirror/view"
import { EditorState } from "@codemirror/state"
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands"
import {
  StreamLanguage,
  syntaxHighlighting,
  defaultHighlightStyle,
} from "@codemirror/language"
import { sparql as sparqlMode } from "@codemirror/legacy-modes/mode/sparql"
import { fetchPersonIds } from "@/lib/static-data"
import {
  DEFAULT_QUERY,
  EXAMPLE_QUERIES,
  buildPersonNumberMap,
  parseSelectResults,
  personRouteForIri,
  shortenIri,
  toCsv,
  type ResultTerm,
  type SelectResults,
} from "@/lib/sparql"
import type { WorkerRequest, WorkerResponse } from "@/lib/sparql-worker"

export const Route = createFileRoute("/sparql")({
  head: () => {
    const title = "SPARQL — DPRR"
    const desc =
      "Query the full DPRR dataset with SPARQL 1.1, running entirely in your browser"
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: SparqlPage,
})

type EngineState =
  | { status: "loading" }
  | { status: "ready"; triples: number }
  | { status: "error"; message: string }

type QueryResult =
  | { kind: "select"; results: SelectResults }
  | { kind: "graph"; turtle: string }
  | { kind: "boolean"; value: boolean }
  | { kind: "error"; message: string }

const MAX_ROWS = 2000

function SparqlPage() {
  const editorRef = useRef<HTMLDivElement>(null)
  const viewRef = useRef<EditorView | null>(null)
  const workerRef = useRef<Worker | null>(null)
  const runRef = useRef<() => void>(() => {})

  const [engine, setEngine] = useState<EngineState>({ status: "loading" })
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<QueryResult | null>(null)
  const [personMap, setPersonMap] = useState<Map<number, string>>(new Map())

  useEffect(() => {
    const worker = new Worker(
      new URL("../lib/sparql-worker.ts", import.meta.url),
      { type: "module" }
    )
    workerRef.current = worker
    worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const msg = event.data
      if (msg.type === "ready") {
        setEngine({ status: "ready", triples: msg.triples })
      } else if (msg.type === "error" && msg.stage === "init") {
        setEngine({ status: "error", message: msg.message })
      } else if (msg.type === "error") {
        setRunning(false)
        setResult({ kind: "error", message: msg.message })
      } else if (msg.type === "result") {
        setRunning(false)
        setResult(
          msg.kind === "select"
            ? { kind: "select", results: parseSelectResults(msg.json) }
            : msg.kind === "graph"
              ? { kind: "graph", turtle: msg.turtle }
              : { kind: "boolean", value: msg.value }
        )
      }
    }
    worker.postMessage({
      type: "init",
      dumpUrl: `${import.meta.env.BASE_URL}dump/dprr.nt.gz`,
    } satisfies WorkerRequest)

    fetchPersonIds()
      .then((ids) => setPersonMap(buildPersonNumberMap(ids)))
      .catch(() => {}) // links degrade to external URIs

    return () => {
      worker.terminate()
      workerRef.current = null
    }
  }, [])

  useEffect(() => {
    if (!editorRef.current || viewRef.current) return
    const view = new EditorView({
      parent: editorRef.current,
      state: EditorState.create({
        doc: DEFAULT_QUERY,
        extensions: [
          lineNumbers(),
          history(),
          StreamLanguage.define(sparqlMode),
          syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
          keymap.of([
            {
              key: "Mod-Enter",
              run: () => {
                runRef.current()
                return true
              },
            },
            ...defaultKeymap,
            ...historyKeymap,
          ]),
          EditorView.theme({
            "&": { fontSize: "0.8125rem", backgroundColor: "transparent" },
            "&.cm-focused": { outline: "none" },
            ".cm-content": { fontFamily: "var(--font-mono, monospace)" },
            ".cm-gutters": {
              backgroundColor: "transparent",
              border: "none",
              opacity: "0.45",
            },
          }),
        ],
      }),
    })
    viewRef.current = view
    return () => {
      view.destroy()
      viewRef.current = null
    }
  }, [])

  const run = () => {
    const view = viewRef.current
    const worker = workerRef.current
    if (!view || !worker || engine.status !== "ready" || running) return
    setRunning(true)
    setResult(null)
    worker.postMessage({
      type: "query",
      sparql: view.state.doc.toString(),
    } satisfies WorkerRequest)
  }
  runRef.current = run

  const loadExample = (query: string) => {
    const view = viewRef.current
    if (!view) return
    view.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: query },
    })
    view.focus()
  }

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">SPARQL</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Query the full dataset with SPARQL 1.1 — the engine (
          <a
            href="https://github.com/oxigraph/oxigraph"
            target="_blank"
            rel="noreferrer"
            className="text-accent-ink hover:underline"
          >
            Oxigraph
          </a>
          , compiled to WebAssembly) and the data run entirely in your browser.
          You can also{" "}
          <a
            href={`${import.meta.env.BASE_URL}dump/dprr.nt.gz`}
            className="text-accent-ink hover:underline"
          >
            download the N-Triples dump
          </a>{" "}
          to query locally.
        </p>
      </header>

      <div className="mt-4 flex flex-wrap items-center gap-3 text-sm">
        <label className="text-muted-foreground" htmlFor="example-query">
          Examples:
        </label>
        <select
          id="example-query"
          className="rounded border border-rule-hair bg-background px-2 py-1 text-sm"
          defaultValue={EXAMPLE_QUERIES[0].label}
          onChange={(e) => {
            const example = EXAMPLE_QUERIES.find(
              (q) => q.label === e.target.value
            )
            if (example) loadExample(example.query)
          }}
        >
          {EXAMPLE_QUERIES.map((q) => (
            <option key={q.label} value={q.label}>
              {q.label}
            </option>
          ))}
        </select>
        <span className="ml-auto text-xs text-muted-foreground">
          {engine.status === "loading" && "Loading dataset…"}
          {engine.status === "ready" &&
            `${engine.triples.toLocaleString()} triples loaded`}
          {engine.status === "error" && (
            <span className="text-destructive">
              Failed to load dataset: {engine.message}
            </span>
          )}
        </span>
      </div>

      <div
        ref={editorRef}
        className="mt-3 min-h-40 rounded border border-rule-hair bg-card"
      />

      <div className="mt-3 flex items-center gap-3">
        <button
          type="button"
          onClick={run}
          disabled={engine.status !== "ready" || running}
          className="rounded bg-foreground px-4 py-1.5 text-sm font-medium text-background disabled:opacity-40"
        >
          {running ? "Running…" : "Run query"}
        </button>
        <span className="text-xs text-muted-foreground">⌘⏎ / Ctrl+Enter</span>
      </div>

      <div className="mt-6">
        {result?.kind === "error" && (
          <p className="rounded bg-muted/50 p-3 font-mono text-sm whitespace-pre-wrap text-destructive">
            {result.message}
          </p>
        )}
        {result?.kind === "boolean" && (
          <p className="text-lg font-medium">
            {result.value ? "Yes" : "No"}
            <span className="ml-2 text-sm font-normal text-muted-foreground">
              (ASK result)
            </span>
          </p>
        )}
        {result?.kind === "graph" && (
          <pre className="max-h-[32rem] overflow-auto rounded bg-muted/50 p-3 text-xs leading-relaxed">
            {result.turtle || "# empty graph"}
          </pre>
        )}
        {result?.kind === "select" && (
          <SelectTable results={result.results} personMap={personMap} />
        )}
      </div>
    </div>
  )
}

function SelectTable({
  results,
  personMap,
}: {
  results: SelectResults
  personMap: Map<number, string>
}) {
  const shown = results.rows.slice(0, MAX_ROWS)
  const downloadCsv = () => {
    const blob = new Blob([toCsv(results)], {
      type: "text/csv;charset=utf-8",
    })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "sparql-results.csv"
    a.click()
    URL.revokeObjectURL(url)
  }
  return (
    <div>
      <p className="mb-2 flex items-baseline gap-3 text-xs text-muted-foreground">
        <span>
          {results.rows.length.toLocaleString()} result
          {results.rows.length === 1 ? "" : "s"}
          {results.rows.length > MAX_ROWS &&
            ` (showing first ${MAX_ROWS.toLocaleString()})`}
        </span>
        {results.rows.length > 0 && (
          <button
            type="button"
            onClick={downloadCsv}
            className="underline hover:text-foreground"
          >
            Download CSV
          </button>
        )}
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-rule-hair text-left">
              {results.vars.map((v) => (
                <th key={v} className="small-caps py-1.5 pr-4 font-medium">
                  {v}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {shown.map((row, i) => (
              <tr key={i} className="border-b border-rule-hair/50">
                {results.vars.map((v) => (
                  <td key={v} className="py-1.5 pr-4 align-top">
                    <TermCell term={row[v]} personMap={personMap} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function TermCell({
  term,
  personMap,
}: {
  term: ResultTerm | undefined
  personMap: Map<number, string>
}) {
  if (!term) return <span className="text-muted-foreground">—</span>
  if (term.type === "uri") {
    const route = personRouteForIri(term.value, personMap)
    if (route) {
      const id = route.slice("/persons/".length)
      return (
        <Link
          to="/persons/$id"
          params={{ id }}
          className="text-accent-ink hover:underline"
        >
          {shortenIri(term.value)}
        </Link>
      )
    }
    return (
      <a
        href={term.value}
        target="_blank"
        rel="noreferrer"
        className="text-accent-ink hover:underline"
      >
        {shortenIri(term.value)}
      </a>
    )
  }
  if (term.type === "bnode") {
    return <span className="font-mono text-xs">_:{term.value}</span>
  }
  return (
    <span>
      {term.value}
      {term["xml:lang"] && (
        <span className="ml-1 text-xs text-muted-foreground">
          @{term["xml:lang"]}
        </span>
      )}
    </span>
  )
}
