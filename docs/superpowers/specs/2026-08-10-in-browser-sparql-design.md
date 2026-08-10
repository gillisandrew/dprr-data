# In-browser SPARQL query page

Date: 2026-08-10
Status: approved

## Goal

Offer SPARQL over the full DPRR dataset at zero hosting cost: a `/sparql`
page on the existing static site that loads the whole corpus (~500k raw
triples, ~3.4MB gzipped) into Oxigraph compiled to WebAssembly and runs
queries entirely client-side. Also publish the deduplicated dump at a
stable URL for anyone who wants a local copy.

## Architecture

```
build time                         browser
----------                        --------
persons/**.ttl ─┐                 /sparql page (React)
reference/*.ttl ├─ dump plugin ─▶ public/data/dprr.nt.gz ─▶ Web Worker:
concordances/* ─┤   (oxigraph        (stable download URL)     fetch + gunzip
ontology.ttl  ──┘    in Node)                                  (DecompressionStream)
                                                               → Store.load()
                                                               → Store.query()
                                                               → postMessage results
```

- **Dump generation** (`site/src/build/sparql-dump-plugin.ts`): Vite plugin
  mirroring `static-data-plugin.ts`. Parses every TTL file into one Oxigraph
  store (set semantics dedupe the person-file overlap), dumps N-Triples,
  gzips to `public/data/dprr.nt.gz`. Skips regeneration when the output is
  newer than every source file (mtime check) so dev restarts stay fast.
- **Worker** (`site/src/lib/sparql-worker.ts`): module worker. On init:
  fetch dump, gunzip via DecompressionStream, `store.load`. On
  `{type:"query", sparql}`: run `store.query` with `results_format` —
  SELECT → SPARQL Results JSON string; CONSTRUCT/DESCRIBE → Turtle string;
  ASK → boolean. Replies with progress/ready/result/error messages; all
  payloads are plain serializable values.
- **UI** (`site/src/routes/sparql.tsx`): CodeMirror 6 editor (legacy SPARQL
  mode via `@codemirror/legacy-modes`), Run button (Cmd/Ctrl+Enter), example
  query picker, load-progress state, results panel:
  - SELECT: table styled like the site's ledger rows; IRI terms shortened
    with the dataset's known prefixes; `entity/Person/<n>` IRIs link to the
    local person page (numeric id ↔ DPRR id digits), other IRIs link out.
  - CONSTRUCT/DESCRIBE: Turtle in a `<pre>`.
  - ASK: yes/no.
  - Errors (bad syntax, etc.): message inline.
- Worker starts in `useEffect` so prerendering emits a plain shell.
- Footer gains a "SPARQL" link; the About page's attribution section
  mentions the query page and the dump URL.

## Person-link mapping

DPRR ids embed a globally unique number (`CORN0076` ↔ `entity/Person/76`).
The page fetches the existing person search index once and builds a
number → id map to make Person IRIs clickable locally. Best-effort: if a
number is missing the IRI just links to the official DPRR URI.

## Testing

- Unit tests: term shortening/prefix table, SPARQL-JSON → table model,
  Person-IRI ↔ local-route mapping.
- `vp check`, `vp test`, `vp build` pass; browser smoke test of a SELECT,
  a CONSTRUCT, an ASK, and a syntax error on the preview build.

## Out of scope

- A network-callable endpoint (documented as available upstream at
  romanrepublic.ac.uk/rdf/).
- Query persistence/sharing, autocomplete, federation.
