// Emit short-URL redirects after `vp build`.
//
// /persons/0522 -> /persons/VALE0522. Every DPRR id ends in four digits and
// those digits are globally unique, so the bare number is an unambiguous
// short citation form.
//
// These have to be real files rather than a client-side redirect: the site
// is served as static files from GitHub Pages and public/404.html is a
// static dead end, so an un-prerendered path never boots the app and a
// pasted short URL would simply 404.
//
// Runs after prerender, so this is plain file writing, not rendering.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { bareIdAliases, redirectHtml } from "./id-aliases.mjs"

const BASE = "/dprr-data"
const dist = new URL("../dist/client/", import.meta.url)

const ids = JSON.parse(
  readFileSync(new URL("data/person-ids.json", dist), "utf8")
)

// Throws on a suffix collision, an id that stops ending in four digits, or
// an alias that would shadow a real person page. Failing the build is the
// point: a redirect to the wrong person looks correct.
const aliases = bareIdAliases(ids)

for (const [alias, canonicalId] of aliases) {
  const dir = new URL(`persons/${alias}/`, dist)
  mkdirSync(dir, { recursive: true })
  writeFileSync(new URL("index.html", dir), redirectHtml(canonicalId, BASE))
}

console.log(`[id-redirects] wrote ${aliases.size} bare-id redirects`)
