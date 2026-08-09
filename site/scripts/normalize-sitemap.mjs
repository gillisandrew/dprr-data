// Normalize dist/client/sitemap.xml after `vp build`.
//
// TanStack Start's sitemap generator joins host + route path but is unaware
// of Vite's `base`: crawled pages carry the /dprr-data prefix in their path
// (doubling it against the prefixed host) while statically discovered routes
// don't. This pass collapses repeated prefixes, adds the prefix where it is
// missing, canonicalizes to the trailing-slash form GitHub Pages actually
// serves (the slashless form 301s), and dedupes the overlap between the two
// discovery mechanisms.
import { readFileSync, writeFileSync } from "node:fs"

const BASE = "/dprr-data"
const file = new URL("../dist/client/sitemap.xml", import.meta.url)
const xml = readFileSync(file, "utf8")

const seen = new Set()
let dropped = 0

const out = xml.replace(
  /<url>[\s\S]*?<loc>([^<]+)<\/loc>([\s\S]*?)<\/url>(\s*)/g,
  (_, loc, rest, ws) => {
    const u = new URL(loc)
    let p = u.pathname.replace(new RegExp(`^(${BASE})+`), BASE)
    if (!p.startsWith(BASE)) p = BASE + p
    if (!p.endsWith("/")) p += "/"
    const url = u.origin + p
    if (seen.has(url)) {
      dropped++
      return ""
    }
    seen.add(url)
    return `<url><loc>${url}</loc>${rest}</url>${ws}`
  }
)

writeFileSync(file, out)
console.log(
  `[normalize-sitemap] ${seen.size} unique URLs (${dropped} duplicates dropped)`
)
