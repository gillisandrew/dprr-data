// Pure alias derivation for bare-ID redirects, kept separate from the
// emitting script so it can be unit-tested without running a build.

/**
 * Map each person's bare four-digit suffix to its canonical DPRR ID.
 *
 * @param {string[]} ids canonical person IDs, e.g. ["VALE0522"]
 * @returns {Map<string, string>} alias -> canonical ID
 */
export function bareIdAliases(ids) {
  const aliases = new Map()
  const canonical = new Set(ids)
  for (const id of ids) {
    const match = /(\d{4})$/.exec(id)
    if (!match) {
      // Skipping silently would hide a data-shape change behind a person
      // that quietly stops having a short URL.
      throw new Error(`Person id does not end in four digits: "${id}"`)
    }
    const alias = match[1]
    if (canonical.has(alias)) {
      // Writing this alias would overwrite a real prerendered person page
      // with a page that redirects to itself.
      throw new Error(
        `Bare-ID alias "${alias}" (from "${id}") is itself a canonical person id`
      )
    }
    const prior = aliases.get(alias)
    if (prior !== undefined) {
      // Fail the build rather than emit a redirect that silently sends
      // readers to the wrong person — that would look correct.
      throw new Error(
        `Bare-ID collision: "${alias}" maps to both "${prior}" and "${id}"`
      )
    }
    aliases.set(alias, id)
  }
  return aliases
}

/**
 * A standalone redirect page for one alias. It must not depend on the app
 * bundle, so the styling is inline — same approach as public/404.html.
 *
 * @param {string} canonicalId the person ID to redirect to
 * @param {string} base Vite's base path, e.g. "/dprr-data"
 * @returns {string} a complete HTML document
 */
export function redirectHtml(canonicalId, base) {
  const target = `${base}/persons/${canonicalId}`
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>${canonicalId} — DPRR</title>
    <link rel="canonical" href="${target}">
    <meta http-equiv="refresh" content="0; url=${target}">
    <style>
      body { font-family: "Inter", sans-serif; display: flex;
        justify-content: center; align-items: center; min-height: 100vh;
        margin: 0; background: #fafafa; color: #333; }
      a { color: #c0392b; }
    </style>
  </head>
  <body>
    <p>Redirecting to <a href="${target}">${canonicalId}</a>…</p>
  </body>
</html>
`
}
