// site/src/data/parse-filiation.ts

// Canonical praenomen abbreviations as they appear in DPRR filiation
// strings ("Q. f. Ser. n." = son of Quintus, grandson of Servius).
// The praenomina reference file carries no abbreviation property, so the
// standard epigraphic table is encoded here. Abbreviations observed in
// the export but with no unambiguous expansion (e.g. "S.", "Stat.",
// "Ann.", "V.") are deliberately absent — unknown slots resolve to null.
const PRAENOMEN_ABBREVIATIONS: Record<string, string> = {
  "A.": "Aulus",
  "Agripp.": "Agrippa",
  "Ap.": "Appius",
  "C.": "Gaius",
  "Cn.": "Gnaeus",
  "D.": "Decimus",
  "K.": "Caeso",
  "L.": "Lucius",
  "M.": "Marcus",
  "M'.": "Manius",
  "Mam.": "Mamercus",
  "Minat.": "Minatius",
  "N.": "Numerius",
  "Opet.": "Opiter",
  "Opit.": "Opiter",
  "P.": "Publius",
  "Post.": "Postumus",
  "Q.": "Quintus",
  "Ser.": "Servius",
  "Sex.": "Sextus",
  "Sp.": "Spurius",
  "T.": "Titus",
  "Ti.": "Tiberius",
  "Voler.": "Volero",
  "Volus.": "Volusus",
  "Vop.": "Vopiscus",
}

export interface FiliationAncestors {
  father: string | null
  grandfather: string | null
}

/**
 * Extract the father's and grandfather's praenomina from a filiation
 * string ("Q. f. Ser. n."). Unknown ("-"), ambiguous ("Q. or L.?"), or
 * unrecognized slots yield null — the search filter simply never matches
 * them.
 */
export function parseFiliation(filiation: string | null): FiliationAncestors {
  if (!filiation) return { father: null, grandfather: null }
  // Strip parentheses so "(Sex. n.)" tokenizes like "Sex. n."
  const tokens = filiation.replace(/[()]/g, "").trim().split(/\s+/)
  return {
    father: slotBefore(tokens, "f."),
    grandfather: slotBefore(tokens, "n."),
  }
}

function slotBefore(tokens: string[], marker: "f." | "n."): string | null {
  const idx = tokens.indexOf(marker)
  if (idx <= 0) return null
  // Walk back past detached uncertainty markers ("Ser. ? f.")
  let i = idx - 1
  while (i >= 0 && tokens[i].replace(/\?/g, "") === "") i--
  if (i < 0) return null
  // "Q. or L.?" — an ambiguous slot; refuse to guess
  if (i >= 1 && tokens[i - 1] === "or") return null
  const abbrev = tokens[i].replace(/\?/g, "")
  return PRAENOMEN_ABBREVIATIONS[abbrev] ?? null
}
