// site/src/data/parse-filiation.ts

// Canonical praenomen abbreviations as they appear in DPRR filiation
// strings ("Q. f. Ser. n." = son of Quintus, grandson of Servius).
//
// reference/praenomina.ttl DOES carry abbreviations — under `dprr:Abbreviation`
// rather than the `hasAbbreviation` every other reference file uses, which is
// why an earlier note here claimed it didn't. That file was checked against
// this table (2026-08-12): the two never disagree, and of the 2,149 filiation
// slots in the data it would resolve exactly one more ("V." -> Vibius, now
// included). Its remaining ~19 extra entries are praenomina that never appear
// in a filiation slot, and it lacks the variant spellings "Agripp." and
// "Opit." that the data does use — so this stays a hand-maintained table
// rather than being derived from the reference file.
//
// Abbreviations with no entry in the reference file either (e.g. "S.",
// "Stat.") are deliberately absent — unknown slots resolve to null rather
// than inventing ancestry. So are the 231 "-" slots recorded as unknown, and
// genitive cognomina like "Vopisci" or "Aemiliani", which are not praenomina.
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
  "V.": "Vibius",
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
