// site/src/lib/gens-name.ts

/**
 * Feminine gens form of a nomen for display on gens pages ("gens Cornelia"
 * rather than "gens Cornelius"), per classical convention. The stored nomen
 * stays masculine everywhere it names a person; only gens page titles and
 * copy use this. Uncertain-attribution punctuation like "(Cornelius?)" is
 * preserved around the transformed name.
 */
export function gensDisplayName(nomen: string): string {
  return nomen.replace(/[A-Za-z]+(?=[^A-Za-z]*$)/, (word) => {
    if (word.endsWith("ius")) return `${word.slice(0, -3)}ia`
    if (word.endsWith("us")) return `${word.slice(0, -2)}a`
    return word
  })
}
