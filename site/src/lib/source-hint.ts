// site/src/lib/source-hint.ts
// Pure helpers for the SourceHint book-icon popover.

export interface HintSource {
  secondarySource: string
  notes: string | null
}

/**
 * Whether a source list would actually render anything. SourceCitation
 * renders null for an empty name, so a list of blank entries produces a
 * book icon that opens onto an empty popover — an affordance promising
 * information the record doesn't have.
 */
export function hasRenderableSources(sources: HintSource[]): boolean {
  return sources.some(
    (s) => s.secondarySource.trim() !== "" || (s.notes ?? "").trim() !== ""
  )
}
