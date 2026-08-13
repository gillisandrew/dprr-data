// site/src/lib/filter-panel.ts
// Disclosure logic for the search page's collapsible filter panel: which
// trigger shows what active count, and when tucked-away tiers/fields must
// start revealed so an active filter is never hidden.
import type { SearchState } from "@/data/types"

export type PanelSection =
  | "office"
  | "name"
  | "status"
  | "tribe"
  | "location"
  | "events"
  | "source"
  | "relationship"

/** Active-filter count shown on a section's trigger pill. */
export function sectionCount(state: SearchState, key: PanelSection): number {
  switch (key) {
    case "office":
      return (
        state.office.length +
        (state.officeMode === "all" ? 1 : 0) +
        (state.officeInRange ? 1 : 0)
      )
    case "name":
      return (
        state.praenomen.length +
        state.nomen.length +
        state.cognomen.length +
        state.father.length +
        state.grandfather.length +
        (state.re.trim() ? 1 : 0)
      )
    case "status":
      return state.status.length + state.sex.length
    case "tribe":
      return state.tribe.length
    case "location":
      return state.province.length
    case "events":
      return state.event.length
    case "source":
      return state.source.length
    case "relationship":
      return state.relationship.length
  }
}

/** Combined active count of the advanced tier (Tribe, Provincia, Events,
 * Source, Relationship);
 * shown on the collapsed "More filters" trigger and used to force the tier
 * open on deep links. */
export function advancedActiveCount(state: SearchState): number {
  return (
    state.tribe.length +
    state.province.length +
    state.event.length +
    state.source.length +
    state.relationship.length
  )
}

/** True when any tucked Name field (praenomen, father, grandfather, RE)
 * carries a value — forces the "More name fields" reveal open. */
export function nameExtrasActive(state: SearchState): boolean {
  return (
    state.praenomen.length > 0 ||
    state.father.length > 0 ||
    state.grandfather.length > 0 ||
    state.re.trim().length > 0
  )
}

/** True when either office option is non-default — forces the Office
 * "Options" reveal open. */
export function officeOptionsActive(state: SearchState): boolean {
  return state.officeMode === "all" || state.officeInRange
}
