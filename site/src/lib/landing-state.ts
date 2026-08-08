// site/src/lib/landing-state.ts

/** Drives the landing → loading → results handoff on the search page. */
export interface LandingBufferState {
  /** Once true, the bare landing screen is gone for good — either the
   * loading-with-input screen or full results render instead. */
  interacted: boolean
  /** The query typed so far, applied to real search state exactly once
   * (when the search bundle finishes loading). */
  pendingQuery: string | null
}

export type LandingBufferAction =
  /** Fired on every keystroke, in both the landing input and the
   * loading-state input that replaces it — the same buffer keeps
   * accumulating across that transition. */
  | { type: "type"; query: string }
  /** Fired by a non-typing interaction (e.g. a "Browse by" card) that
   * should also leave the landing screen. */
  | { type: "interact" }
  /** Fired once the buffered query has been applied to real search state. */
  | { type: "apply-pending" }

export const initialLandingBufferState: LandingBufferState = {
  interacted: false,
  pendingQuery: null,
}

/**
 * Pure reducer for the landing-input buffer. Extracted so the fix for
 * "typing at the landing screen loses everything after the first
 * keystroke" has a regression test independent of React: previously the
 * landing's input unmounted the instant `interacted` flipped true (on the
 * first non-blank character), replacing it with a bare "Loading…" message
 * with no input at all — every subsequent keystroke had nowhere to go.
 *
 * `interacted` latches true on the first non-blank keystroke (or an
 * explicit "interact") and never resets; `pendingQuery` keeps tracking the
 * latest typed text right up until it's applied.
 */
export function landingBufferReducer(
  state: LandingBufferState,
  action: LandingBufferAction
): LandingBufferState {
  switch (action.type) {
    case "type":
      return {
        pendingQuery: action.query,
        interacted: state.interacted || action.query.trim().length > 0,
      }
    case "interact":
      return { ...state, interacted: true }
    case "apply-pending":
      return { ...state, pendingQuery: null }
  }
}
