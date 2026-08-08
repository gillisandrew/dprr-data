import { expect, test, describe } from "vite-plus/test"
import {
  landingBufferReducer,
  initialLandingBufferState,
  type LandingBufferState,
} from "./landing-state"

function typeChars(
  state: LandingBufferState,
  chars: string
): LandingBufferState {
  let acc = ""
  let current = state
  for (const ch of chars) {
    acc += ch
    current = landingBufferReducer(current, { type: "type", query: acc })
  }
  return current
}

describe("landingBufferReducer", () => {
  test("starts on the landing screen with no buffered query", () => {
    expect(initialLandingBufferState).toEqual({
      interacted: false,
      pendingQuery: null,
    })
  })

  test("typing 'brutus' rapidly survives into the final buffer", () => {
    const final = typeChars(initialLandingBufferState, "brutus")
    expect(final.pendingQuery).toBe("brutus")
    expect(final.interacted).toBe(true)
  })

  test("interacted latches true after the first non-blank keystroke", () => {
    const afterFirst = landingBufferReducer(initialLandingBufferState, {
      type: "type",
      query: "b",
    })
    expect(afterFirst.interacted).toBe(true)
    // Deleting back to empty must not un-latch it — the landing screen
    // shouldn't reappear mid-typing.
    const afterDelete = landingBufferReducer(afterFirst, {
      type: "type",
      query: "",
    })
    expect(afterDelete.interacted).toBe(true)
    expect(afterDelete.pendingQuery).toBe("")
  })

  test("whitespace-only typing does not leave the landing screen", () => {
    const afterSpace = landingBufferReducer(initialLandingBufferState, {
      type: "type",
      query: "   ",
    })
    expect(afterSpace.interacted).toBe(false)
    expect(afterSpace.pendingQuery).toBe("   ")
  })

  test("'interact' leaves the landing screen without touching the buffer", () => {
    const result = landingBufferReducer(initialLandingBufferState, {
      type: "interact",
    })
    expect(result.interacted).toBe(true)
    expect(result.pendingQuery).toBeNull()
  })

  test("'apply-pending' clears the buffer but keeps interacted latched", () => {
    const typed = typeChars(initialLandingBufferState, "brutus")
    const applied = landingBufferReducer(typed, { type: "apply-pending" })
    expect(applied.pendingQuery).toBeNull()
    expect(applied.interacted).toBe(true)
  })
})
