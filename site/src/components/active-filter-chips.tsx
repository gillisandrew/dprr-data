// site/src/components/active-filter-chips.tsx
import { X } from "lucide-react"
import type { SearchState } from "@/data/types"

interface ActiveFilterChipsProps {
  state: SearchState
  onRemove: (updates: Partial<SearchState>) => void
  onClearAll: () => void
  officesWithChildren?: Set<string>
  provincesWithChildren?: Set<string>
}

export function ActiveFilterChips({
  state,
  onRemove,
  onClearAll,
  officesWithChildren = new Set(),
  provincesWithChildren = new Set(),
}: ActiveFilterChipsProps) {
  const chips: { label: string; onRemove: () => void }[] = []

  for (const office of state.office) {
    const suffix = officesWithChildren.has(office) ? " + sub-offices" : ""
    chips.push({
      label: `Office: ${office}${suffix}`,
      onRemove: () =>
        onRemove({ office: state.office.filter((o) => o !== office) }),
    })
  }
  for (const nomen of state.nomen) {
    chips.push({
      label: `Gens: ${nomen}`,
      onRemove: () =>
        onRemove({ nomen: state.nomen.filter((n) => n !== nomen) }),
    })
  }
  for (const sex of state.sex) {
    chips.push({
      label: sex,
      onRemove: () => onRemove({ sex: state.sex.filter((s) => s !== sex) }),
    })
  }
  for (const status of state.status) {
    chips.push({
      label: status,
      onRemove: () =>
        onRemove({ status: state.status.filter((s) => s !== status) }),
    })
  }
  for (const father of state.father) {
    chips.push({
      label: `Father: ${father}`,
      onRemove: () =>
        onRemove({ father: state.father.filter((f) => f !== father) }),
    })
  }
  for (const grandfather of state.grandfather) {
    chips.push({
      label: `Grandfather: ${grandfather}`,
      onRemove: () =>
        onRemove({
          grandfather: state.grandfather.filter((g) => g !== grandfather),
        }),
    })
  }
  for (const tribe of state.tribe) {
    chips.push({
      label: `Tribe: ${tribe}`,
      onRemove: () =>
        onRemove({ tribe: state.tribe.filter((t) => t !== tribe) }),
    })
  }
  for (const province of state.province) {
    const suffix = provincesWithChildren.has(province)
      ? " + sub-provinciae"
      : ""
    chips.push({
      label: `Provincia: ${province}${suffix}`,
      onRemove: () =>
        onRemove({ province: state.province.filter((p) => p !== province) }),
    })
  }
  for (const event of state.event) {
    chips.push({
      label: `Event: ${event}`,
      onRemove: () =>
        onRemove({ event: state.event.filter((e) => e !== event) }),
    })
  }
  for (const praenomen of state.praenomen) {
    chips.push({
      label: `Praenomen: ${praenomen}`,
      onRemove: () =>
        onRemove({
          praenomen: state.praenomen.filter((p) => p !== praenomen),
        }),
    })
  }
  for (const cognomen of state.cognomen) {
    chips.push({
      label: `Cognomen: ${cognomen}`,
      onRemove: () =>
        onRemove({ cognomen: state.cognomen.filter((c) => c !== cognomen) }),
    })
  }
  if (state.officeMode === "all") {
    chips.push({
      label: "Offices: all of",
      onRemove: () => onRemove({ officeMode: "any" }),
    })
  }
  if (state.officeInRange) {
    chips.push({
      label: "Offices in time range",
      onRemove: () => onRemove({ officeInRange: false }),
    })
  }
  if (state.re.trim()) {
    chips.push({
      label: `RE: ${state.re.trim()}`,
      onRemove: () => onRemove({ re: "" }),
    })
  }

  if (chips.length === 0) return null

  return (
    <div className="flex flex-wrap items-center gap-2">
      {chips.map((chip) => (
        <button
          type="button"
          key={chip.label}
          onClick={chip.onRemove}
          className="rule-hair inline-flex items-center gap-1 pb-0.5 text-xs text-foreground hover:text-accent-ink"
        >
          {chip.label}
          <X className="h-3 w-3 text-muted-foreground" />
        </button>
      ))}
      {chips.length > 1 && (
        <button
          type="button"
          onClick={onClearAll}
          className="text-xs text-muted-foreground underline hover:text-foreground"
        >
          Clear all
        </button>
      )}
    </div>
  )
}
