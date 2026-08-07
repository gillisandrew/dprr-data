// site/src/components/active-filter-chips.tsx
import { Badge } from "@/components/ui/badge"
import { X } from "lucide-react"
import type { SearchState } from "@/data/types"

interface ActiveFilterChipsProps {
  state: SearchState
  onRemove: (updates: Partial<SearchState>) => void
  onClearAll: () => void
}

export function ActiveFilterChips({
  state,
  onRemove,
  onClearAll,
}: ActiveFilterChipsProps) {
  const chips: { label: string; onRemove: () => void }[] = []

  for (const office of state.office) {
    chips.push({
      label: `Office: ${office}`,
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
  if (state.patrician !== null) {
    chips.push({
      label: state.patrician ? "Patrician" : "Non-Patrician",
      onRemove: () => onRemove({ patrician: null }),
    })
  }
  if (state.nobilis !== null) {
    chips.push({
      label: state.nobilis ? "Nobilis" : "Non-Nobilis",
      onRemove: () => onRemove({ nobilis: null }),
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
    chips.push({
      label: `Location: ${province}`,
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
  if (state.re) {
    chips.push({
      label: `RE: ${state.re}`,
      onRemove: () => onRemove({ re: "" }),
    })
  }

  if (chips.length === 0) return null

  return (
    <div className="flex flex-wrap items-center gap-2">
      {chips.map((chip) => (
        <Badge
          key={chip.label}
          variant="secondary"
          className="cursor-pointer gap-1"
          onClick={chip.onRemove}
        >
          {chip.label}
          <X className="h-3 w-3" />
        </Badge>
      ))}
      {chips.length > 1 && (
        <button
          onClick={onClearAll}
          className="text-xs text-muted-foreground underline hover:text-foreground"
        >
          Clear all
        </button>
      )}
    </div>
  )
}
