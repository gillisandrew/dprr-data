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
      label: `Province: ${province}`,
      onRemove: () =>
        onRemove({ province: state.province.filter((p) => p !== province) }),
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
