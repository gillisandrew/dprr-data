// site/src/components/advanced-search.tsx
import { FacetCombobox } from "./facet-combobox"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import type { SearchState, FacetValue } from "@/data/types"

interface AdvancedSearchProps {
  facets: {
    office: FacetValue[]
    praenomen: FacetValue[]
    cognomen: FacetValue[]
  }
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
}

export function AdvancedSearch({
  facets,
  state,
  onUpdate,
}: AdvancedSearchProps) {
  return (
    <div className="space-y-3">
      <FacetCombobox
        label="Office"
        values={facets.office}
        selected={state.office}
        onChange={(office) => onUpdate({ office })}
      />
      <FacetCombobox
        label="Praenomen"
        values={facets.praenomen}
        selected={state.praenomen}
        onChange={(praenomen) => onUpdate({ praenomen })}
      />
      <FacetCombobox
        label="Cognomen"
        values={facets.cognomen}
        selected={state.cognomen}
        onChange={(cognomen) => onUpdate({ cognomen })}
      />
      <div className="space-y-1">
        <p className="micro-label-muted">RE number</p>
        <Input
          value={state.re}
          onChange={(e) => onUpdate({ re: e.target.value })}
          placeholder="e.g. 46a"
          className="h-7 text-xs"
        />
      </div>
      <label className="flex cursor-pointer items-center gap-2 text-xs">
        <Checkbox
          checked={state.officeMode === "all"}
          onCheckedChange={(c) => onUpdate({ officeMode: c ? "all" : "any" })}
        />
        <span>Require every selected office (AND)</span>
      </label>
      <label className="flex cursor-pointer items-center gap-2 text-xs">
        <Checkbox
          checked={state.officeInRange}
          onCheckedChange={(c) => onUpdate({ officeInRange: c === true })}
        />
        <span>Apply time period to offices (held in range)</span>
      </label>
    </div>
  )
}
