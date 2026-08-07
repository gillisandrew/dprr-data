// site/src/components/facet-sidebar.tsx
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { FacetRangeGroup } from "./facet-range-group"
import { Checkbox } from "@/components/ui/checkbox"
import type { SearchState, FacetValue } from "@/data/types"

interface FacetSidebarProps {
  facets: {
    office: FacetValue[]
    nomen: FacetValue[]
    sex: FacetValue[]
    tribe: FacetValue[]
    province: FacetValue[]
  }
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
}

export function FacetSidebar({
  facets,
  state,
  onUpdate,
  officeHierarchy,
  provinceHierarchy,
}: FacetSidebarProps) {
  return (
    <aside className="w-56 shrink-0 space-y-1">
      {/* Primary facets — expanded by default */}
      <FacetHierarchyGroup
        title="Office"
        items={facets.office}
        parentOf={officeHierarchy}
        selected={state.office}
        onChange={(office) => onUpdate({ office })}
        defaultOpen={false}
      />
      <FacetGroup
        title="Gens"
        items={facets.nomen}
        selected={state.nomen}
        onChange={(nomen) => onUpdate({ nomen })}
        searchable
      />
      <FacetRangeGroup
        title="Era"
        fromValue={state.eraFrom}
        toValue={state.eraTo}
        onFromChange={(eraFrom) => onUpdate({ eraFrom })}
        onToChange={(eraTo) => onUpdate({ eraTo })}
        fromPlaceholder="-509"
        toPlaceholder="-31"
      />

      {/* Secondary facets — collapsed by default */}
      <FacetGroup
        title="Sex"
        items={facets.sex}
        selected={state.sex}
        onChange={(sex) => onUpdate({ sex })}
        defaultOpen={false}
      />
      <div className="space-y-1 py-2">
        <p className="text-sm font-semibold">Status</p>
        <div className="space-y-1 pl-5">
          <label className="flex cursor-pointer items-center gap-2 text-sm">
            <Checkbox
              checked={state.patrician === true}
              onCheckedChange={(checked) =>
                onUpdate({ patrician: checked ? true : null })
              }
            />
            <span>Patrician</span>
          </label>
          <label className="flex cursor-pointer items-center gap-2 text-sm">
            <Checkbox
              checked={state.nobilis === true}
              onCheckedChange={(checked) =>
                onUpdate({ nobilis: checked ? true : null })
              }
            />
            <span>Nobilis</span>
          </label>
        </div>
      </div>
      <FacetGroup
        title="Tribe"
        items={facets.tribe}
        selected={state.tribe}
        onChange={(tribe) => onUpdate({ tribe })}
        defaultOpen={false}
        searchable
      />
      <FacetHierarchyGroup
        title="Province"
        items={facets.province}
        parentOf={provinceHierarchy}
        selected={state.province}
        onChange={(province) => onUpdate({ province })}
        defaultOpen={false}
      />
    </aside>
  )
}
