// site/src/components/facet-sidebar.tsx
import { useState } from "react"
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { AdvancedSearch } from "./advanced-search"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Checkbox } from "@/components/ui/checkbox"
import type { SearchState, FacetValue } from "@/data/types"

interface FacetSidebarProps {
  facets: {
    office: FacetValue[]
    nomen: FacetValue[]
    sex: FacetValue[]
    tribe: FacetValue[]
    province: FacetValue[]
    event: FacetValue[]
    praenomen: FacetValue[]
    cognomen: FacetValue[]
  }
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
  /** Facet to force-open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "time" | "gens"
}

export function FacetSidebar({
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
  initialFocus,
}: FacetSidebarProps) {
  const tier2Active =
    state.nomen.length > 0 ||
    state.tribe.length > 0 ||
    state.province.length > 0 ||
    state.sex.length > 0 ||
    state.patrician !== null ||
    state.nobilis !== null ||
    state.event.length > 0
  const tier3Active =
    state.officeMode !== "any" ||
    state.officeInRange ||
    state.praenomen.length > 0 ||
    state.cognomen.length > 0 ||
    state.re !== ""

  const [tier2Open, setTier2Open] = useState(
    () => tier2Active || initialFocus === "gens"
  )
  const [tier3Open, setTier3Open] = useState(() => tier3Active)

  return (
    <aside className="w-56 shrink-0 space-y-1">
      {/* Tier 1 — always visible. Always open by default: per spec §1.2 the
       * Tier-1 tree must be visible, and a deep link must never hide its
       * own filters. */}
      <FacetHierarchyGroup
        title="Office"
        items={facets.office}
        parentOf={officeHierarchy}
        selected={state.office}
        onChange={(office) => onUpdate({ office })}
        defaultOpen={true}
        hideCounts={state.officeMode === "all" || state.officeInRange}
      />

      {/* Tier 2 — more filters */}
      <Collapsible open={tier2Open} onOpenChange={setTier2Open}>
        <CollapsibleTrigger className="rule-hair flex w-full items-center justify-between pt-4 pb-1 text-sm font-medium">
          More filters
          <span className="text-muted-foreground" aria-hidden="true">
            {tier2Open ? "−" : "+"}
          </span>
        </CollapsibleTrigger>
        <CollapsibleContent className="space-y-1">
          <FacetGroup
            title="Gens"
            items={facets.nomen}
            selected={state.nomen}
            onChange={(nomen) => onUpdate({ nomen })}
            searchable
          />
          <FacetGroup
            title="Tribe"
            items={facets.tribe}
            selected={state.tribe}
            onChange={(tribe) => onUpdate({ tribe })}
            defaultOpen={false}
            searchable
          />
          <FacetHierarchyGroup
            title="Location"
            items={facets.province}
            parentOf={provinceHierarchy}
            selected={state.province}
            onChange={(province) => onUpdate({ province })}
            defaultOpen={false}
          />
          <FacetGroup
            title="Sex"
            items={facets.sex}
            selected={state.sex}
            onChange={(sex) => onUpdate({ sex })}
            defaultOpen={false}
          />
          <div className="space-y-1">
            <p className="micro-label rule-hair pt-3 pb-1">Status</p>
            <div>
              <label className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6">
                <Checkbox
                  checked={state.patrician === true}
                  onCheckedChange={(checked) =>
                    onUpdate({ patrician: checked ? true : null })
                  }
                />
                <span>Patrician</span>
              </label>
              <label className="flex cursor-pointer items-center gap-2 py-0.5 text-[0.8125rem] leading-6">
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
            title="Life events"
            items={facets.event}
            selected={state.event}
            onChange={(event) => onUpdate({ event })}
            defaultOpen={false}
          />
        </CollapsibleContent>
      </Collapsible>

      {/* Tier 3 — advanced search */}
      <Collapsible open={tier3Open} onOpenChange={setTier3Open}>
        <CollapsibleTrigger className="rule-hair flex w-full items-center justify-between pt-4 pb-1 text-sm font-medium">
          Advanced search
          <span className="text-muted-foreground" aria-hidden="true">
            {tier3Open ? "−" : "+"}
          </span>
        </CollapsibleTrigger>
        <CollapsibleContent className="pt-2 pb-3">
          <AdvancedSearch facets={facets} state={state} onUpdate={onUpdate} />
        </CollapsibleContent>
      </Collapsible>
    </aside>
  )
}
