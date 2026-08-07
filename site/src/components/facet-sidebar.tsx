// site/src/components/facet-sidebar.tsx
import { useState } from "react"
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { EraTimeline } from "./era-timeline"
import { AdvancedSearch } from "./advanced-search"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Checkbox } from "@/components/ui/checkbox"
import { ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"
import type { SearchState, FacetValue } from "@/data/types"
import type { Histogram } from "@/lib/histogram"

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
  histogram: Histogram
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
  /** Facet to force-open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "time" | "gens"
}

export function FacetSidebar({
  facets,
  histogram,
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
      {/* Tier 1 — always visible */}
      <FacetHierarchyGroup
        title="Office"
        items={facets.office}
        parentOf={officeHierarchy}
        selected={state.office}
        onChange={(office) => onUpdate({ office })}
        defaultOpen={initialFocus === "office"}
      />
      <div className="space-y-1 py-2">
        <p className="text-sm font-semibold">Time period</p>
        <EraTimeline
          histogram={histogram}
          from={state.eraFrom}
          to={state.eraTo}
          onChange={(eraFrom, eraTo) => onUpdate({ eraFrom, eraTo })}
        />
      </div>

      {/* Tier 2 — more filters */}
      <Collapsible open={tier2Open} onOpenChange={setTier2Open}>
        <CollapsibleTrigger className="flex w-full items-center gap-1.5 py-2 text-sm font-semibold">
          <ChevronRight
            className={cn(
              "h-3.5 w-3.5 shrink-0 transition-transform",
              tier2Open && "rotate-90"
            )}
          />
          More filters
        </CollapsibleTrigger>
        <CollapsibleContent className="space-y-1 pl-2">
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
        <CollapsibleTrigger className="flex w-full items-center gap-1.5 py-2 text-sm font-semibold">
          <ChevronRight
            className={cn(
              "h-3.5 w-3.5 shrink-0 transition-transform",
              tier3Open && "rotate-90"
            )}
          />
          Advanced search
        </CollapsibleTrigger>
        <CollapsibleContent className="pb-3 pl-2">
          <AdvancedSearch facets={facets} state={state} onUpdate={onUpdate} />
        </CollapsibleContent>
      </Collapsible>
    </aside>
  )
}
