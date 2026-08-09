import { useState } from "react"
import { FilterPopover } from "./filter-popover"
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { FacetCombobox } from "./facet-combobox"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import type { SearchState, FacetValue } from "@/data/types"

type BandKey = "office" | "name" | "status" | "tribe" | "location" | "events"

interface FilterBandProps {
  facets: {
    office: FacetValue[]
    nomen: FacetValue[]
    sex: FacetValue[]
    tribe: FacetValue[]
    province: FacetValue[]
    event: FacetValue[]
    praenomen: FacetValue[]
    cognomen: FacetValue[]
    status: FacetValue[]
    father: FacetValue[]
    grandfather: FacetValue[]
  }
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
  /** Popover to open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "gens"
}

export function FilterBand({
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
  initialFocus,
}: FilterBandProps) {
  // One open popover at a time; the band owns which.
  const [openKey, setOpenKey] = useState<BandKey | null>(() =>
    initialFocus === "office"
      ? "office"
      : initialFocus === "gens"
        ? "name"
        : null
  )
  const openFor = (key: BandKey) => ({
    open: openKey === key,
    onOpenChange: (open: boolean) => setOpenKey(open ? key : null),
  })

  const nameCount =
    state.praenomen.length +
    state.nomen.length +
    state.cognomen.length +
    state.father.length +
    state.grandfather.length +
    (state.re ? 1 : 0)
  const officeCount =
    state.office.length +
    (state.officeMode === "all" ? 1 : 0) +
    (state.officeInRange ? 1 : 0)
  const statusCount = state.status.length + state.sex.length

  return (
    <div className="rule-lead flex flex-wrap items-center gap-x-2 gap-y-1.5 pt-2.5 pb-2">
      <FilterPopover
        label="Office"
        activeCount={officeCount}
        {...openFor("office")}
      >
        <FacetHierarchyGroup
          title="Office"
          items={facets.office}
          parentOf={officeHierarchy}
          selected={state.office}
          onChange={(office) => onUpdate({ office })}
          frameless
          hideCounts={state.officeMode === "all" || state.officeInRange}
        />
        <div className="mt-3 space-y-2 border-t border-rule-hair pt-3">
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <Checkbox
              checked={state.officeMode === "all"}
              onCheckedChange={(c) =>
                onUpdate({ officeMode: c ? "all" : "any" })
              }
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
      </FilterPopover>

      <FilterPopover label="Name" activeCount={nameCount} {...openFor("name")}>
        <div className="space-y-3">
          <FacetCombobox
            label="Praenomen"
            values={facets.praenomen}
            selected={state.praenomen}
            onChange={(praenomen) => onUpdate({ praenomen })}
          />
          <FacetCombobox
            label="Gens (nomen)"
            values={facets.nomen}
            selected={state.nomen}
            onChange={(nomen) => onUpdate({ nomen })}
          />
          <FacetCombobox
            label="Cognomen"
            values={facets.cognomen}
            selected={state.cognomen}
            onChange={(cognomen) => onUpdate({ cognomen })}
          />
          <FacetCombobox
            label="Father (praenomen)"
            values={facets.father}
            selected={state.father}
            onChange={(father) => onUpdate({ father })}
          />
          <FacetCombobox
            label="Grandfather (praenomen)"
            values={facets.grandfather}
            selected={state.grandfather}
            onChange={(grandfather) => onUpdate({ grandfather })}
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
        </div>
      </FilterPopover>

      <FilterPopover
        label="Status"
        activeCount={statusCount}
        {...openFor("status")}
      >
        <FacetGroup
          title="Status"
          items={facets.status}
          selected={state.status}
          onChange={(status) => onUpdate({ status })}
          frameless
        />
        <p className="micro-label-muted mt-3 mb-1 border-t border-rule-hair pt-3">
          Sex
        </p>
        <FacetGroup
          title="Sex"
          items={facets.sex}
          selected={state.sex}
          onChange={(sex) => onUpdate({ sex })}
          frameless
        />
      </FilterPopover>

      <FilterPopover
        label="Tribe"
        activeCount={state.tribe.length}
        {...openFor("tribe")}
      >
        <FacetGroup
          title="Tribe"
          items={facets.tribe}
          selected={state.tribe}
          onChange={(tribe) => onUpdate({ tribe })}
          frameless
          searchable
        />
      </FilterPopover>

      <FilterPopover
        label="Location"
        activeCount={state.province.length}
        {...openFor("location")}
      >
        <FacetHierarchyGroup
          title="Location"
          items={facets.province}
          parentOf={provinceHierarchy}
          selected={state.province}
          onChange={(province) => onUpdate({ province })}
          frameless
        />
      </FilterPopover>

      <FilterPopover
        label="Events"
        activeCount={state.event.length}
        {...openFor("events")}
      >
        <FacetGroup
          title="Life events"
          items={facets.event}
          selected={state.event}
          onChange={(event) => onUpdate({ event })}
          frameless
        />
      </FilterPopover>
    </div>
  )
}
