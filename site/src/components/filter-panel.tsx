// site/src/components/filter-panel.tsx
// Collapsible filter sections for the search page: a header row of pill
// triggers with the single open section expanding in place below it.
// Progressive disclosure at two levels — the advanced tier (Tribe,
// Location, Events) sits behind "More filters", and Name/Office tuck their
// rarely-used fields behind in-section reveals. Replaces the popover band.
import { useState } from "react"
import { ChevronDown } from "lucide-react"
import { FacetGroup } from "./facet-group"
import { FacetHierarchyGroup } from "./facet-hierarchy-group"
import { FacetCombobox } from "./facet-combobox"
import { InfoHint } from "./info-hint"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import {
  sectionCount,
  advancedActiveCount,
  nameExtrasActive,
  officeOptionsActive,
  type PanelSection,
} from "@/lib/filter-panel"
import type { GlossaryTermId } from "@/lib/glossary"
import type { SearchState, FacetValue } from "@/data/types"

const BASIC: { key: PanelSection; label: string }[] = [
  { key: "office", label: "Office" },
  { key: "name", label: "Name" },
  { key: "status", label: "Status" },
]
const ADVANCED: { key: PanelSection; label: string }[] = [
  { key: "tribe", label: "Tribe" },
  { key: "location", label: "Location" },
  { key: "events", label: "Events" },
]

const SECTION_TERM: Record<PanelSection, GlossaryTermId> = {
  office: "office",
  name: "roman-names",
  status: "status",
  tribe: "tribe",
  location: "location",
  events: "life-events",
}

interface FilterPanelProps {
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
  /** Section to open on first render (from a landing "Browse by" card). */
  initialFocus?: "office" | "gens"
}

export function FilterPanel({
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
  initialFocus,
}: FilterPanelProps) {
  // Accordion: at most one section open, the panel owns which.
  const [openKey, setOpenKey] = useState<PanelSection | null>(() =>
    initialFocus === "office"
      ? "office"
      : initialFocus === "gens"
        ? "name"
        : null
  )
  // Deep links into Tribe/Location/Events must not hide their own filters.
  const [advancedRevealed, setAdvancedRevealed] = useState(
    () => advancedActiveCount(state) > 0
  )

  const toggle = (key: PanelSection) =>
    setOpenKey((cur) => (cur === key ? null : key))
  const collapseAdvanced = () => {
    setAdvancedRevealed(false)
    // Closing the tier must not leave an advanced section's body orphaned
    // below a header row that no longer shows its trigger.
    setOpenKey((cur) =>
      cur !== null && ADVANCED.some((s) => s.key === cur) ? null : cur
    )
  }

  const hiddenCount = advancedActiveCount(state)
  const triggers = advancedRevealed ? [...BASIC, ...ADVANCED] : BASIC

  return (
    <div className="rule-lead pt-2.5 pb-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
        {triggers.map(({ key, label }) => (
          <SectionTrigger
            key={key}
            id={`filter-trigger-${key}`}
            controls={`filter-section-${key}`}
            label={label}
            count={sectionCount(state, key)}
            open={openKey === key}
            onClick={() => toggle(key)}
          />
        ))}
        <button
          type="button"
          onClick={() =>
            advancedRevealed ? collapseAdvanced() : setAdvancedRevealed(true)
          }
          className="inline-flex items-center gap-1.5 rounded-[4px] border border-dashed border-border px-2.5 py-1 text-[0.6875rem] font-medium tracking-[0.1em] text-muted-foreground uppercase transition-colors hover:border-muted-foreground hover:text-foreground"
        >
          {advancedRevealed ? "Fewer filters" : "More filters"}
          {!advancedRevealed && hiddenCount > 0 && (
            <span className="text-accent-ink">({hiddenCount})</span>
          )}
        </button>
      </div>

      {openKey !== null && (
        <div
          role="region"
          id={`filter-section-${openKey}`}
          aria-labelledby={`filter-trigger-${openKey}`}
          className="mt-2 max-h-[45vh] overflow-y-auto rounded-[4px] border border-rule-hair p-3"
        >
          <div className="mb-2 flex items-center gap-1.5">
            <span className="micro-label-muted">
              {[...BASIC, ...ADVANCED].find((s) => s.key === openKey)!.label}
            </span>
            <InfoHint term={SECTION_TERM[openKey]} />
          </div>
          <SectionBody
            section={openKey}
            facets={facets}
            officeHierarchy={officeHierarchy}
            provinceHierarchy={provinceHierarchy}
            state={state}
            onUpdate={onUpdate}
          />
        </div>
      )}
    </div>
  )
}

/** Pill trigger, same visual language as the old band: accent ink when the
 * section carries active selections, quiet otherwise. */
function SectionTrigger({
  id,
  controls,
  label,
  count,
  open,
  onClick,
}: {
  id: string
  controls: string
  label: string
  count: number
  open: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      id={id}
      aria-expanded={open}
      aria-controls={controls}
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-[4px] border px-2.5 py-1 text-[0.6875rem] font-medium tracking-[0.1em] uppercase transition-colors",
        count > 0
          ? "border-accent-ink text-accent-ink"
          : "border-border text-muted-foreground hover:border-muted-foreground hover:text-foreground"
      )}
    >
      {label}
      {count > 0 && <span>({count})</span>}
      <ChevronDown
        aria-hidden="true"
        className={cn("h-3 w-3 transition-transform", open && "rotate-180")}
      />
    </button>
  )
}

/** In-section reveal for rarely-used fields. Forced open (with the button
 * hidden) while any tucked field is active, so no active filter is ever
 * invisible. Once revealed — by a click, or by mounting already forced —
 * it stays revealed for as long as the section stays open; closing the
 * section unmounts this component and resets the latch. */
function InSectionReveal({
  label,
  forced,
  children,
}: {
  label: string
  forced: boolean
  children: React.ReactNode
}) {
  // Seed from `forced` so a deep link that mounts already-forced-open
  // latches revealed=true. Without this, clearing the last forcing value
  // (e.g. deleting the RE number) would flip `forced` false with `revealed`
  // still false, unmounting the field group out from under the user.
  const [revealed, setRevealed] = useState(forced)
  if (!revealed && !forced) {
    return (
      <button
        type="button"
        onClick={() => setRevealed(true)}
        className="mt-2 text-xs text-muted-foreground hover:underline"
      >
        {label}
      </button>
    )
  }
  return <>{children}</>
}

function SectionBody({
  section,
  facets,
  officeHierarchy,
  provinceHierarchy,
  state,
  onUpdate,
}: {
  section: PanelSection
  facets: FilterPanelProps["facets"]
  officeHierarchy: Record<string, string | null>
  provinceHierarchy: Record<string, string | null>
  state: SearchState
  onUpdate: (updates: Partial<SearchState>) => void
}) {
  switch (section) {
    case "office":
      return (
        <>
          <FacetHierarchyGroup
            title="Office"
            items={facets.office}
            parentOf={officeHierarchy}
            selected={state.office}
            onChange={(office) => onUpdate({ office })}
            frameless
            hideCounts={state.officeMode === "all" || state.officeInRange}
          />
          <InSectionReveal label="Options…" forced={officeOptionsActive(state)}>
            <div className="mt-3 space-y-2 border-t border-rule-hair pt-3">
              <label className="flex cursor-pointer items-center gap-2 text-xs">
                <Checkbox
                  checked={state.officeMode === "all"}
                  onCheckedChange={(c) =>
                    onUpdate({ officeMode: c ? "all" : "any" })
                  }
                />
                <span>Require every selected office (AND)</span>
                <InfoHint term="office-and-mode" />
              </label>
              <label className="flex cursor-pointer items-center gap-2 text-xs">
                <Checkbox
                  checked={state.officeInRange}
                  onCheckedChange={(c) =>
                    onUpdate({ officeInRange: c === true })
                  }
                />
                <span>Apply time period to offices (held in range)</span>
                <InfoHint term="office-in-range" />
              </label>
            </div>
          </InSectionReveal>
        </>
      )
    case "name":
      return (
        <div className="space-y-3">
          <div className="grid gap-3 md:grid-cols-2">
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
          </div>
          <InSectionReveal
            label="More name fields…"
            forced={nameExtrasActive(state)}
          >
            <div className="grid gap-3 border-t border-rule-hair pt-3 md:grid-cols-2">
              <FacetCombobox
                label="Praenomen"
                values={facets.praenomen}
                selected={state.praenomen}
                onChange={(praenomen) => onUpdate({ praenomen })}
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
          </InSectionReveal>
        </div>
      )
    case "status":
      return (
        <>
          <FacetGroup
            title="Status"
            items={facets.status}
            selected={state.status}
            onChange={(status) => onUpdate({ status })}
            frameless
            listClassName="sm:columns-2 md:columns-3 gap-x-6"
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
        </>
      )
    case "tribe":
      return (
        <FacetGroup
          title="Tribe"
          items={facets.tribe}
          selected={state.tribe}
          onChange={(tribe) => onUpdate({ tribe })}
          frameless
          searchable
          listClassName="sm:columns-2 md:columns-3 gap-x-6"
        />
      )
    case "location":
      return (
        <FacetHierarchyGroup
          title="Location"
          items={facets.province}
          parentOf={provinceHierarchy}
          selected={state.province}
          onChange={(province) => onUpdate({ province })}
          frameless
          childNoun="sub-locations"
        />
      )
    case "events":
      return (
        <FacetGroup
          title="Life events"
          items={facets.event}
          selected={state.event}
          onChange={(event) => onUpdate({ event })}
          frameless
          listClassName="sm:columns-2 md:columns-3 gap-x-6"
        />
      )
  }
}
