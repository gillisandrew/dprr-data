// site/src/components/facet-combobox.tsx
import { useState } from "react"
import { X } from "lucide-react"
import { Input } from "@/components/ui/input"
import type { FacetValue } from "@/data/types"

interface FacetComboboxProps {
  label: string
  values: FacetValue[]
  selected: string[]
  onChange: (selected: string[]) => void
  placeholder?: string
}

export function FacetCombobox({
  label,
  values,
  selected,
  onChange,
  placeholder,
}: FacetComboboxProps) {
  const [query, setQuery] = useState("")
  const suggestions = query.trim()
    ? values
        .filter(
          (v) =>
            !selected.includes(v.value) &&
            v.value.toLowerCase().includes(query.trim().toLowerCase())
        )
        .slice(0, 8)
    : []

  return (
    <div className="space-y-1">
      <p className="micro-label-muted">{label}</p>
      {selected.length > 0 && (
        <div className="flex flex-wrap items-center gap-2">
          {selected.map((s) => (
            <button
              key={s}
              onClick={() => onChange(selected.filter((v) => v !== s))}
              className="rule-hair inline-flex items-center gap-1 pb-0.5 text-xs text-foreground hover:text-primary"
            >
              {s}
              <X className="h-3 w-3 text-muted-foreground" />
            </button>
          ))}
        </div>
      )}
      <Input
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={placeholder ?? `Search ${label.toLowerCase()}…`}
        className="h-7 text-xs"
      />
      {suggestions.length > 0 && (
        <ul className="rounded-md border bg-background text-xs shadow-sm">
          {suggestions.map((v) => (
            <li key={v.value}>
              <button
                className="flex w-full items-baseline justify-between px-2 py-1 text-left hover:bg-accent"
                onClick={() => {
                  onChange([...selected, v.value])
                  setQuery("")
                }}
              >
                <span className="min-w-0 truncate">{v.value}</span>
                <span className="ml-2 text-muted-foreground">{v.count}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
