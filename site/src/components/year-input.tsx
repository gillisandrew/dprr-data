// site/src/components/year-input.tsx
import { useEffect, useState } from "react"
import { Input } from "@/components/ui/input"
import { toSignedYear, fromSignedYear, type EraLabel } from "@/lib/dates"

interface YearInputProps {
  value: number | null
  onChange: (value: number | null) => void
  placeholder?: string
  "aria-label"?: string
}

/** Unsigned year + BC/AD selector; emits signed years (BC negative). */
export function YearInput({
  value,
  onChange,
  placeholder,
  ...aria
}: YearInputProps) {
  const parts = value !== null ? fromSignedYear(value) : null
  const [text, setText] = useState(parts ? String(parts.year) : "")
  const [era, setEra] = useState<EraLabel>(parts?.era ?? "BC")

  useEffect(() => {
    const next = value !== null ? fromSignedYear(value) : null
    setText(next ? String(next.year) : "")
    if (next) setEra(next.era)
  }, [value])

  function emit(nextText: string, nextEra: EraLabel) {
    const year = Number.parseInt(nextText, 10)
    onChange(Number.isNaN(year) ? null : toSignedYear(year, nextEra))
  }

  return (
    <span className="inline-flex items-center gap-1">
      <Input
        type="number"
        min={1}
        inputMode="numeric"
        value={text}
        placeholder={placeholder}
        onChange={(e) => {
          setText(e.target.value)
          emit(e.target.value, era)
        }}
        className="h-7 w-20 text-xs"
        {...aria}
      />
      <select
        value={era}
        onChange={(e) => {
          const next = e.target.value as EraLabel
          setEra(next)
          emit(text, next)
        }}
        className="h-7 rounded-md border bg-transparent px-1 text-xs"
        aria-label="era"
      >
        <option>BC</option>
        <option>AD</option>
      </select>
    </span>
  )
}
