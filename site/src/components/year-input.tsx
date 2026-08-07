// site/src/components/year-input.tsx
import { useEffect, useState } from "react"
import { Input } from "@/components/ui/input"
import { fromSignedYear } from "@/lib/dates"

interface YearInputProps {
  value: number | null
  onChange: (value: number | null) => void
  placeholder?: string
  "aria-label"?: string
}

/**
 * Unsigned year input; every date in the dataset is BC, so the value it
 * emits is always a non-positive signed year (never AD). Displays a static
 * "BC" suffix rather than an era selector. Edits are held in local draft
 * text and only committed via `onChange` on blur or Enter — not on every
 * keystroke.
 */
export function YearInput({
  value,
  onChange,
  placeholder,
  ...aria
}: YearInputProps) {
  const [text, setText] = useState(
    value !== null ? String(fromSignedYear(value).year) : ""
  )

  useEffect(() => {
    setText(value !== null ? String(fromSignedYear(value).year) : "")
  }, [value])

  function commit() {
    if (text === "") {
      onChange(null)
      return
    }
    const n = Number.parseFloat(text)
    onChange(
      Number.isNaN(n) ? null : -Math.max(1, Math.floor(Math.abs(n)) || 1)
    )
  }

  return (
    <span className="inline-flex items-center gap-1">
      <Input
        type="number"
        min={1}
        inputMode="numeric"
        value={text}
        placeholder={placeholder}
        onChange={(e) => setText(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") e.currentTarget.blur()
        }}
        className="h-7 w-20 text-xs"
        {...aria}
      />
      <span className="text-xs text-muted-foreground">BC</span>
    </span>
  )
}
