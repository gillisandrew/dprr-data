// site/src/components/search-input.tsx
import { useEffect, useRef, useState } from "react"
import { Input } from "@/components/ui/input"
import { Search } from "lucide-react"

export function SearchInput({
  value,
  onChange,
  autoFocus,
}: {
  value: string
  onChange: (value: string) => void
  autoFocus?: boolean
}) {
  const [local, setLocal] = useState(value)
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>(undefined)

  useEffect(() => {
    setLocal(value)
  }, [value])

  // Clear pending debounce on unmount
  useEffect(() => () => clearTimeout(timeoutRef.current), [])

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    setLocal(v)
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => onChange(v), 200)
  }

  return (
    <div className="relative">
      <Search className="absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
      <Input
        type="search"
        placeholder="Search persons..."
        value={local}
        onChange={handleChange}
        autoFocus={autoFocus}
        className="pl-9"
      />
    </div>
  )
}
