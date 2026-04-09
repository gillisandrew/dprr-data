// site/src/components/search-input.tsx
import { useEffect, useRef, useState } from "react"
import { Input } from "@/components/ui/input"
import { Search } from "lucide-react"

export function SearchInput({
  value,
  onChange,
}: {
  value: string
  onChange: (value: string) => void
}) {
  const [local, setLocal] = useState(value)
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>(undefined)

  useEffect(() => {
    setLocal(value)
  }, [value])

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    setLocal(v)
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => onChange(v), 200)
  }

  return (
    <div className="relative">
      <Search className="text-muted-foreground absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2" />
      <Input
        type="search"
        placeholder="Search persons..."
        value={local}
        onChange={handleChange}
        className="pl-9"
      />
    </div>
  )
}
