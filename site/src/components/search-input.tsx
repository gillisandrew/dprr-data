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
  // Latest keystroke the debounce hasn't delivered yet; null when settled.
  const pendingRef = useRef<string | null>(null)
  const onChangeRef = useRef(onChange)
  // Assigned after commit, not during render: render must stay pure because
  // React may replay or discard it. The unmount cleanup below runs after the
  // last commit's effects, so it always reads the current onChange.
  useEffect(() => {
    onChangeRef.current = onChange
  })

  useEffect(() => {
    setLocal(value)
    // An external value change (chip removal, clear-all, navigation)
    // supersedes any in-flight keystroke — don't flush stale text over it.
    pendingRef.current = null
  }, [value])

  // Flush (not drop) a pending keystroke on unmount, so a query typed just
  // before navigating away still lands in the URL.
  useEffect(
    () => () => {
      clearTimeout(timeoutRef.current)
      if (pendingRef.current !== null) onChangeRef.current(pendingRef.current)
    },
    []
  )

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    setLocal(v)
    pendingRef.current = v
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => {
      pendingRef.current = null
      onChange(v)
    }, 200)
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
