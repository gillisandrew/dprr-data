// site/src/components/search-landing.tsx
import { useState } from "react"
import { Input } from "@/components/ui/input"

interface SearchLandingProps {
  onSearch: (q: string) => void
  onBrowse: (focus: "office" | "time" | "gens") => void
}

const cards = [
  {
    key: "office" as const,
    title: "Office",
    blurb:
      "Consuls, praetors, priesthoods — browse the hierarchy of Roman offices",
  },
  {
    key: "time" as const,
    title: "Time",
    blurb: "Sweep across five centuries of attested careers on a timeline",
  },
  {
    key: "gens" as const,
    title: "Gens",
    blurb: "Explore families — Cornelii, Iunii, Claudii, and 700 more",
  },
]

export function SearchLanding({ onSearch, onBrowse }: SearchLandingProps) {
  const [q, setQ] = useState("")
  return (
    <div className="mx-auto max-w-2xl px-4 py-16 text-center">
      <h1 className="font-heading text-3xl font-bold">
        Digital Prosopography of the Roman Republic
      </h1>
      <p className="mt-2 text-muted-foreground">
        4,876 persons of the Roman Republic, 509–31 BC — offices, families,
        dates, and sources
      </p>
      <Input
        autoFocus
        value={q}
        onChange={(e) => {
          setQ(e.target.value)
          if (e.target.value.trim()) onSearch(e.target.value)
        }}
        placeholder="Search 4,876 persons…"
        className="mx-auto mt-6 h-11 max-w-md text-base"
      />
      <div className="mt-8 grid grid-cols-1 gap-3 sm:grid-cols-3">
        {cards.map((c) => (
          <button
            key={c.key}
            onClick={() => onBrowse(c.key)}
            className="rounded-lg border p-4 text-left transition-colors hover:bg-accent"
          >
            <p className="font-heading font-semibold">Browse by {c.title}</p>
            <p className="mt-1 text-xs text-muted-foreground">{c.blurb}</p>
          </button>
        ))}
      </div>
    </div>
  )
}
