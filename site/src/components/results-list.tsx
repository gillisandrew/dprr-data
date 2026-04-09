// site/src/components/results-list.tsx
import { useEffect, useState } from "react"
import { PersonCard } from "./person-card"
import type { PersonSummary } from "@/data/types"

const PAGE_SIZE = 50

export function ResultsList({ results }: { results: PersonSummary[] }) {
  const [page, setPage] = useState(0)

  // Reset to first page when results change (new search/filter)
  useEffect(() => {
    setPage(0)
  }, [results])

  const totalPages = Math.ceil(results.length / PAGE_SIZE)
  const visible = results.slice(0, (page + 1) * PAGE_SIZE)

  return (
    <div>
      <p className="text-muted-foreground mb-3 text-sm">
        {results.length.toLocaleString()} result
        {results.length !== 1 && "s"}
      </p>
      <div className="space-y-2">
        {visible.map((person) => (
          <PersonCard key={person.id} person={person} />
        ))}
      </div>
      {page + 1 < totalPages && (
        <button
          onClick={() => setPage((p) => p + 1)}
          className="text-primary mt-4 text-sm hover:underline"
        >
          Show more ({results.length - visible.length} remaining)
        </button>
      )}
    </div>
  )
}
