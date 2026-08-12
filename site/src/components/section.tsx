// site/src/components/section.tsx
import { InfoHint } from "@/components/info-hint"
import type { GlossaryTermId } from "@/lib/glossary"

export function Section({
  title,
  children,
  count,
  hint,
}: {
  title: string
  children: React.ReactNode
  count?: number
  hint?: GlossaryTermId
}) {
  return (
    <section className="mt-7">
      <h2 className="micro-label rule-hair flex items-baseline justify-between pb-1">
        <span className="flex items-baseline gap-1.5">
          {title}
          {hint && <InfoHint term={hint} />}
        </span>
        {count !== undefined && (
          <span className="micro-label-muted">{count}</span>
        )}
      </h2>
      <div className="mt-2">{children}</div>
    </section>
  )
}
