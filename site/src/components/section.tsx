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
        {title}
        {count !== undefined && (
          <span className="micro-label-muted">{count}</span>
        )}
        {hint && <InfoHint term={hint} />}
      </h2>
      <div className="mt-2">{children}</div>
    </section>
  )
}
