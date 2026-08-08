// site/src/components/section.tsx
export function Section({
  title,
  children,
  count,
}: {
  title: string
  children: React.ReactNode
  count?: number
}) {
  return (
    <section className="mt-7">
      <h2 className="micro-label rule-hair flex items-baseline justify-between pb-1">
        {title}
        {count !== undefined && (
          <span className="micro-label-muted">{count}</span>
        )}
      </h2>
      <div className="mt-2">{children}</div>
    </section>
  )
}
