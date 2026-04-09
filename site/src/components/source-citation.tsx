// site/src/components/source-citation.tsx
export function SourceCitation({
  name,
  className,
}: {
  name: string
  className?: string
}) {
  if (!name) return null
  return <cite className={className}>{name}</cite>
}
