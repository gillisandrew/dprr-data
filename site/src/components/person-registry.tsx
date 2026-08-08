// site/src/components/person-registry.tsx
import { Link } from "@tanstack/react-router"
import { slugify } from "@/lib/slug"
import type { Person } from "@/data/types"

function Field({
  label,
  children,
}: {
  label: string
  children: React.ReactNode
}) {
  return (
    <div className="min-w-0">
      <p className="micro-label-muted">{label}</p>
      <p className="text-sm break-words">{children}</p>
    </div>
  )
}

/** Wrapping identity strip under the person header — replaces the rail's IdentityCard/LinksCard. */
export function PersonRegistry({ person }: { person: Person }) {
  const nomenSlug = person.nomen ? slugify(person.nomen) : ""
  return (
    <div className="rule-hair flex flex-wrap gap-x-8 gap-y-2 pt-2 pb-3">
      {person.praenomen && <Field label="Praenomen">{person.praenomen}</Field>}
      {person.nomen && (
        <Field label="Nomen">
          {nomenSlug ? (
            <Link
              to="/gentes/$slug"
              params={{ slug: nomenSlug }}
              className="text-accent-ink hover:underline"
            >
              {person.nomen}
            </Link>
          ) : (
            person.nomen
          )}
        </Field>
      )}
      {person.cognomen && <Field label="Cognomen">{person.cognomen}</Field>}
      {person.filiation && <Field label="Filiation">{person.filiation}</Field>}
      {person.sex && <Field label="Sex">{person.sex}</Field>}
      {person.reNumber && <Field label="RE">{person.reNumber}</Field>}
      {person.tribes.length > 0 && (
        <Field label="Tribe">
          {person.tribes.map((t, i) => (
            <span key={t}>
              {i > 0 && ", "}
              <Link
                to="/tribes/$slug"
                params={{ slug: slugify(t) }}
                className="text-accent-ink hover:underline"
              >
                {t}
              </Link>
            </span>
          ))}
        </Field>
      )}
      <Field label="DPRR ID">
        <span className="font-mono text-xs">{person.id}</span>
      </Field>
      {person.concordances.length > 0 && (
        <Field label="Links">
          {[
            ...new Map(person.concordances.map((c) => [c.system, c])).values(),
          ].map((c, i) => (
            <span key={c.system}>
              {i > 0 && " · "}
              <a
                href={c.uri}
                target="_blank"
                rel="noopener noreferrer"
                className="text-accent-ink capitalize hover:underline"
              >
                {c.system}
              </a>
            </span>
          ))}
        </Field>
      )}
    </div>
  )
}
