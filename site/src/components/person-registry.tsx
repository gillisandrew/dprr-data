// site/src/components/person-registry.tsx
import { Link } from "@tanstack/react-router"
import { slugify } from "@/lib/slug"
import { InfoHint } from "@/components/info-hint"
import { SourceHint } from "@/components/source-hint"
import type { GlossaryTermId } from "@/lib/glossary"
import type { Person, TribeAssertionRecord } from "@/data/types"

/** Groups tribe assertions by tribe name, preserving first-seen order. */
function groupTribeAssertions(
  assertions: TribeAssertionRecord[]
): Map<string, TribeAssertionRecord[]> {
  const groups = new Map<string, TribeAssertionRecord[]>()
  for (const assertion of assertions) {
    const existing = groups.get(assertion.tribeName)
    if (existing) {
      existing.push(assertion)
    } else {
      groups.set(assertion.tribeName, [assertion])
    }
  }
  return groups
}

function Field({
  label,
  hint,
  children,
}: {
  label: string
  hint?: GlossaryTermId
  children: React.ReactNode
}) {
  return (
    <div className="min-w-0">
      <p className="micro-label-muted">
        {label}
        {hint && (
          <span className="ml-1">
            <InfoHint term={hint} />
          </span>
        )}
      </p>
      <p className="text-sm break-words">{children}</p>
    </div>
  )
}

/** Wrapping identity strip under the person header — replaces the rail's IdentityCard/LinksCard. */
export function PersonRegistry({ person }: { person: Person }) {
  const nomenSlug = person.nomen ? slugify(person.nomen) : ""
  return (
    <div className="rule-hair flex flex-wrap gap-x-8 gap-y-2 pt-2 pb-3">
      {person.praenomen && (
        <Field label="Praenomen" hint="praenomen">
          {person.praenomen}
          {person.isPraenomenUncertain && (
            <InfoHint term="uncertain" mark="?" />
          )}
        </Field>
      )}
      {person.nomen && (
        <Field label="Nomen" hint="nomen">
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
          {person.isNomenUncertain && <InfoHint term="uncertain" mark="?" />}
        </Field>
      )}
      {person.cognomen && (
        <Field label="Cognomen" hint="cognomen">
          {person.cognomen}
          {person.isCognomenUncertain && <InfoHint term="uncertain" mark="?" />}
        </Field>
      )}
      {person.otherNames && (
        <Field label="Other names" hint="other-names">
          {person.otherNames}
          {person.isOtherNamesUncertain && (
            <InfoHint term="uncertain" mark="?" />
          )}
        </Field>
      )}
      {person.filiation && (
        <Field label="Filiation" hint="filiation">
          {person.filiation}
          {person.isFiliationUncertain && (
            <InfoHint term="uncertain" mark="?" />
          )}
        </Field>
      )}
      {person.origin && (
        <Field label="Origin" hint="origin">
          {person.origin}
        </Field>
      )}
      {person.sex && <Field label="Sex">{person.sex}</Field>}
      {person.reNumber && (
        <Field label="RE" hint="re-number">
          {person.reNumber}
        </Field>
      )}
      {person.tribeAssertions.length > 0 && (
        <Field label="Tribe" hint="tribe">
          {[...groupTribeAssertions(person.tribeAssertions)].map(
            ([name, asserts], i) => (
              <span key={name}>
                {i > 0 && ", "}
                <Link
                  to="/tribes/$slug"
                  params={{ slug: slugify(name) }}
                  className="text-accent-ink hover:underline"
                >
                  {name}
                </Link>
                {asserts.some((a) => a.isUncertain) && (
                  <InfoHint term="uncertain" mark="?" />
                )}
                <SourceHint
                  sources={asserts}
                  label={`Sources for tribe ${name}`}
                />
              </span>
            )
          )}
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
