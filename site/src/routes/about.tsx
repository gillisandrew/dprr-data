// site/src/routes/about.tsx
import { createFileRoute, Link } from "@tanstack/react-router"
import { GLOSSARY } from "@/lib/glossary"

const REPO_URL = "https://github.com/gillisandrew/dprr-data"

export const Route = createFileRoute("/about")({
  head: () => {
    const title = "About — DPRR"
    const desc =
      "An unofficial interface to the Digital Prosopography of the Roman Republic dataset (King's College London), CC BY-NC 4.0"
    return {
      meta: [
        { title },
        { name: "description", content: desc },
        { property: "og:title", content: title },
        { property: "og:description", content: desc },
      ],
    }
  },
  component: AboutPage,
})

function ExternalLink({ href, children }: { href: string; children: string }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="text-accent-ink hover:underline"
    >
      {children}
    </a>
  )
}

function AboutPage() {
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <header className="rule-lead pb-3">
        <h1 className="font-heading text-3xl font-bold">About this site</h1>
      </header>

      <div className="mt-6 space-y-8 text-sm leading-relaxed">
        <section>
          <h2 className="mb-2 font-heading text-xl font-semibold">
            What this is
          </h2>
          <p>
            A read-only interface to the prosopographical dataset of the Digital
            Prosopography of the Roman Republic (DPRR): the persons,
            magistracies, priesthoods, family relationships, and scholarly notes
            of the Roman Republic, drawn chiefly from Broughton&apos;s{" "}
            <i>Magistrates of the Roman Republic</i> and related scholarship.
            The site is statically built from the RDF (Turtle) files in{" "}
            <ExternalLink href={REPO_URL}>this repository</ExternalLink>, which
            reshards the official DPRR RDF dump into one file per person.
          </p>
        </section>

        <section>
          <h2 className="mb-2 font-heading text-xl font-semibold">
            Not the official project
          </h2>
          <p>
            This site is an independent, unofficial rendering of the data. It is
            not affiliated with, maintained by, or endorsed by the DPRR project
            or King&apos;s College London. The official project lives at{" "}
            <ExternalLink href="https://romanrepublic.ac.uk">
              romanrepublic.ac.uk
            </ExternalLink>
            . Any errors introduced by this site&apos;s data curation or
            rendering are ours, not the DPRR project&apos;s — each record page
            links to its source file and an issue tracker so mistakes can be
            reported.
          </p>
        </section>

        <section>
          <h2 className="mb-2 font-heading text-xl font-semibold">
            Attribution &amp; license
          </h2>
          <p>
            Source data: Digital Prosopography of the Roman Republic,
            King&apos;s College London, licensed under{" "}
            <ExternalLink href="https://creativecommons.org/licenses/by-nc/4.0/">
              CC BY-NC 4.0
            </ExternalLink>
            . This dataset and site are distributed under the same license.
            Changes were made to the original data: the RDF dump was resharded
            and lightly normalized, as documented in{" "}
            <ExternalLink href={`${REPO_URL}#changes-from-upstream`}>
              Changes from upstream
            </ExternalLink>
            , and the site adds its own curation (for example the mapping of
            free-text province strings to canonical provinces).
          </p>
          <p className="mt-3">
            The full dataset can be queried with SPARQL on the{" "}
            <Link to="/sparql" className="text-accent-ink hover:underline">
              query page
            </Link>{" "}
            (runs entirely in your browser) or downloaded as a{" "}
            <a
              href={`${import.meta.env.BASE_URL}dump/dprr.nt.gz`}
              className="text-accent-ink hover:underline"
            >
              N-Triples dump
            </a>
            .
          </p>
        </section>

        <section>
          <h2 className="mb-2 font-heading text-xl font-semibold">Glossary</h2>
          <dl className="space-y-3">
            {Object.entries(GLOSSARY).map(([id, entry]) => (
              <div key={id}>
                <dt className="text-sm font-medium">{entry.label}</dt>
                <dd className="text-sm text-muted-foreground">{entry.text}</dd>
              </div>
            ))}
          </dl>
        </section>
      </div>
    </div>
  )
}
