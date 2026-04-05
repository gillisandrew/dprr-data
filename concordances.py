"""Generate concordance files from Wikidata for DPRR persons.

Queries Wikidata's SPARQL endpoint for items with DPRR IDs (P6863) and
extracts external identifiers linking to authority files, scholarly
databases, and numismatic collections.

Output: concordances/{system}.ttl — one file per target system, containing
skos:exactMatch triples from DPRR person URIs to external URIs.

Usage:
    python concordances.py [output-dir]
    python concordances.py .              # default: current directory
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "linked-past/1.0 (https://github.com/gillisandrew/dprr-data)"
DPRR_ENTITY = "http://romanrepublic.ac.uk/rdf/entity/Person/"

PREFIXES = """\
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix entity: <http://romanrepublic.ac.uk/rdf/entity/> .
"""


@dataclass
class Concordance:
    """A target system to extract from Wikidata."""

    name: str  # filename stem
    label: str  # human-readable name
    wikidata_prop: str  # Wikidata property ID (e.g., "P214")
    uri_template: str | None  # template to build URI from ID value, or None for Wikidata items
    predicate: str = "skos:exactMatch"  # RDF predicate to use


# Scholarly and authority-file concordances, ordered by relevance.
# uri_template uses {id} as placeholder for the property value.
CONCORDANCES = [
    # Wikidata itself
    Concordance(
        name="wikidata",
        label="Wikidata",
        wikidata_prop="P6863",  # We query by DPRR ID, emit Wikidata item URI
        uri_template=None,  # special case: emit the Wikidata item URI
        predicate="owl:sameAs",
    ),
    # Authority files
    Concordance(
        name="viaf",
        label="Virtual International Authority File (VIAF)",
        wikidata_prop="P214",
        uri_template="https://viaf.org/viaf/{id}",
    ),
    Concordance(
        name="gnd",
        label="Gemeinsame Normdatei (GND)",
        wikidata_prop="P227",
        uri_template="https://d-nb.info/gnd/{id}",
    ),
    Concordance(
        name="loc",
        label="Library of Congress Name Authority File",
        wikidata_prop="P244",
        uri_template="https://id.loc.gov/authorities/names/{id}",
    ),
    # Numismatics
    Concordance(
        name="nomisma",
        label="Nomisma.org",
        wikidata_prop="P2950",
        uri_template="http://nomisma.org/id/{id}",
    ),
    Concordance(
        name="munzkabinett",
        label="Münzkabinett, Staatliche Museen zu Berlin",
        wikidata_prop="P13030",
        uri_template="https://ikmk.smb.museum/ndp/person/{id}",
    ),
    # Classical scholarship
    Concordance(
        name="topostext",
        label="ToposText",
        wikidata_prop="P8069",
        uri_template="https://topostext.org/people/{id}",
    ),
    Concordance(
        name="perseus",
        label="Perseus Digital Library",
        wikidata_prop="P7041",
        uri_template="https://catalog.perseus.org/catalog/{id}",
    ),
    Concordance(
        name="phi-latin",
        label="PHI Latin Texts",
        wikidata_prop="P6941",
        uri_template="https://latin.packhum.org/author/{id}",
    ),
    Concordance(
        name="ocd",
        label="Oxford Classical Dictionary",
        wikidata_prop="P9106",
        uri_template="https://doi.org/{id}",
    ),
    # Epigraphy and papyrology
    Concordance(
        name="trismegistos",
        label="Trismegistos Authors",
        wikidata_prop="P11252",
        uri_template="https://www.trismegistos.org/author/{id}",
    ),
    # Museums
    Concordance(
        name="british-museum",
        label="British Museum",
        wikidata_prop="P1711",
        uri_template="https://www.britishmuseum.org/collection/term/BIOG{id}",
    ),
]


def query_wikidata(sparql: str) -> list[dict]:
    """Execute a SPARQL query against the Wikidata endpoint."""
    url = f"{WIKIDATA_SPARQL}?format=json&query={urllib.parse.quote(sparql)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read())
    return data["results"]["bindings"]


def generate_concordance(conc: Concordance) -> tuple[str, list[tuple[str, str]]]:
    """Query Wikidata and return (header_comment, [(dprr_id, target_uri)])."""
    import urllib.parse

    if conc.name == "wikidata":
        # Special case: get DPRR ID → Wikidata item mapping
        sparql = """
        SELECT ?dprrId ?item WHERE {
          ?item wdt:P6863 ?dprrId .
        }
        ORDER BY xsd:integer(?dprrId)
        """
        results = query_wikidata(sparql)
        links = []
        for r in results:
            dprr_id = r["dprrId"]["value"]
            wd_uri = r["item"]["value"]
            links.append((dprr_id, wd_uri))
        return f"# DPRR person ↔ Wikidata item concordance\n# Total links: {len(links)}", links

    # Standard case: get DPRR ID + external ID
    sparql = f"""
    SELECT ?dprrId ?extId WHERE {{
      ?item wdt:P6863 ?dprrId ;
            wdt:{conc.wikidata_prop} ?extId .
    }}
    ORDER BY xsd:integer(?dprrId)
    """
    results = query_wikidata(sparql)
    links = []
    for r in results:
        dprr_id = r["dprrId"]["value"]
        ext_id = r["extId"]["value"]
        uri = conc.uri_template.replace("{id}", ext_id)
        links.append((dprr_id, uri))

    header = (
        f"# DPRR person ↔ {conc.label} concordance\n"
        f"# Wikidata property: {conc.wikidata_prop}\n"
        f"# Total links: {len(links)}"
    )
    return header, links


def write_concordance(conc: Concordance, output_dir: Path) -> int:
    """Generate and write a concordance file. Returns link count."""
    header, links = generate_concordance(conc)
    if not links:
        print(f"  {conc.name}: 0 links (skipped)")
        return 0

    lines = [PREFIXES, "", header, f"# Generated from Wikidata: {time.strftime('%Y-%m-%d')}", ""]
    for dprr_id, target_uri in links:
        lines.append(
            f"<{DPRR_ENTITY}{dprr_id}> {conc.predicate} <{target_uri}> ."
        )
    lines.append("")

    content = "\n".join(lines)
    path = output_dir / "concordances" / f"{conc.name}.ttl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"  {conc.name}: {len(links)} links")
    return len(links)


def main(output_dir: str = "."):
    output_dir = Path(output_dir)
    total = 0

    print("Generating concordances from Wikidata...")
    for conc in CONCORDANCES:
        try:
            count = write_concordance(conc, output_dir)
            total += count
        except Exception as e:
            print(f"  {conc.name}: ERROR — {e}")
        # Be polite to the Wikidata endpoint
        time.sleep(1)

    print(f"\nTotal: {total} links across {len(CONCORDANCES)} systems")


if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    main(output_dir)
