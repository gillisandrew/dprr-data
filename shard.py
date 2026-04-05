"""Shard DPRR RDF dump into per-person Turtle files with canonical output.

Reads the monolithic dprr.ttl and produces:
  ontology.ttl          — OWL/RDFS schema definitions
  reference/offices.ttl — Office entities
  reference/provinces.ttl
  reference/praenomina.ttl
  reference/tribes.ttl
  reference/relationships.ttl
  reference/sources.ttl
  reference/misc.ttl    — Sex, Status, DateType, NoteType
  persons/NN/NNNN.ttl   — One file per person with all related assertions

Uses pyoxigraph for parsing and RDFC 1.0 canonicalization to ensure
deterministic output. Git only sees real data changes between runs.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyoxigraph import (
    Dataset,
    CanonicalizationAlgorithm,
    DefaultGraph,
    NamedNode,
    Quad,
    RdfFormat,
    Store,
    serialize,
)

VOCAB = "http://romanrepublic.ac.uk/rdf/ontology#"
ENTITY = "http://romanrepublic.ac.uk/rdf/entity/"
RDF_TYPE = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

# Predicates to strip — implementation details with no semantic value.
# hasID is always the numeric suffix of the entity URI (100% redundant).
STRIP_PREDICATES = frozenset({
    NamedNode(VOCAB + "hasID"),
})

PREFIXES = """\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix entity: <http://romanrepublic.ac.uk/rdf/entity/> .
"""

# Reference types grouped into files
REFERENCE_FILES = {
    "offices": ["Office"],
    "provinces": ["Province"],
    "praenomina": ["Praenomen"],
    "tribes": ["Tribe"],
    "relationships": ["Relationship", "RelationshipInverse"],
    "sources": ["SecondarySource", "PrimarySource"],
    "misc": ["Sex", "Status", "DateType", "NoteType"],
}

# Properties that link an assertion to a person
PERSON_LINK_PREDS = [
    NamedNode(VOCAB + "isAboutPerson"),
    NamedNode(VOCAB + "hasPerson"),
]

# Properties on a person/assertion that point to notes (outbound links)
NOTE_PREDS = [
    NamedNode(VOCAB + "hasPersonNote"),
    NamedNode(VOCAB + "hasPostAssertionNote"),
    NamedNode(VOCAB + "hasStatusAssertionNote"),
]

# Property on a note/reference that points back to an assertion
FOR_ASSERTION = NamedNode(VOCAB + "forAssertion")


def collect_person_triples(store: Store, person: NamedNode) -> list[Quad]:
    """Collect all triples for a person and their related entities.

    Follows the DPRR graph structure:
      Person → assertions (via isAboutPerson/hasPerson on assertion)
      Person → PersonNotes (via hasPersonNote on person)
      Assertion → AssertionNotes (via hasPostAssertionNote etc. on assertion)
      Note/Reference → forAssertion → Assertion (reverse link, also collected)
    """
    collected: set[str] = set()
    triples: list[Quad] = []

    def collect_subject(subj: NamedNode) -> None:
        key = str(subj)
        if key in collected:
            return
        collected.add(key)
        for q in store.quads_for_pattern(subj, None, None, None):
            if q.predicate not in STRIP_PREDICATES:
                triples.append(q)

    # The person itself
    collect_subject(person)

    # Assertions about this person (assertion → isAboutPerson → person)
    assertions: set[NamedNode] = set()
    for pred in PERSON_LINK_PREDS:
        for q in store.quads_for_pattern(None, pred, person, None):
            assertions.add(q.subject)
            collect_subject(q.subject)

    # Notes linked from person (person → hasPersonNote → note)
    for pred in NOTE_PREDS:
        for q in store.quads_for_pattern(person, pred, None, None):
            collect_subject(q.object)

    # Notes/references linked from assertions (assertion → hasPostAssertionNote → note)
    for assertion in assertions:
        for pred in NOTE_PREDS:
            for q in store.quads_for_pattern(assertion, pred, None, None):
                collect_subject(q.object)
        # Also pick up PostAssertionProvince (assertion → hasPostAssertionProvince)
        for q in store.quads_for_pattern(assertion, None, None, None):
            obj_str = str(q.object)
            if "/entity/" in obj_str and obj_str not in collected:
                # Follow links to PostAssertionProvince and similar
                obj_type = None
                for tq in store.quads_for_pattern(q.object, RDF_TYPE, None, None):
                    obj_type = str(tq.object)
                if obj_type and "Province" not in obj_type and "Office" not in obj_type:
                    collect_subject(q.object)

    # References that point back to our assertions (ref → forAssertion → assertion)
    for assertion in assertions:
        for q in store.quads_for_pattern(None, FOR_ASSERTION, assertion, None):
            collect_subject(q.subject)

    return triples


# Prefix compaction applied to serialized Turtle.
# Order matters: longer prefixes first to avoid partial matches.
_PREFIX_REPLACEMENTS = [
    (b"<http://romanrepublic.ac.uk/rdf/entity/", b"entity:"),
    (b"<http://romanrepublic.ac.uk/rdf/ontology#", b"dprr:"),
    (b"<http://www.w3.org/1999/02/22-rdf-syntax-ns#", b"rdf:"),
    (b"<http://www.w3.org/2000/01/rdf-schema#", b"rdfs:"),
    (b"<http://www.w3.org/2002/07/owl#", b"owl:"),
]


def _compact_uris(ttl: bytes) -> bytes:
    """Replace full URIs with prefixed names where possible."""
    for full, prefix in _PREFIX_REPLACEMENTS:
        # <http://...#Foo> or <http://...#foo> → prefix:Foo or prefix:foo
        # Need to handle the closing > which becomes absent in prefixed form
        result = bytearray()
        i = 0
        while i < len(ttl):
            if ttl[i : i + len(full)] == full:
                # Find the closing >
                end = ttl.index(b">", i + len(full))
                local = ttl[i + len(full) : end]
                # Turtle local names can't contain certain chars
                if b" " not in local and b"/" not in local and b"(" not in local:
                    result.extend(prefix)
                    result.extend(local)
                    i = end + 1
                    continue
            result.append(ttl[i])
            i += 1
        ttl = bytes(result)
    return ttl


def serialize_ttl(quads: list[Quad]) -> bytes:
    """Canonicalize and serialize quads to deterministic Turtle with prefixes.

    Uses RDFC 1.0 for blank node canonicalization, then serializes to
    sorted N-Quads and re-parses into Turtle for grouped subject output.
    The N-Quads sort ensures byte-stable output across runs.
    """
    if not quads:
        return b""
    # Canonicalize for deterministic blank node labels
    ds = Dataset()
    for q in quads:
        ds.add(Quad(q.subject, q.predicate, q.object, DefaultGraph()))
    ds.canonicalize(CanonicalizationAlgorithm.RDFC_1_0)
    # Serialize to N-Quads (one line per triple), sort for determinism,
    # then re-parse into a fresh store to get grouped Turtle output
    nq = serialize(ds, format=RdfFormat.N_QUADS)
    lines = sorted(nq.strip().split(b"\n"))
    sorted_nq = b"\n".join(lines) + b"\n"
    # Re-parse sorted N-Quads into Turtle
    sorted_store = Store()
    sorted_store.load(sorted_nq, format=RdfFormat.N_QUADS)
    body = serialize(
        sorted_store.quads_for_pattern(None, None, None, None),
        format=RdfFormat.TURTLE,
    )
    body = _compact_uris(body)
    return PREFIXES.encode() + b"\n" + body


def write_file(path: Path, content: bytes) -> bool:
    """Write file only if content changed. Returns True if written."""
    if path.exists() and path.read_bytes() == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


def main(input_path: str, output_dir: str):
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    print(f"Loading {input_path}...")
    store = Store()
    store.bulk_load(path=str(input_path), format=RdfFormat.TURTLE)
    print(f"Loaded {len(store):,} triples")

    # Index: subject → type name
    subject_type: dict[str, str] = {}
    for q in store.quads_for_pattern(None, RDF_TYPE, None, None):
        cls = str(q.object)
        if VOCAB in cls:
            short = cls.split("#")[-1].rstrip(">")
            subject_type[str(q.subject)] = short

    # --- Ontology ---
    ontology_quads = []
    owl_types = {
        "http://www.w3.org/2002/07/owl#Class",
        "http://www.w3.org/2002/07/owl#ObjectProperty",
        "http://www.w3.org/2002/07/owl#DatatypeProperty",
        "http://www.w3.org/2002/07/owl#AnnotationProperty",
        "http://www.w3.org/2002/07/owl#Ontology",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
    }
    ontology_subjects = set()
    for owl_type in owl_types:
        for q in store.quads_for_pattern(None, RDF_TYPE, NamedNode(owl_type), None):
            ontology_subjects.add(q.subject)
    for subj in ontology_subjects:
        for q in store.quads_for_pattern(subj, None, None, None):
            if q.predicate not in STRIP_PREDICATES:
                ontology_quads.append(q)

    content = serialize_ttl(ontology_quads)
    changed = write_file(output_dir / "ontology.ttl", content)
    print(f"  ontology.ttl: {len(ontology_quads)} triples {'(updated)' if changed else '(unchanged)'}")

    # --- Reference data ---
    all_ref_types = set()
    for filename, type_names in REFERENCE_FILES.items():
        ref_quads = []
        for type_name in type_names:
            all_ref_types.add(type_name)
            type_uri = NamedNode(VOCAB + type_name)
            for q in store.quads_for_pattern(None, RDF_TYPE, type_uri, None):
                for rq in store.quads_for_pattern(q.subject, None, None, None):
                    if rq.predicate not in STRIP_PREDICATES:
                        ref_quads.append(rq)

        content = serialize_ttl(ref_quads)
        path = output_dir / "reference" / f"{filename}.ttl"
        changed = write_file(path, content)
        print(f"  reference/{filename}.ttl: {len(ref_quads)} triples {'(updated)' if changed else '(unchanged)'}")

    # --- Persons ---
    person_type = NamedNode(VOCAB + "Person")
    persons = []
    for q in store.quads_for_pattern(None, RDF_TYPE, person_type, None):
        persons.append(q.subject)
    persons.sort(key=lambda n: int(str(n).split("/")[-1].rstrip(">")))

    # Build DPRR ID index (Person → "IUNI0001" etc.)
    dprr_id_pred = NamedNode(VOCAB + "hasDprrID")
    person_dprr_id: dict[str, str] = {}
    for q in store.quads_for_pattern(None, dprr_id_pred, None, None):
        person_dprr_id[str(q.subject)] = str(q.object).strip('"')

    written = 0
    unchanged = 0
    total_person_triples = 0
    for person in persons:
        dprr_id = person_dprr_id.get(str(person))
        if not dprr_id:
            person_id = str(person).split("/")[-1].rstrip(">")
            dprr_id = f"UNKNOWN{person_id}"
        nomen = dprr_id[:4]
        filename = f"{dprr_id}.ttl"

        quads = collect_person_triples(store, person)
        total_person_triples += len(quads)

        content = serialize_ttl(quads)
        path = output_dir / "persons" / nomen / filename
        if write_file(path, content):
            written += 1
        else:
            unchanged += 1

    print(f"  persons/: {len(persons)} files, {total_person_triples:,} triples ({written} updated, {unchanged} unchanged)")

    # --- Summary ---
    accounted = len(ontology_quads) + total_person_triples
    for filename, type_names in REFERENCE_FILES.items():
        for type_name in type_names:
            type_uri = NamedNode(VOCAB + type_name)
            for q in store.quads_for_pattern(None, RDF_TYPE, type_uri, None):
                accounted += len(list(store.quads_for_pattern(q.subject, None, None, None)))

    print(f"\nTotal: {len(store):,} triples, {accounted:,} accounted for")
    if accounted < len(store):
        print(f"  {len(store) - accounted:,} triples not assigned (may be duplicates across shards)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python shard.py <input.ttl> <output-dir>")
        print("  e.g.: python shard.py /path/to/dprr.ttl .")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
