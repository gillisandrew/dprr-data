"""Patch ontology.ttl with clearer rdfs:comment descriptions.

Run after shard.py to update the class descriptions in-place.
"""

from pathlib import Path
from pyoxigraph import Store, RdfFormat, NamedNode, Literal, Quad, DefaultGraph, serialize

VOCAB = "http://romanrepublic.ac.uk/rdf/ontology#"
COMMENT = NamedNode("http://www.w3.org/2000/01/rdf-schema#comment")

# New descriptions: concise, focused on what the type represents and
# how it relates to other types. Avoids implementation details.
NEW_COMMENTS = {
    "Person": "A historical person from the Roman Republic (c. 509–31 BC). Central entity: assertions, notes, and relationships attach to persons.",
    "PostAssertion": "An assertion that a person held a political or religious office. Links a Person to an Office with a date range and scholarly sources.",
    "PostAssertionNote": "A scholarly note attached to a PostAssertion — typically source citations or editorial commentary from Broughton's MRR.",
    "PostAssertionProvince": "Links a PostAssertion to a Province, recording the territorial assignment of an office-holding.",
    "RelationshipAssertion": "An assertion of a family or social relationship between two persons (e.g., father of, married to). Links one Person (via isAboutPerson) to another (via hasRelatedPerson) through a Relationship type.",
    "RelationshipAssertionReference": "A secondary source citation supporting a RelationshipAssertion.",
    "StatusAssertion": "An assertion that a person held a particular social or political status (senator or eques Romanus).",
    "StatusAssertionNote": "A scholarly note attached to a StatusAssertion.",
    "StatusAssertionProvince": "Links a StatusAssertion to a Province.",
    "TribeAssertion": "An assertion that a person belonged to a particular Roman voting tribe.",
    "DateInformation": "A dated biographical event for a person — birth, death, attestation, exile, proscription, etc. Classified by DateType.",
    "PersonNote": "A scholarly note attached directly to a Person (not to a specific assertion).",
    "PrimarySource": "An ancient textual source (e.g., Livy, Cicero). Only 2 instances — most primary source data is recorded as free text in PrimarySourceReference.",
    "PrimarySourceReference": "A citation of an ancient source attached to a specific PostAssertion (e.g., 'Liv. 25.14.4'). Each reference belongs to exactly one assertion.",
    "SecondarySource": "A modern scholarly work used as a source for DPRR data (e.g., Broughton's MRR, Zmeskal 2009). Shared across many assertions.",
    "Office": "A political or religious office in the Roman state (e.g., consul, praetor, tribune). Offices form a hierarchy via hasParent.",
    "Province": "A Roman province or territorial jurisdiction (e.g., Sicilia, Africa, Asia). Provinces form a hierarchy via hasParent.",
    "Praenomen": "A Roman first name (e.g., Lucius, Marcus, Gaius). 45 instances.",
    "Tribe": "A Roman voting tribe (e.g., Cornelia, Palatina). 37 instances.",
    "Relationship": "A type of family or social relationship (e.g., father of, married to, cousin of). 44 types. Inverse pairs are declared with owl:inverseOf.",
    "Sex": "Sex classification: Male or Female.",
    "Status": "A social or political status: senator or eques Romanus.",
    "DateType": "A classification of biographical dates: birth, death, violent death, attestation, proscription, exile, expulsion from Senate, extradition, recall, restoration.",
    "NoteType": "A classification scheme for scholarly notes, drawn from Broughton and Rüpke's categorization of source material.",
    "Source": "A primary or secondary source that provided information for DPRR. See PrimarySource and SecondarySource.",
    "Assertion": "Base type for all scholarly assertions about a person. Subtypes: PostAssertion, StatusAssertion, TribeAssertion, RelationshipAssertion, DateInformation.",
    "AssertionWithDateRange": "An assertion that includes a date range (hasDateStart/hasDateEnd). Subtypes: PostAssertion, StatusAssertion.",
    "Note": "Base type for scholarly notes. Subtypes: PostAssertionNote, StatusAssertionNote, PersonNote.",
    "NoteContainer": "Base type for entities that carry note text (hasNoteText). Subtypes: Note, PrimarySourceReference.",
    "NoteForProvince": "Base type for province linkages. Subtypes: PostAssertionProvince, StatusAssertionProvince.",
    "AuthorityList": "Base type for controlled vocabulary lists (offices, provinces, etc.).",
    "AuthorityWithAbbreviation": "An authority list entry with an abbreviated form (e.g., 'cos.' for consul).",
    "AuthorityWithDescription": "An authority list entry with a prose description.",
    "HierarchicalAuthority": "An authority list with parent-child hierarchy (offices, provinces).",
    "ThingWithID": "Base type for all DPRR entities.",
    "ThingWithName": "Base type for entities with a name property.",
    "ThingWithExtraInfo": "Base type for entities that may carry a hasExtraInfo annotation.",
}


def main():
    path = Path("ontology.ttl")
    s = Store()
    s.bulk_load(path=str(path), format=RdfFormat.TURTLE)

    # Remove old comments for DPRR classes, add new ones
    to_remove = []
    to_add = []
    for cls_name, new_comment in NEW_COMMENTS.items():
        cls = NamedNode(VOCAB + cls_name)
        # Remove existing comments
        for q in s.quads_for_pattern(cls, COMMENT, None, None):
            to_remove.append(q)
        # Add new comment
        to_add.append(Quad(cls, COMMENT, Literal(new_comment), DefaultGraph()))

    for q in to_remove:
        s.remove(q)
    for q in to_add:
        s.add(q)

    # Serialize
    nq = serialize(s.quads_for_pattern(None, None, None, None), format=RdfFormat.N_QUADS)
    lines = sorted(nq.strip().split(b"\n"))
    sorted_nq = b"\n".join(lines) + b"\n"
    sorted_store = Store()
    sorted_store.load(sorted_nq, format=RdfFormat.N_QUADS)
    body = serialize(
        sorted_store.quads_for_pattern(None, None, None, None),
        format=RdfFormat.TURTLE,
    )

    # Compact prefixes
    PREFIXES = b"""\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix entity: <http://romanrepublic.ac.uk/rdf/entity/> .
"""
    replacements = [
        (b"<http://romanrepublic.ac.uk/rdf/entity/", b"entity:"),
        (b"<http://romanrepublic.ac.uk/rdf/ontology#", b"dprr:"),
        (b"<http://www.w3.org/1999/02/22-rdf-syntax-ns#", b"rdf:"),
        (b"<http://www.w3.org/2000/01/rdf-schema#", b"rdfs:"),
        (b"<http://www.w3.org/2002/07/owl#", b"owl:"),
    ]
    for full, prefix in replacements:
        result = bytearray()
        i = 0
        while i < len(body):
            if body[i:i + len(full)] == full:
                end = body.index(b">", i + len(full))
                local = body[i + len(full):end]
                if b" " not in local and b"/" not in local and b"(" not in local:
                    result.extend(prefix)
                    result.extend(local)
                    i = end + 1
                    continue
            result.append(body[i])
            i += 1
        body = bytes(result)

    path.write_bytes(PREFIXES + b"\n" + body)

    updated = len([c for c in NEW_COMMENTS if NamedNode(VOCAB + c) in {q.subject for q in to_remove}])
    added = len(NEW_COMMENTS) - updated
    print(f"Updated {updated} comments, added {added} new")


if __name__ == "__main__":
    main()
