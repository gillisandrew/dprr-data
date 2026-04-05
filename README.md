# DPRR Data

Sharded RDF dataset from the [Digital Prosopography of the Roman Republic](https://romanrepublic.ac.uk) (DPRR).

Each person in the prosopography has their own Turtle file containing their full record — biographical data, office-holdings, family relationships, status assertions, scholarly notes, and source references.

## Layout

```
ontology.ttl              # OWL/RDFS schema definitions
reference/
  offices.ttl             # 204 political and religious offices
  provinces.ttl           # 92 Roman provinces
  praenomina.ttl          # 45 Roman first names
  tribes.ttl              # 37 voting tribes
  relationships.ttl       # 44 relationship types + inverses
  sources.ttl             # 32 secondary + 2 primary sources
  misc.ttl                # Sex, Status, DateType, NoteType
persons/
  CORN/                   # Cornelii (grouped by gens)
    CORN0017.ttl          # C. Cornelius (17) Cinna
    CORN0076.ttl          # P. Cornelius (76) Scipio
    ...
  IUNI/
    IUNI0001.ttl          # L. Iunius Brutus
    ...
```

Files are named by DPRR ID (e.g., `IUNI0001`) and grouped into directories by the 4-letter nomen code. Each person file is self-contained — it includes the person's triples plus all related assertions, notes, and references.

## Prefixes

```turtle
@prefix dprr: <http://romanrepublic.ac.uk/rdf/ontology#> .
@prefix entity: <http://romanrepublic.ac.uk/rdf/entity/> .
```

## Deterministic output

Files are generated using [RDFC 1.0](https://www.w3.org/TR/rdf-canon/) canonicalization with sorted N-Quads to ensure byte-stable output. Running the sharding script twice on the same input produces identical files, so git diffs reflect only actual data changes.

## Citation

> Mouritsen, H., Rathbone, D., Bradley, J., and Robb, M. (2017).
> Digital Prosopography of the Roman Republic.
> King's College London. https://romanrepublic.ac.uk/

## License

CC BY-NC 4.0 — see [LICENSE](LICENSE).
