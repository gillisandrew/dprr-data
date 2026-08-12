# Normalize Person Files (Drop Related-Person Stubs) — Design

**Date:** 2026-08-12
**Status:** Approved

## Problem

`shard.py`'s `collect_person_triples` follows every entity link out of a
person's assertions; `hasRelatedPerson` targets are `dprr:Person` entities,
so each person file embeds denormalized copies ("stubs") of every related
person's core triples. The site never needed them: the build loads all
files and already runs `dedupePersons` (discard stubs by richness) plus a
second pass that resolves cross-file relationship names from the full
person set. The stubs bloat the data, churn relatives' files whenever a
person's core fields change, and muddy the "one file per person" contract.

## Change

1. **`shard.py`:** in the assertion-object loop of `collect_person_triples`,
   skip objects typed `dprr:Person` (alongside the existing reference-type
   skip). Result: each `persons/**.ttl` contains exactly one `dprr:Person`
   plus that person's assertions, notes, and per-assertion entities.
2. **Re-shard** from `~/Downloads/dprr.ttl`. Expected diff: pure deletions
   of stub Person blocks (plus canonicalization reflow).
3. **Site:** no functional change — `parse-persons.ts` already stores the
   raw URI when a related person isn't co-located, and the loader's second
   pass resolves it. Refactor: extract that pass as an exported
   `resolveCrossFileRelationships(persons)` with a unit test proving a
   cross-file relationship resolves to name + DPRR ID; keep
   `dedupePersons` as a cheap invariant guard with an updated comment.
4. **README:** layout note says one person per file; related persons are
   bare URIs resolved at build time.

## Trade-off (accepted)

Raw TTL files are no longer individually self-contained: a relationship's
target appears as a bare `entity/Person/N` URI when read directly on
GitHub. The site is unaffected.

## Verification

`vp check`/`vp test` green; `SATR4945.ttl` contains exactly one
`a dprr:Person`; person count still 4,876; built person JSON for SATR4945
has `relationships[0].relatedPersonName` resolved to the Minucius Basilus
display name; full `vp run build` passes.
