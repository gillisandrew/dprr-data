// site/src/data/province-mapping.ts
//
// Curated mapping from free-text hasProvinceOriginal(Expanded) strings in
// the person TTL export to canonical province names in
// reference/provinces.ttl. The export has no structured province links,
// so this file IS the province resolution — every entry is a curation
// decision. Strings absent from this map are surfaced as build warnings
// and excluded from the province facet; their raw text still renders on
// person pages.
//
// Two notations coexist in the export:
//   1. Plain names, mostly from Broughton MRR ("Sicily", "Spain", "Rome").
//   2. A terse provincia code system used for praetorian/quaestorian
//      assignments, where hasProvinceOriginal holds the code and
//      hasProvinceOriginalExpanded its expansion. Every code below was
//      verified against its paired expansion in the export rather than
//      guessed: U=urbanus, C=city, Si=Sicilia, Sd=Sardinia, A=Asia,
//      Af=Africa, G=Gallia, GC=Gallia Cisalpina, GT=Gallia Transalpina,
//      H=Hispania, HC=Hispania Citerior, HU=Hispania Ulterior,
//      M=Macedonia/Achaea, Ci=Cilicia, Cy=Cyrene, Etr=Etruria, T=Tarentum,
//      L=Luceria, III=Illyria, FL=fleet, amb=ambitus, ip=inter peregrinos,
//      qsc=quo senatus censuisset, Z=provincia declined.
//
// Notation within that code system:
//   "?" / "??" prefix = uncertain attribution. Per the curation rules these
//       qualifiers are stripped and the province is still recorded, so the
//       facet cannot express uncertainty — the person page shows the raw
//       text, which retains it.
//   "/" separates successive provinciae within one post; a bare "?"
//       segment is an unknown province and contributes nothing.
//   "+" combines concurrent provinciae. ">" marks a transfer.

export const PROVINCE_MAPPING: Record<string, string[]> = {
  // --- Plain names: exact canonical matches -------------------------------
  Achaea: ["Achaea"],
  Africa: ["Africa"],
  "Africa Vetus": ["Africa Vetus"],
  Apulia: ["Apulia"],
  Armenia: ["Armenia"],
  Asia: ["Asia"],
  Bithynia: ["Bithynia"],
  Campania: ["Campania"],
  Cilicia: ["Cilicia"],
  Crete: ["Crete"],
  Cyrenaica: ["Cyrenaica"],
  Dalmatia: ["Dalmatia"],
  Etruria: ["Etruria"],
  Gallia: ["Gallia"],
  "Gallia Cisalpina": ["Gallia Cisalpina"],
  "Gallia Cispadana": ["Gallia Cispadana"],
  "Gallia Transalpina": ["Gallia Transalpina"],
  Hispania: ["Hispania"],
  "Hispania Citerior": ["Hispania Citerior"],
  "Hispania Ulterior": ["Hispania Ulterior"],
  Illyricum: ["Illyricum"],
  Italia: ["Italia"],
  Iudaea: ["Iudaea"],
  Macedonia: ["Macedonia"],
  Mauretania: ["Mauretania"],
  Numidia: ["Numidia"],
  Rome: ["Rome"],
  Samnium: ["Samnium"],
  Sardinia: ["Sardinia"],
  Sicilia: ["Sicilia"],
  Syria: ["Syria"],
  Tarentum: ["Tarentum"],
  // "Macedonia/Achaea" is itself a canonical province entity in
  // provinces.ttl (the combined Republican command), so it maps to itself
  // rather than being split into its two components.
  "Macedonia/Achaea": ["Macedonia/Achaea"],

  // --- Plain names: spelling, case and language variants -------------------
  Sicily: ["Sicilia"],
  Spain: ["Hispania"],
  Gaul: ["Gallia"],
  Italy: ["Italia"],
  Thrace: ["Thracia"],
  Fleet: ["fleet"],
  Urbanus: ["urbanus"],
  "Hisp. Cit.": ["Hispania Citerior"],
  // No canonical "Greece" exists; Achaea is the canonical province covering
  // Greece proper in this vocabulary (cf. the canonical "Macedonia/Achaea").
  Greece: ["Achaea"],
  // Broughton's "Palestine" denotes the Judaean theatre; Iudaea is the
  // canonical name.
  Palestine: ["Iudaea"],

  // --- Jurisdictional and court provinciae ---------------------------------
  city: ["city"],
  urbanus: ["urbanus"],
  "inter peregrinos": ["inter peregrinos"],
  "quo senatus censuisset": ["quo senatus censuisset"],
  // The praetor peregrinus' provincia is recorded canonically as the
  // jurisdiction "inter peregrinos".
  Peregrinus: ["inter peregrinos"],
  // Standing-court provinciae: the export uses "quaestio de X" phrasing,
  // the canonical vocabulary the bare offence names.
  "Quaestio de ambitu": ["ambitus"],
  "Quaestio de maiestate": ["maiestas"],
  "Quaestio de vi": ["de vi"],
  "de repetundis": ["repetundae"],

  // --- Conjunctions: multiple provinciae in one string ---------------------
  "Greece and Asia": ["Achaea", "Asia"],
  "Greece, Asia": ["Achaea", "Asia"],
  "Greece and Macedonia": ["Achaea", "Macedonia"],
  "Macedonia and Thrace": ["Macedonia", "Thracia"],
  "Crete and Achaea": ["Crete", "Achaea"],
  "Crete and Cyrenaica": ["Crete", "Cyrenaica"],
  "Crete and Cyrene": ["Crete", "Cyrene"],
  "Cilicia and Syria": ["Cilicia", "Syria"],
  "Cilicia, Cyprus": ["Cilicia", "Cyprus"],
  "Bithynia, Cappadocia": ["Bithynia", "Cappadocia"],
  "Italy, Africa": ["Italia", "Africa"],
  "Italia, Numidia": ["Italia", "Numidia"],
  "Gallia Cisalpina & Gallia Transalpina": [
    "Gallia Cisalpina",
    "Gallia Transalpina",
  ],
  "Fleet and Macedonia": ["fleet", "Macedonia"],
  // Pompey's eastern command is written as a hyphenated pair; the canonical
  // vocabulary keeps Bithynia and Pontus as separate provinces.
  "Bithynia-Pontus": ["Bithynia", "Pontus"],
  "Bithynia and Pontus": ["Bithynia", "Pontus"],
  "Pontus & Bithynia": ["Pontus", "Bithynia"],
  "Asia, Bithynia, and Pontus": ["Asia", "Bithynia", "Pontus"],
  "Bithynia-Pontus, Armenia": ["Bithynia", "Pontus", "Armenia"],
  "Armenia, Bithynia-Pontus": ["Armenia", "Bithynia", "Pontus"],
  "Fleet, Bithynia-Pontus": ["fleet", "Bithynia", "Pontus"],

  // --- Uncertainty qualifiers stripped -------------------------------------
  "Spain?": ["Hispania"],
  "Sicilia?": ["Sicilia"],
  "Crete and Cyrenaica?": ["Crete", "Cyrenaica"],
  // "(and Ulterior?)" queries the second province rather than the first;
  // both are recorded so the person surfaces under either.
  "Hispania Citerior (and Ulterior?)": [
    "Hispania Citerior",
    "Hispania Ulterior",
  ],
  "Hispania Citerior (and Ulterior?), Gallia Transalpina": [
    "Hispania Citerior",
    "Hispania Ulterior",
    "Gallia Transalpina",
  ],

  // --- Provincia codes (hasProvinceOriginal) -------------------------------
  A: ["Asia"],
  Af: ["Africa"],
  C: ["city"],
  Etr: ["Etruria"],
  G: ["Gallia"],
  H: ["Hispania"],
  HC: ["Hispania Citerior"],
  HU: ["Hispania Ulterior"],
  M: ["Macedonia/Achaea"],
  Sd: ["Sardinia"],
  Si: ["Sicilia"],
  T: ["Tarentum"],
  U: ["urbanus"],
  ip: ["inter peregrinos"],
  qsc: ["quo senatus censuisset"],

  // --- Coded and expanded forms with uncertainty markers -------------------
  "?A": ["Asia"],
  "??A": ["Asia"],
  "?Asia": ["Asia"],
  "??Asia": ["Asia"],
  "?C": ["city"],
  "?city": ["city"],
  "?FL": ["fleet"],
  "?fleet": ["fleet"],
  "?GC": ["Gallia Cisalpina"],
  "?Gallia Cisalpina": ["Gallia Cisalpina"],
  "??G": ["Gallia"],
  "??Gallia": ["Gallia"],
  "??HU": ["Hispania Ulterior"],
  "??Hispania Ulterior": ["Hispania Ulterior"],
  "?M": ["Macedonia/Achaea"],
  "??M": ["Macedonia/Achaea"],
  "?Macedonia/Achaea": ["Macedonia/Achaea"],
  "??Macedonia/Achaea": ["Macedonia/Achaea"],
  "?Sd": ["Sardinia"],
  "?Sardinia": ["Sardinia"],
  "?Si": ["Sicilia"],
  "?Sicilia": ["Sicilia"],
  "?U": ["urbanus"],
  "?urbanus": ["urbanus"],
  "?Z": ["provincia declined"],
  "?provincia declined": ["provincia declined"],
  "?ip": ["inter peregrinos"],
  "?inter peregrinos": ["inter peregrinos"],
  "?qsc": ["quo senatus censuisset"],
  "?quo senatus censuisset": ["quo senatus censuisset"],

  // --- Successive provinciae ("/"); unknown "?" segments contribute nothing
  "?/A": ["Asia"],
  "??/A": ["Asia"],
  "?/Asia": ["Asia"],
  "??/Asia": ["Asia"],
  "?/Ci": ["Cilicia"],
  "?/Cilicia": ["Cilicia"],
  "?/HC": ["Hispania Citerior"],
  "?/?HC": ["Hispania Citerior"],
  "?/Hispania Citerior": ["Hispania Citerior"],
  "?/?Hispania Citerior": ["Hispania Citerior"],
  "?/III": ["Illyria"],
  "?/Illyria": ["Illyria"],
  "?/M": ["Macedonia/Achaea"],
  "?/Macedonia/Achaea": ["Macedonia/Achaea"],
  "?/Si": ["Sicilia"],
  "?/Sicilia": ["Sicilia"],
  "?/GT>HC": ["Gallia Transalpina", "Hispania Citerior"],
  "?/Gallia Transalpina>Hispania Citerior": [
    "Gallia Transalpina",
    "Hispania Citerior",
  ],
  "?/?Z/A": ["provincia declined", "Asia"],
  "?/?provincia declined/Asia": ["provincia declined", "Asia"],
  "?/?Z/?Cy": ["provincia declined", "Cyrene"],
  "?/?provincia declined/?Cyrene": ["provincia declined", "Cyrene"],
  "amb/?Z": ["ambitus", "provincia declined"],
  "ambitus/?provincia declined": ["ambitus", "provincia declined"],
  "U/?Z": ["urbanus", "provincia declined"],
  "urbanus/?provincia declined": ["urbanus", "provincia declined"],
  "C/A": ["city", "Asia"],
  "city/Asia": ["city", "Asia"],
  "C/Af": ["city", "Africa"],
  "city/Africa": ["city", "Africa"],
  "C/??Ci": ["city", "Cilicia"],
  "city/??Cilicia": ["city", "Cilicia"],
  "C/??M": ["city", "Macedonia/Achaea"],
  "city/??Macedonia/Achaea": ["city", "Macedonia/Achaea"],

  // --- Concurrent provinciae ("+") -----------------------------------------
  "C+qsc": ["city", "quo senatus censuisset"],
  "city+quo senatus censuisset": ["city", "quo senatus censuisset"],
  "U+qsc": ["urbanus", "quo senatus censuisset"],
  "urbanus+quo senatus censuisset": ["urbanus", "quo senatus censuisset"],
  "ip+qsc": ["inter peregrinos", "quo senatus censuisset"],
  "inter peregrinos+quo senatus censuisset": [
    "inter peregrinos",
    "quo senatus censuisset",
  ],
  "ip+L": ["inter peregrinos", "Luceria"],
  "inter peregrinos+Luceria": ["inter peregrinos", "Luceria"],
}

// Deliberately left unmapped (each becomes a build warning):
//   "Narbo"                 town/colony; ambiguous between Narbo Martius
//                           itself and Gallia Narbonensis.
//   "Parthia"               no canonical equivalent; never a Roman province.
//   "Aquitania"             a region of Gaul with no canonical entry; mapping
//                           it to Gallia would over-broaden the facet.
//   "Osca", "Perusia"       towns with no canonical province entry.
//   "Laodiceia-ad-Lycum ?"  town in Asia; uncertain and not canonical.
//   "urbanus or inter peregrinos"
//                           a genuine disjunction — asserting both would be
//                           wrong. (Q. Maenius, pr. 170: the post carries only
//                           hasProvinceOriginalExpanded, so it contributes no
//                           facet value; the raw text still shows on his page.)
//   "?/?"                   two successive unknown provinces; nothing to map.

export function mapProvinceText(raw: string): string[] | null {
  return PROVINCE_MAPPING[raw.trim()] ?? null
}

export function collectUnmappedProvinces(
  rawValues: Iterable<string>
): string[] {
  const unmapped = new Set<string>()
  for (const raw of rawValues) {
    if (raw && mapProvinceText(raw) === null) unmapped.add(raw)
  }
  return [...unmapped].sort()
}
