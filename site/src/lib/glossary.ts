// site/src/lib/glossary.ts
// User-facing explanations of DPRR's domain terms. Single source of truth
// for InfoHint popovers and the About-page glossary. Texts are adapted
// from the DPRR ontology's rdfs:comment annotations.

export interface GlossaryEntry {
  label: string
  text: string
}

export const GLOSSARY = {
  patrician: {
    label: "Patrician",
    text: "A member of Rome's hereditary aristocracy, the small group of families that originally monopolized major offices and priesthoods. DPRR marks a person patrician only when it believes the sources support it; a following ? records that the attribution is uncertain.",
  },
  nobilis: {
    label: "Nobilis",
    text: "A member of the nobilitas — by the usual modern definition, a descendant of a consul. DPRR flags a person as nobilis when scholarship considered the family noble; the note shown beside the flag records why, usually one or two brief primary-source references.",
  },
  novus: {
    label: "Novus",
    text: 'A novus homo, "new man": the first of his family to reach the Senate or, in the stricter sense, the consulship. DPRR asserts this only where it believes the sources support it; a note may record the evidence.',
  },
  "eques-romanus": {
    label: "Eques Romanus",
    text: "A member of the equestrian order, the property class ranking below the Senate. Recorded as a dated status assertion with its supporting source rather than as a permanent attribute.",
  },
  senator: {
    label: "Senator",
    text: "Attested membership of the Senate, recorded as a dated status assertion with its supporting source — used chiefly where a person is known as a senator without a specific recorded magistracy.",
  },
  "roman-names": {
    label: "Roman names",
    text: "Roman men carried up to three names — a personal praenomen, the clan's nomen, and a branch cognomen — often cited with a filiation (father's and grandfather's praenomina) and, in modern scholarship, an RE number. Any part can be searched here; gens (nomen) finds every recorded member of a clan.",
  },
  praenomen: {
    label: "Praenomen",
    text: "The Roman personal name (Marcus, Gaius, Lucius…), conventionally abbreviated (M., C., L.). Only a small set was in use, so it rarely identifies a person on its own.",
  },
  nomen: {
    label: "Nomen (gens)",
    text: "The family or clan name (Iulius, Cornelius, Claudius…), shared by all members of a gens. Filtering by gens finds every recorded member of the clan.",
  },
  cognomen: {
    label: "Cognomen",
    text: "The surname distinguishing branches of a gens (Caesar, Scipio, Cicero…). Not every person carries one, especially in the early Republic.",
  },
  "other-names": {
    label: "Other names",
    text: "Names DPRR records for the person beyond the usual three-part praenomen–nomen–cognomen convention — adoptive names, agnomina, or alternative identifications found in the sources.",
  },
  filiation: {
    label: "Filiation",
    text: "The patronymic formula recorded in Roman naming: typically the father's (f.) and grandfather's (n. for nepos) praenomina, e.g. \"M. f. M. n.\" — son of Marcus, grandson of Marcus. DPRR records the filiation chosen by its research team from the sources.",
  },
  "re-number": {
    label: "RE number",
    text: "The person's entry number in the Realencyclopädie der classischen Altertumswissenschaft (Pauly–Wissowa), the standard reference work — e.g. Cicero is Tullius (29). Numbers distinguish homonymous members of the same gens.",
  },
  tribe: {
    label: "Tribe",
    text: "One of the 35 Roman voting tribes (tribus) in which citizens were registered, e.g. Cornelia or Fabia. A person's tribe is part of their formal identification; differing source claims can leave a person with more than one recorded tribe.",
  },
  office: {
    label: "Office",
    text: 'A political or religious post in the Roman state (consul, praetor, tribune, pontifex…). Offices form a hierarchy — selecting a parent like "Magisterial Posts" includes every office beneath it.',
  },
  location: {
    label: "Provincia",
    text: "The sphere of responsibility assigned with a post — often a territory (Sicilia, Hispania), but equally a task or command: a war, a fleet, the courts, the grain supply. DPRR records the provincia as given in the sources; the geographic grouping used for browsing is this site's curation.",
  },
  "life-events": {
    label: "Life events",
    text: "Dated biographical events recorded for a person beyond office-holding: birth, death (including violent death), exile, proscription, and similar. Each carries its supporting source.",
  },
  era: {
    label: "Era dates",
    text: "The date range in which DPRR believes the person lived. Birth and death dates rarely survive, so these are estimates derived from the attested record, not precise lifespans.",
  },
  uncertain: {
    label: "Uncertainty (?)",
    text: "A question mark records that the DPRR team considers the flagged element uncertain — a name part, a date, or a whole assertion. It reflects the state of the evidence, not an error.",
  },
  "office-and-mode": {
    label: "Require every office",
    text: 'With this on, results must have held ALL of the selected offices at some point (an intersection), instead of any one of them. Useful for questions like "who was both consul and censor?".',
  },
  "office-in-range": {
    label: "Offices in time range",
    text: 'With this on, the selected time period applies to when the offices were held, not just to when the person lived — "praetors in the 60s BC" rather than "praetors alive in the 60s BC".',
  },
  status: {
    label: "Status",
    text: "Social-rank attributes recorded by DPRR: Patrician, Nobilis, and Novus are person-level flags; Eques Romanus and Senator are dated, sourced assertions. Selecting several requires all of them (AND).",
  },
  origin: {
    label: "Origin",
    text: "A plausible geographic origin for the person — a town or region — as suggested by the primary and secondary sources and recorded by DPRR.",
  },
  "broughton-label": {
    label: "Source label",
    text: 'The abbreviated post as it appears in the source scholarship (chiefly Broughton\'s Magistrates of the Roman Republic), e.g. "cos. 63" or "Pr. Peregrinus" — sometimes more specific than the standardized office name.',
  },
} as const satisfies Record<string, GlossaryEntry>

export type GlossaryTermId = keyof typeof GLOSSARY
