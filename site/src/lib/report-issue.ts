// site/src/lib/report-issue.ts
//
// Builds prefilled GitHub issue-form URLs so every detail page can offer
// "report an issue with this record" pointing at the exact TTL source in
// the repo. Field ids (entity, file) must match .github/ISSUE_TEMPLATE/
// data-issue.yml — GitHub prefills form fields from query params keyed by
// field id.

const REPO_URL = "https://github.com/gillisandrew/dprr-data"

export const REFERENCE_TTL = {
  office: "reference/offices.ttl",
  province: "reference/provinces.ttl",
  tribe: "reference/tribes.ttl",
} as const

/** shard.py places each person at persons/<id[:4]>/<id>.ttl */
export function personTtlPath(id: string): string {
  return `persons/${id.slice(0, 4)}/${id}.ttl`
}

/** A gens has no single file; link the shard directory via any member id. */
export function gensTtlPath(memberId: string): string {
  return `persons/${memberId.slice(0, 4)}/`
}

export function reportIssueUrl(opts: {
  entityLabel: string
  ttlPath: string
}): string {
  const params = new URLSearchParams({
    template: "data-issue.yml",
    title: `[data] ${opts.entityLabel}`,
    entity: opts.entityLabel,
    file: opts.ttlPath,
  })
  return `${REPO_URL}/issues/new?${params}`
}

/** GitHub blob/tree URL for the source file, shown beside the issue link. */
export function sourceFileUrl(ttlPath: string): string {
  const kind = ttlPath.endsWith("/") ? "tree" : "blob"
  return `${REPO_URL}/${kind}/main/${ttlPath}`
}
