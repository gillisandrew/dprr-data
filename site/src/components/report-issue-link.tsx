// site/src/components/report-issue-link.tsx
import { reportIssueUrl, sourceFileUrl } from "@/lib/report-issue"

export function ReportIssueLink({
  entityLabel,
  ttlPath,
}: {
  entityLabel: string
  ttlPath: string
}) {
  return (
    <p className="mt-10 border-t border-rule-hair pt-3 text-xs text-muted-foreground">
      Spotted an error?{" "}
      <a
        href={reportIssueUrl({ entityLabel, ttlPath })}
        target="_blank"
        rel="noreferrer"
        className="underline hover:text-foreground"
      >
        Report an issue with this record
      </a>{" "}
      or inspect its{" "}
      <a
        href={sourceFileUrl(ttlPath)}
        target="_blank"
        rel="noreferrer"
        className="underline hover:text-foreground"
      >
        source data
      </a>
      .
    </p>
  )
}
