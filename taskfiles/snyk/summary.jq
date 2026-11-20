# Merges the per-project entries (from project.jq, read with -s) into report.json, with a summary
# of each upgrade across projects.
{
  generatedAt: (now | todate),
  projects: .,
  summary: (
    [.[] | .file as $f | .upgrades[] | . + {project: $f}]
    | group_by([.package, .from, .to])
    | map({
        package: .[0].package,
        from: .[0].from,
        to: .[0].to,
        fixedCves: ([.[].fixedCves[]] | unique),
        unfixedCves: ([.[].unfixedCves[]] | unique),
        projects: (map(.project) | unique)
      })
    | sort_by(.to == null, .package)
  )
}
