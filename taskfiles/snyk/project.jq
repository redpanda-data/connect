# Turns the raw stdout of `snyk test --json` for one project into a report.json project entry.
# Run with -R -s so output that isn't JSON still becomes an entry with `error` set, rather than
# the project silently disappearing from the report.
#   jq -R -s -L taskfiles/snyk --arg file go.mod --argjson rc 1 -f taskfiles/snyk/project.jq raw.json
include "semver";

def vuln: {id, title, severity, fixedIn, fixVersion: .fix,
           cves: (.identifiers.CVE // []), ghsa: (.identifiers.GHSA // [])};

# One entry per package, version and release line of the fix. Advisories whose only fix is on a
# newer line get their own entry, so they don't drag the same-line fixes into a major bump.
# Advisories with no fix are attached to the first entry.
def upgrades:
  [.vulnerabilities[]?] | unique_by([.id, .packageName, .version])
  | group_by([.packageName, .version]) | map(
      .[0].packageName as $pkg | .[0].version as $from
      | map(. + {fix: (if (.fixedIn | length) > 0 then (.fixedIn | pickfix($from)) else null end)})
      | map(select(.fix == null)) as $unfixed
      | (map(select(.fix != null)) | group_by(.fix | line) | sort_by(.[0].fix | vkey)) as $lines
      | (if ($lines | length) == 0 then [[]] else $lines end)
      | to_entries | map(
          (.value + (if .key == 0 then $unfixed else [] end)) as $g
          | {
              package: $pkg,
              from: $from,
              to: ([$g[] | .fix // empty] | sort_by(vkey) | last),
              fixedCves: ([$g[] | select(.fix != null) | .identifiers.CVE[]?] | unique),
              unfixedCves: ([$g[] | select(.fix == null) | .identifiers.CVE[]?] | unique),
              vulnerabilities: ($g | map(vuln) | sort_by(.id))
            })
    ) | flatten | sort_by(.to == null, .package, (.to // "0" | vkey));

. as $raw
| (try fromjson catch null) as $r
| if ($r | type) == "object" or ($r | type) == "array" then
    ($r | if type == "array" then .[0] else . end)
    | {
        file: $file,
        packageManager,
        exitCode: $rc,
        error: (if $rc >= 2 then (.error // "snyk failed") else null end),
        upgrades: upgrades
      }
  else
    {
      file: $file,
      packageManager: null,
      exitCode: $rc,
      error: ("snyk output is not valid JSON"
        + (if $raw == "" then " (empty)" else ": " + ($raw[:300] | gsub("\n"; " ")) end)),
      upgrades: []
    }
  end
