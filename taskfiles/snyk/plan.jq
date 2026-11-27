# Plans the go get for one report.json project ($proj) against its module graph ($mods, from
# `go list -m -json all` via --slurpfile). Emits one fix.json result row per module, with status
# "skipped" (message is the reason) or "pending" (message lists the CVEs); the fix task turns
# pending rows into planned, updated or failed.
#   jq -c -n -L taskfiles/snyk --arg project go.mod --slurpfile mods mods.json --argjson proj '{...}' \
#     -f taskfiles/snyk/plan.jq
include "semver";

# Snyk reports Go package paths; resolve each to its module, the longest module path that is the
# package or a prefix of it.
[$proj.upgrades[] | select(.to != null) | . as $u
  | ([$mods[] | select(.Main | not) | .Path as $p
      | select($u.package == $p or ($u.package | startswith($p + "/")))]
     | sort_by(.Path | length) | last) as $m
  | {package: $u.package, module: $m.Path, current: $m.Version, replaced: ($m.Replace != null),
     target: ("v" + ($u.to | ltrimstr("v"))), cves: $u.fixedCves}]
# One entry per module: go get the highest target that needs no manual major migration, and list
# any targets that do need one as separate skipped entries so their CVEs aren't lost.
| group_by(.module)
| map(
    (.[0] | {module, current, replaced}) as $base
    | def entry: $base + {target: (map(.target) | sort_by(vkey) | last),
                          packages: (map(.package) | unique), cves: ([.[].cves[]] | unique)};
    # Unresolved packages all share module null, so keep them as one entry each.
    if $base.module == null then map($base + {target, packages: [.package], cves, skip: "package not found in the module graph"})
    elif $base.replaced then [entry + {skip: "module is replaced in go.mod"}]
    else
      map(. + {mskip: ($base + {target} | majorskip)})
      | (map(select(.mskip == null)) | if length > 0 then [entry] else [] end
         | map(. + {skip: (if (.current | vkey) >= (.target | vkey) then "already at \(.current)" else null end)}))
        + (map(select(.mskip != null)) | group_by(.mskip) | map(entry + {skip: .[0].mskip}))
    end)
| flatten
| sort_by(.module, (.skip != null))
| .[]
| {project: $project, module: (.module // .packages[0]), from: (.current // ""), to: .target,
   status: (if .skip != null then "skipped" else "pending" end), message: (.skip // (.cves | join(",")))}
