# Version helpers shared by the snyk tasks. Load with `jq -L taskfiles/snyk` and `include "semver";`.

# Sort key for a Go/semver version: numeric parts, then 0 for pre-release / 1 for release, then
# the pre-release string, so pseudo-versions (by timestamp) and -rc.N with the same base still
# order. "+incompatible" and other build metadata are ignored.
def vkey: ltrimstr("v") | split("+")[0] | split("-") as $p
  | ($p[0] | split(".") | map(tonumber? // 0)) + [if ($p | length) > 1 then 0 else 1 end, ($p[1:] | join("-"))];

# Release line: the major version, or major.minor for v0 (where minor bumps are breaking).
def line: ltrimstr("v") | split("+")[0] | split("-")[0] | split(".") | if .[0] == "0" then .[0:2] else .[0:1] end;

# fixedIn lists one fixed version per maintained branch. Pick the fix for the branch $from is on:
# the lowest version >= $from on the same line, else the lowest >= $from on any line, else the
# highest listed.
def pickfix($from):
  map(select(vkey >= ($from | vkey))) as $ge
  | ($ge | map(select(line == ($from | line))) | sort_by(vkey) | first)
    // ($ge | sort_by(vkey) | first)
    // (sort_by(vkey) | last);

# Major version implied by a module path (/vN or gopkg.in .vN); null when the path has no suffix.
def pathmajor: ((capture("/v(?<n>[0-9]+)$") // capture("[.]v(?<n>[0-9]+)$")).n | tonumber) // null;

# Major version of a version string; v0 and v1 share a path, so both count as 1.
def vermajor: ltrimstr("v") | split(".")[0] | (tonumber? // 0) | if . < 2 then 1 else . end;

# Why {module, current, target} can't be reached with a plain go get, or null if it can.
#  - /vN path: the target must be vN, otherwise the fix lives under a different import path.
#  - +incompatible (v2+ without /vN): Go accepts any major on the same path, but only same-major
#    upgrades are applied unattended.
#  - otherwise: the path is the v0/v1 path, so a v2+ target needs the /vN path.
def majorskip:
  (.module | pathmajor) as $pm | (.target | vermajor) as $tm | (.current | vermajor) as $cm
  | if $pm != null then
      (if $pm != $tm then "fix is in major version \($tm), needs a manual migration to the /v\($tm) module path" else null end)
    elif (.current | endswith("+incompatible")) then
      (if $cm != $tm then "fix is a major version bump (v\($cm) -> v\($tm)) of a +incompatible module, review API changes and upgrade manually" else null end)
    elif $tm != 1 then "fix is in major version \($tm), needs a manual migration to the /v\($tm) module path"
    else null end;
