#!/usr/bin/env bash
# Prints the pull requests related to a pull request as one JSON array: the other open PRs of
# its author, and the open PRs stacked on it (their base is its head branch). Small PRs often
# leave a concern to a sibling PR on purpose, so the reviewer checks these before it flags one.
# Bodies are truncated to 2000 characters to keep the review input small.
#
# Example output:
#
#   [
#     {
#       "number": 4541,
#       "title": "oracledb_cdc: Pre-filter relevant transactions via a common table expression",
#       "body": "(...)",
#       "baseRefName": "main",
#       "headRefName": "jw/oracledbcte",
#       "files": ["cmd/tools/integration/packages.json", "internal/impl/oracledb/(...)", (...)]
#     },
#     (...)
#   ]
#
# Used by the /review skill and by the CI reviewer (.github/workflows/claude-code-review.yml).
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 <pr-number>" >&2
  exit 2
fi

pr=$(gh pr view "$1" --json author,headRefName)
author=$(jq -r '.author.login' <<<"$pr")
head_ref=$(jq -r '.headRefName' <<<"$pr")
fields=number,title,body,baseRefName,headRefName,files

{
  gh pr list --author "$author" --state open --limit 30 --json "$fields"
  gh pr list --base "$head_ref" --state open --limit 30 --json "$fields"
} | jq -s --argjson pr "$1" '
  add | unique_by(.number) | map(select(.number != $pr)
    | .body = ((.body // "")[0:2000]) | .files = [.files[].path])'
