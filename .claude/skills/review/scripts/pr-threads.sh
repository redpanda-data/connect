#!/usr/bin/env bash
# Prints the review threads of a pull request as one JSON array: the resolved and outdated
# state, the path and line, and each comment with its author and authorAssociation.
#
# Example output:
#
#   [
#     {
#       "isResolved": true,
#       "isOutdated": false,
#       "path": ".github/workflows/needs-review-digest.yml",
#       "line": 38,
#       "originalLine": 29,
#       "comments": {
#         "nodes": [
#           {"author": {"login": "claude"}, "authorAssociation": "NONE",
#            "body": "**A `gh pr list` failure is swallowed (...)", "createdAt": "2026-09-16T14:12:07Z"},
#           {"author": {"login": "josephwoodward"}, "authorAssociation": "MEMBER",
#            "body": "83ebdfdec480", "createdAt": "2026-09-16T14:25:24Z"}
#         ]
#       }
#     },
#     (...)
#   ]
#
# "line" is null when the thread is outdated.
#
# Used by the /review skill and by the CI reviewer (.github/workflows/claude-code-review.yml).
# The skill allows this script instead of `gh api graphql *`. That pattern also matches
# mutations, so a prompt injection in PR content could write to GitHub with the user's token.
# Here the query is fixed and the only input is a PR number, so the script can only read.
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 <pr-number>" >&2
  exit 2
fi

# gh fills {owner} and {repo} from GH_REPO, or else from the git remote of the current directory.
gh api graphql --paginate -F owner='{owner}' -F repo='{repo}' -F pr="$1" -f query='
  query($owner: String!, $repo: String!, $pr: Int!, $endCursor: String) {
    repository(owner: $owner, name: $repo) {
      pullRequest(number: $pr) {
        reviewThreads(first: 50, after: $endCursor) {
          pageInfo { hasNextPage endCursor }
          nodes {
            isResolved isOutdated path line originalLine
            comments(first: 50) { nodes { author { login } authorAssociation body createdAt } }
          }
        }
      }
    }
  }' --jq '.data.repository.pullRequest.reviewThreads.nodes[]' | jq -s '.'
