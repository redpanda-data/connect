#!/usr/bin/env bash
# Prints the review threads of a pull request as one JSON array: the resolved and outdated
# state, the path and line, and each comment with its author and authorAssociation.
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

repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
gh api graphql --paginate -F owner="${repo%/*}" -F repo="${repo#*/}" -F pr="$1" -f query='
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
