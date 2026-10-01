#!/usr/bin/env bash
# Prints the title, description, author, base and head branch, and conversation comments of a
# pull request as JSON. The conversation includes the earlier summary comments of the reviewer.
#
# Example output:
#
#   {
#     "author": {"login": "josephwoodward", "name": "Joseph Woodward", (...)},
#     "baseRefName": "main",
#     "body": "Adds a workflow that (...)",
#     "comments": [
#       {"author": {"login": "josephwoodward"}, "authorAssociation": "MEMBER", "body": "/soak",
#        "createdAt": "2026-09-25T15:04:40Z", "url": "https://github.com/(...)", (...)},
#       (...)
#     ],
#     "headRefName": "jw/pr_review_gh",
#     "title": "CI: Pull request review workflow"
#   }
#
# Used by the /review skill and by the CI reviewer (.github/workflows/claude-code-review.yml).
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 <pr-number>" >&2
  exit 2
fi

gh pr view "$1" --json title,body,author,baseRefName,headRefName,comments
