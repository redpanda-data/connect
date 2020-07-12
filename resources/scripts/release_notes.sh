#!/bin/sh
echo "For installation instructions check out the [getting started guide](https://www.benthos.dev/docs/guides/getting_started)."
cat CHANGELOG.md | awk '
  /^## [0-9]/ {
      release++;
  }
  !/^## [0-9]/ {
      if ( release == 1 ) print;
      if ( release > 1 ) exit;
  }'