#!/bin/sh
echo "For installation instructions check out the [Getting Started Guide](https://www.benthos.dev/docs/guides/getting_started)."
cat CHANGELOG.md | awk '
  /^## [0-9]/ {
      release++;
  }
  !/^## [0-9]/ {
      if ( release == 1 ) print;
      if ( release > 1 ) exit;
  }'