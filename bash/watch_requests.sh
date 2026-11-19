#!/bin/bash

./tail_pds_log.sh | grep --line-buffered 'END REQUEST' | sed -u -E \
  -e 's/(\[END REQUEST\] \[)200( +\()/\1   \2/' \
  -e 's/(\[END REQUEST\] \[[0-9 ]{3} )\(([^)]* ms)\)\]/\1\2]/' \
  -e 's#( (GET|POST|PUT|DELETE|PATCH) +/[^ ]*).*#\1#'