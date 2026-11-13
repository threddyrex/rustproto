#!/bin/bash

./tail_pds_log.sh | grep --line-buffered 'END REQUEST' | sed -u -E 's#( (GET|POST|PUT|DELETE|PATCH) +/[^ ]*).*#\1#'