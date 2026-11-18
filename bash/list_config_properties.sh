#!/bin/bash

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)" || exit 1
MSYS2_ARG_CONV_EXCL='/command;/dataDir' exec "$script_dir/../target/debug/rustproto" \
    /command ListConfigProperties /dataDir "${1:-$script_dir/../data}"
