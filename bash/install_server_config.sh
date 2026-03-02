#!/bin/bash



../target/debug/rustproto /command InstallServerConfig /datadir ../data/ /listenScheme $1 /listenHost $2 /listenPort $3
