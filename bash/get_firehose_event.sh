#!/bin/bash

# get seq from command line
../target/debug/rustproto /command GetFirehoseEvent /datadir ../data/ /seq $1
    
