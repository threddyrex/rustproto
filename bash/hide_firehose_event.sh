#!/bin/bash

# get seq from command line
../target/debug/rustproto /command HideFirehoseEvent /datadir ../data/ /seq $1
    
