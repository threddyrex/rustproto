#!/bin/bash


echo ""
echo "SYSTEMCTL LIST UNITS:"
echo ""
systemctl list-units --type=service | grep rustproto
echo ""


echo ""
echo "PS AUX | GREP RUSTPROTO:"
echo ""
ps aux | grep rustproto
echo ""

