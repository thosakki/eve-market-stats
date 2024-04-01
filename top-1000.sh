#!/bin/sh -e

awk -F , -vOFS=, '(NR < 1001){print $2,$5}' | sort -t , -k 2 -k 1
