#/bin/sh

python btree-generator/btree.py $1 | dot -Tsvg > /tmp/tree.svg
sxiv tree.svg
