#!/bin/bash

MOUNTPOINT="/tmp/fusemountpoint"
FILENAME="testfile"
BLOCK_SIZES=(1K 10K 100K 1M)
ITERATIONS=10

mkdir -p $MOUNTPOINT
./bfs -f $MOUNTPOINT & # Start the BFS filesystem in the background

sleep 2 # Wait for the filesystem to mount

for SIZE in "${BLOCK_SIZES[@]}"; do
    echo "Testing with file size: $SIZE"
    for ((i=1; i<=ITERATIONS; i++)); do
        echo "Iteration $i for $SIZE"
        dd if=/dev/zero of=$MOUNTPOINT/$FILENAME bs=$SIZE count=1 oflag=direct
        rm $MOUNTPOINT/$FILENAME
    done
done

fusermount -u $MOUNTPOINT
