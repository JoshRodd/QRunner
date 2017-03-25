#!/bin/bash

TEMPDIR="demo"

echo Example of how to use QRunner:
echo Going to probe 1,016 IP addresses on 10.20.48/22
echo The command is: ping -c 1 -t 1 10.20.c.d
echo -n Generating list..." "
mkdir -p $TEMPDIR
find $TEMPDIR -maxdepth 1 \( -name \*.in.txt -or -name \*.out.txt -or -name \*.err.txt \) -exec rm -f '{}' ';'
for x in `seq 48 51`; do for y in `seq 1 254`; do echo TASK_10.20.$x.$y,NEW,,,ping -c 1 -t 1 10.20.$x.$y,,,,$TEMPDIR; done; done > tasks.csv
echo done. Number of lines:$(wc -l tasks.csv)
echo The CSV file looks like this:
head -2 tasks.csv

./QRunner.py

echo QRunner is done. Now going to grep for any successful pings. Showing first 10 lines:

find $TEMPDIR -name \*.txt -exec grep '64 bytes' '{}' ';' | head -10 | awk '{print $4 " " $7 }'
