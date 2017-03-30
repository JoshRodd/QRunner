#!/bin/bash

INPUTFILE="inputlist.txt"

if [ ! -f "$INPUTFILE" ]; then
	echo 'Make a simple list of IP addresses and put it in `inputlist.txt'"'"
	echo For example:
	echo
	echo 10.1.2.3
	echo 10.4.5.6
	echo 10.7.8.9
	exit 1
fi

OUTPUTFILE="tasks.csv"

echo -n > "$OUTPUTFILE"

TEMPLATE="ping -c 1 -t 1"

counter=0
grp=1
while read hosttest; do
	counter=$(($counter + 1))
	if [ $counter -gt 510 ]; then
		counter=1
		grp=$(($grp + 1))
	fi
	echo TASK_"$grp"_"$counter"_"$hosttest",NEW,,,"$TEMPLATE $hosttest",$grp >> "$OUTPUTFILE"
done < "$INPUTFILE"

echo Sample generated CSV file '`'"$OUTPUTFILE""'":
head -5 "$OUTPUTFILE"
echo You can run ./QRunner.py now.
