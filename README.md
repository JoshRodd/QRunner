# QRunner 0.1

A simple task runner based on Python which will run multiple commands
in different processes, and track the results in a simple CSV file.

To get started, just run the `./example.sh' program. It will ping
every host in the 10.20.48/22 network and then show the results of
hosts that responded.

For an example of how to make the CSV file, see `make\_tasks\_csv.sh`
and its related input file `inputfile.txt'.

This program doesn't have anything to do with GNU mailman's `qrunner'.
