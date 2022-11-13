../tools/runmany.py -r ./run.sh -t fifo -l merdalog -p $argv[1] -m 100000000
math (cat merdalog/proc01.output | grep d | wc -l) / 30
