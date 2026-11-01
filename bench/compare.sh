#!/bin/sh
# The timed comparison: a minimal Asio server against http with the timeouts off (http) and on (httpt), one
# reactor, one connection and 64, 4 s per run. Give several builds of http_server_compare (from different
# revisions, with different file names) to compare them: the Asio server runs from the first, and the http kinds
# from every build in an order that rotates by round.
#   sh bench/compare.sh [BIN ...]            REPS=3 rounds by default; prints one line per run
#   python3 bench/summarize.py < output      medians per connection count, kind and build
set -u
cd "$(dirname "$0")/.." || exit 1
[ $# -gt 0 ] || set -- build/http_server_compare
REPS=${REPS:-3}
echo "# $(uname -sm) $(date '+%F %T') REPS=$REPS"
i=0
while [ $i -lt $REPS ]; do
	i=$((i+1)); echo "## round $i"
	for c in 1 64; do
		BIN=$1 sh bench/run.sh asio 1 $c 4
		for k in http httpt; do
			n=$#; j=0
			while [ $j -lt $n ]; do   # rotate the starting build by round
				j=$((j+1))
				idx=$(( (j + i - 2) % n + 1 ))
				eval "bin=\${$idx}"
				BIN=$bin sh bench/run.sh $k 1 $c 4
			done
		done
	done
done
