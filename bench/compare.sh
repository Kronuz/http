#!/bin/sh
# The timed comparison: a minimal Asio server, http with the timeouts off (http) and on (httpt), Boost.Beast (beast)
# and cpp-httplib (httplib), each answering the same reply on one reactor (one thread), at one connection and 64, 4 s
# per run, wrk on the same host over loopback. Every server's reply is checked byte for byte first (bench/verify.py).
# Give several builds of http_server_compare (from different revisions, with different file names) to compare them:
# the other servers run from $BUILD, and the http kinds from every build in an order that rotates by round.
#   sh bench/compare.sh [BIN ...]            REPS=3 rounds, KINDS="asio http httpt beast httplib" (httpr too), CONNS="1 64"
#   python3 bench/summarize.py < output      medians per connection count, kind and build
# BUILD (default build) holds the binaries; KINDS, CONNS and REPS select a piece of the run (a long one is run in parts).
set -u
cd "$(dirname "$0")/.." || exit 1
BUILD=${BUILD:-build}; export BUILD
[ $# -gt 0 ] || set -- "$BUILD/http_server_compare"
REPS=${REPS:-3}
KINDS=${KINDS:-"asio http httpt beast httplib"}
CONNS=${CONNS:-"1 64"}
echo "# $(uname -sm) $(date '+%F %T') REPS=$REPS KINDS=$KINDS CONNS=$CONNS"
echo "# $(wrk --version 2>&1 | head -1)"
for b in compare beast httplib; do
	[ -x "$BUILD/http_server_$b" ] && echo "# $b: $("$BUILD/http_server_$b" version)"
done
V=""
for k in $KINDS; do
	case $k in
		beast) V="$V beast=$BUILD/http_server_beast" ;;
		httplib) V="$V httplib=$BUILD/http_server_httplib" ;;
		*) V="$V $k=$1" ;;
	esac
done
# shellcheck disable=SC2086
python3 bench/verify.py $V || exit 1
i=0
while [ $i -lt $REPS ]; do
	i=$((i+1)); echo "## round $i"
	for c in $CONNS; do
		for k in $KINDS; do
			case $k in
				http|httpt|httpr)
					n=$#; j=0
					while [ $j -lt $n ]; do   # rotate the starting build by round
						j=$((j+1))
						idx=$(( (j + i - 2) % n + 1 ))
						eval "bin=\${$idx}"
						BIN=$bin sh bench/run.sh $k 1 $c 4
					done ;;
				asio) BIN=$1 sh bench/run.sh $k 1 $c 4 ;;
				*) sh bench/run.sh $k 1 $c 4 ;;
			esac
		done
	done
done
