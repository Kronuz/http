#!/bin/sh
# The paired A/A(/B) harness for http_server_compare: A (this checkout), A' (an identical copy of A: the noise of the
# measurement) and any further binaries, driven by bench/run.sh over rotated rounds and compared by ab_compare.py on
# req/s, p99 and server CPU per request. Builds and timed runs are separate steps so the caller holds the host for
# each: `build` under `quiet light`, `run` under `quiet strict` (or exclusive for a gate), `report` anywhere.
#
#   sh bench/ab.sh build     Release builds: A ($BUILD_DIR) and the counting build ($COUNT_DIR); A' is a copy of A.
#   sh bench/ab.sh verify    every series answers the same bytes (bench/verify.py).
#   sh bench/ab.sh run       ROUNDS rounds; per round, every kind and connection count, the series rotating by round.
#   sh bench/ab.sh counts    one counted run per kind and connection count: allocs/req and handlers/req (counting build).
#   sh bench/ab.sh report    ab_compare.py on the saved rounds: A against A' (or against B), with A' as the control.
#
# Environment (defaults): OUT=.scratch/ab  KINDS="http httpr"  CONNS="8 64"  ROUNDS=7  SECS=4  REACTORS=2
#   EXTRA="b0=/path/to/binary b1=..."  further series after a and aa; `report` compares a against the first of them
#   (with aa as the control) when EXTRA is set, and a against aa otherwise.
#   BUILD_DIR=build-ab COUNT_DIR=build-ab-count JOBS=4  (a BUILD_DIR that exists is reused)
#   AB_COMPARE=~/code/Kronuz/cpp-libraries/tools/ab_compare.py
# Smoke: ROUNDS=3 SECS=2. Results: $OUT/runs/<series>/rNN.txt (bench/run.sh lines), $OUT/counts.txt, $OUT/report.txt.
set -u
cd "$(dirname "$0")/.." || exit 1
OUT=${OUT:-.scratch/ab}
KINDS=${KINDS:-"http httpr"}
CONNS=${CONNS:-"8 64"}
ROUNDS=${ROUNDS:-7}
SECS=${SECS:-4}
REACTORS=${REACTORS:-2}
EXTRA=${EXTRA:-}
AB_COMPARE=${AB_COMPARE:-$HOME/code/Kronuz/cpp-libraries/tools/ab_compare.py}
JOBS=${JOBS:-4}
BUILD_DIR=${BUILD_DIR:-build-ab}
COUNT_DIR=${COUNT_DIR:-build-ab-count}

series() { echo "a aa $(for e in $EXTRA; do printf '%s ' "${e%%=*}"; done)"; }
binary() {
	case $1 in
		a|aa) echo "$OUT/bin/$1" ;;
		*) for e in $EXTRA; do [ "${e%%=*}" = "$1" ] && echo "${e#*=}"; done ;;
	esac
}

cmd=${1:-}
case $cmd in
build)
	mkdir -p "$OUT/bin"
	cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON -DHTTP_BENCH_COUNT_ALLOCS=OFF || exit 1
	nice -n 19 cmake --build "$BUILD_DIR" --target http_server_compare -j"$JOBS" || exit 1
	cp "$BUILD_DIR/http_server_compare" "$OUT/bin/a" && cp "$BUILD_DIR/http_server_compare" "$OUT/bin/aa" || exit 1
	cmake -S . -B "$COUNT_DIR" -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON -DHTTP_BENCH_COUNT_ALLOCS=ON || exit 1
	nice -n 19 cmake --build "$COUNT_DIR" --target http_server_compare -j"$JOBS" || exit 1
	cp "$COUNT_DIR/http_server_compare" "$OUT/bin/a-count"
	;;
verify)
	for s in $(series); do
		args=""
		for k in $KINDS; do args="$args $k=$(binary "$s")"; done
		# shellcheck disable=SC2086
		python3 bench/verify.py $args || exit 1
	done
	;;
run)
	[ -x "$OUT/bin/a" ] || { echo "ab.sh: run 'sh bench/ab.sh build' first" >&2; exit 1; }
	mkdir -p "$OUT/tmp"
	S=$(series); n=$(echo "$S" | wc -w | tr -d ' ')
	for s in $S; do mkdir -p "$OUT/runs/$s"; done
	echo "# $(uname -sm) $(date '+%F %T') KINDS=$KINDS CONNS=$CONNS ROUNDS=$ROUNDS SECS=$SECS REACTORS=$REACTORS series=$S" | tee "$OUT/run.log"
	r=0
	while [ $r -lt "$ROUNDS" ]; do
		r=$((r+1)); f=$(printf 'r%02d.txt' $r)
		for c in $CONNS; do
			for k in $KINDS; do
				j=0
				while [ $j -lt "$n" ]; do   # the starting series rotates by round
					j=$((j+1))
					s=$(echo "$S" | awk -v i=$(( (j + r - 2) % n + 1 )) '{print $i}')
					BIN=$(binary "$s") LABEL=$s TMPDIR="$OUT/tmp" sh bench/run.sh "$k" "$REACTORS" "$c" "$SECS" | tee -a "$OUT/run.log" | grep -v '^RESULT' >> "$OUT/runs/$s/$f"
				done
			done
		done
	done
	;;
counts)
	[ -x "$OUT/bin/a-count" ] || { echo "ab.sh: run 'sh bench/ab.sh build' first" >&2; exit 1; }
	mkdir -p "$OUT/tmp"; : > "$OUT/counts.txt"
	for c in 1 $CONNS; do
		for k in $KINDS; do
			BIN="$OUT/bin/a-count" LABEL=a-count TMPDIR="$OUT/tmp" sh bench/run.sh "$k" "$REACTORS" "$c" "$SECS" | tee -a "$OUT/counts.txt"
		done
	done
	;;
report)
	b=aa; ctl=""
	if [ -n "$EXTRA" ]; then b=${EXTRA%%=*}; ctl="--aa $OUT/runs/aa"; fi
	ROW='^(?P<kind>\S+) .* C=(?P<conns>\d+) '
	{
		echo "# a against $b ($(date '+%F %T')), ratio above 1 means $b is better"
		echo "## req/s"
		python3 "$AB_COMPARE" compare --a "$OUT/runs/a" --b "$OUT/runs/$b" $ctl --higher-better \
			--regex "$ROW"'+\S+ +req/s +(?P<value>[\d.]+)'
		echo "## p99 latency"
		python3 "$AB_COMPARE" compare --a "$OUT/runs/a" --b "$OUT/runs/$b" $ctl \
			--regex "$ROW"'.*p99 +(?P<value>[\d.]+)(?P<unit>us|ms|s)'
		echo "## server CPU per request (us)"
		python3 "$AB_COMPARE" compare --a "$OUT/runs/a" --b "$OUT/runs/$b" $ctl \
			--regex "$ROW"'.*server CPU +(?P<value>[\d.]+) us/req'
		[ -s "$OUT/counts.txt" ] && { echo "## allocations and handlers per request (counting build, A)"; grep -v '^RESULT' "$OUT/counts.txt" | awk '{for (i=1;i<=NF;i++) if ($i=="allocs/req") a=$(i+1); else if ($i=="handlers/req") h=$(i+1); print $1, $4, "allocs/req", a, "handlers/req", h}'; }
	} 2>&1 | tee "$OUT/report.txt"
	;;
*)
	sed -n '2,/^set -u/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
