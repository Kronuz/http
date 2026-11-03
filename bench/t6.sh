#!/bin/sh
# T6/T7 on Linux metal in one command: every compiler's A, AA (a byte copy of A), B0 and B1 binaries, rotated per round
# by bench/ab.sh over every kind and connection count, then ab_compare.py (with AA as the control) for B0 and B1
# against A on req/s, p99, and server CPU per request (getrusage, and /proc as a check); then T7's counted runs.
#
#   BINS=DIR sh bench/t6.sh all        run, counts and report
#   sh bench/t6.sh run|counts|report   one step
#
# BINS holds one directory per compiler (COMPILERS="clang gcc"), each with the timing builds a, b0, b1 and the
# counting builds a-count, b1-count (b0-count if present); aa is copied from a. Environment (defaults):
#   OUT=$HOME/out/t6  KINDS="http httpr httpt httpb"  CONNS="8 64"  ROUNDS=7  SECS=4  REACTORS=2
#   SERVER_CPUS=  WRK_CPUS=      cpu lists for taskset (unset: unpinned); recorded in every RESULT line and meta.txt
#   COUNT_CONNS="1 8 64"  COUNT_SECS=3
set -u
cd "$(dirname "$0")/.." || exit 1
BINS=${BINS:-$HOME/bins}
OUT=${OUT:-$HOME/out/t6}
COMPILERS=${COMPILERS:-"clang gcc"}
KINDS=${KINDS:-"http httpr httpt httpb"}
CONNS=${CONNS:-"8 64"}
ROUNDS=${ROUNDS:-7}
SECS=${SECS:-4}
REACTORS=${REACTORS:-2}
COUNT_CONNS=${COUNT_CONNS:-"1 8 64"}
COUNT_SECS=${COUNT_SECS:-3}
AB_COMPARE=${AB_COMPARE:-$HOME/ab_compare.py}
export SERVER_CPUS WRK_CPUS
SERVER_CPUS=${SERVER_CPUS:-}; WRK_CPUS=${WRK_CPUS:-}
HERE=$(pwd)

meta() {
	mkdir -p "$OUT"
	{
		echo "date $(date '+%F %T %Z')"; echo "host $(uname -a)"
		echo "instance $(curl -s -m 2 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null)"
		lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Socket|NUMA|L3'
		echo "governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null)"
		echo "SERVER_CPUS=$SERVER_CPUS WRK_CPUS=$WRK_CPUS KINDS=$KINDS CONNS=$CONNS ROUNDS=$ROUNDS SECS=$SECS REACTORS=$REACTORS"
		echo "bench revision $(git rev-parse HEAD) ($(git describe --always --dirty))"
		wrk --version 2>&1 | head -1
		for c in $COMPILERS; do for f in "$BINS/$c"/*; do echo "sha256 $(sha256sum "$f" | cut -c1-16) $c/$(basename "$f")"; done; done
	} > "$OUT/meta.txt"
}

run() {
	meta
	for c in $COMPILERS; do
		o=$OUT/$c; mkdir -p "$o/bin"
		cp "$BINS/$c/a" "$o/bin/a"; cp "$BINS/$c/a" "$o/bin/aa"
		E="b0=$BINS/$c/b0 b1=$BINS/$c/b1"
		# the harness aborts the unit when a series answers other bytes
		ex=""; for k in $KINDS; do ex="$ex $k=$o/bin/a"; done
		# shellcheck disable=SC2086
		python3 bench/verify.py $ex || exit 1
		for s in b0 b1; do
			ex=""; for k in $KINDS; do ex="$ex $k=$BINS/$c/$s"; done
			# shellcheck disable=SC2086
			python3 bench/verify.py $ex || exit 1
		done
		OUT=$o KINDS="$KINDS" CONNS="$CONNS" ROUNDS=$ROUNDS SECS=$SECS REACTORS=$REACTORS EXTRA="$E" sh bench/ab.sh run || exit 1
	done
}

counts() {
	meta
	for c in $COMPILERS; do
		o=$OUT/$c; mkdir -p "$o/tmp"; : > "$o/counts.txt"
		for s in a b0 b1; do
			f=$BINS/$c/$s-count; [ -x "$f" ] || continue
			for n in $COUNT_CONNS; do
				for k in $KINDS; do
					BIN=$f LABEL=$s-count TMPDIR="$o/tmp" sh bench/run.sh "$k" "$REACTORS" "$n" "$COUNT_SECS" | tee -a "$o/counts.txt" | grep -v '^RESULT'
				done
			done
		done
	done
}

report() {
	ROW='^(?P<kind>\S+) .* C=(?P<conns>\d+) '
	for c in $COMPILERS; do
		o=$OUT/$c
		for b in aa b0 b1; do
			ctl="--aa $o/runs/aa"; [ $b = aa ] && ctl=""
			{
				echo "# $c: a against $b ($(date '+%F %T')); ratio above 1 means $b is better${ctl:+; control: aa}"
				echo "## req/s (higher is better)"
				python3 "$AB_COMPARE" compare --a "$o/runs/a" --b "$o/runs/$b" $ctl --higher-better --regex "$ROW"'+\S+ +req/s +(?P<value>[\d.]+)'
				echo "## p99 latency"
				python3 "$AB_COMPARE" compare --a "$o/runs/a" --b "$o/runs/$b" $ctl --regex "$ROW"'.*p99 +(?P<value>[\d.]+)(?P<unit>us|ms|s)'
				echo "## server CPU per request (us, getrusage)"
				python3 "$AB_COMPARE" compare --a "$o/runs/a" --b "$o/runs/$b" $ctl --regex "$ROW"'.*server CPU +(?P<value>[\d.]+) us/req'
				echo "## server CPU per request (us, /proc)"
				python3 "$AB_COMPARE" compare --a "$o/runs/a" --b "$o/runs/$b" $ctl --regex "$ROW"'.*proc CPU +(?P<value>[\d.]+) us/req'
			} > "$o/report-a-vs-$b.txt" 2>&1
			echo "report: $o/report-a-vs-$b.txt"
		done
		[ -s "$o/counts.txt" ] && grep -v '^RESULT' "$o/counts.txt" | awk '{for (i=1;i<=NF;i++) if ($i=="allocs/req") a=$(i+1); else if ($i=="handlers/req") h=$(i+1); print $1, $2, $4, "allocs/req", a, "handlers/req", h}' > "$o/counts-summary.txt"
	done
}

case ${1:-} in
run) run ;;
counts) counts ;;
report) report ;;
all) run && counts && report ;;
*) sed -n '2,/^set -u/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
