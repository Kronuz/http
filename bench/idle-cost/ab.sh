#!/bin/sh
# Idle memory per connection, before and after, in one job: builds http_idle_cost and its owners
# profile against each checkout ARMS names (the bench sources are this checkout's), then runs
# ROUNDS rounds per transport, alternating the arm order each round, and one owners-profile run
# per arm and transport.
#
#   sh bench/idle-cost/ab.sh build    Release builds of every arm, stamped with their commits
#   sh bench/idle-cost/ab.sh run      build what is missing, run, summarize
#   sh bench/idle-cost/ab.sh load     wrk rows (LOAD_ROWS, LOAD_ROUNDS, LOAD_SECONDS) per arm and an
#                                     A/A copy of the first, through load_ab.py
#
# Environment (defaults): ARMS="head=." (e.g. "base=$JOB_REF_BASE head=."), MODES="plain tls",
#   COUNT=10000, ROUNDS=3, WARM=0 (bytes each connection POSTs before idling), OWNERS=1,
#   IDLE=5 (seconds between the first two idle snapshots), OUT=.scratch/idle/out,
#   BINS=.scratch/idle (builds, shared by runs). Results: $OUT/runs/*.jsonl and
#   $OUT/summary.md, copied to $JOB_OUT when a campaign job runs it.
set -eu
cd "$(dirname "$0")/../.."
ARMS=${ARMS:-"head=."}
MODES=${MODES:-"plain tls"}
COUNT=${COUNT:-10000}
ROUNDS=${ROUNDS:-3}
WARM=${WARM:-0}
OWNERS=${OWNERS:-1}
IDLE=${IDLE:-5}
OUT=${OUT:-.scratch/idle/out}
BINS=${BINS:-.scratch/idle}
OPENSSL_ROOT=${OPENSSL_ROOT:-$(brew --prefix openssl@3 2>/dev/null || echo /usr)}
mkdir -p "$OUT/runs" "$BINS"

build_arm() {
	name=$1; src=$(cd "$2" && pwd); rev=$(git -C "$src" rev-parse HEAD)
	bench=$(git rev-parse HEAD)
	dir=$(pwd)/$BINS/build-$name
	if [ "$(cat "$dir/stamp" 2>/dev/null)" != "$rev $bench" ]; then
		cmake -S bench/idle-cost -B "$dir" -DCMAKE_BUILD_TYPE=Release -DHTTP_IDLE_PRODUCT_DIR="$src" \
			-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT" -DFETCHCONTENT_BASE_DIR="$dir/_deps" > /dev/null
		cmake --build "$dir" -j4 --target http_idle_cost http_idle_owners
		echo "$rev $bench" > "$dir/stamp"
	fi
	echo "# $name: product $rev bench $bench ($(file -b "$dir/http_idle_cost"))"
}

build_all() { for a in $ARMS; do build_arm "${a%%=*}" "${a#*=}"; done; }

# run_one ARM MODE ROUND BINARY: one run of the driver, its rows in $OUT/runs.
run_one() {
	arm=$1; mode=$2; round=$3; binary=$4
	n=$(find "$OUT/runs" -name '*.jsonl' | wc -l | tr -d ' ')
	base=$((20000 + 8 * (n % 90)))
	tls=""; [ "$mode" = tls ] && tls=--tls
	file="$OUT/runs/$arm-$mode-$COUNT-$round.jsonl"
	python3 bench/idle-cost/run.py "$binary" $tls --count "$COUNT" --warm-bytes "$WARM" --idle-seconds "$IDLE" \
		--base-port "$base" --source-port 30000 --label "$arm/$mode/$round" > "$file"
	echo "# ran $arm $mode round $round: $(tail -1 "$file")"
	if [ -n "${JOB_OUT:-}" ]; then mkdir -p "$JOB_OUT/runs" && cp "$file" "$JOB_OUT/runs/"; fi
}

case ${1:-} in
build) build_all ;;
run)
	build_all
	rm -rf "$OUT/runs" && mkdir -p "$OUT/runs"
	arms=""; for a in $ARMS; do arms="$arms ${a%%=*}"; done
	reversed=""; for a in $arms; do reversed="$a $reversed"; done
	for mode in $MODES; do
		r=0
		while [ "$r" -lt "$ROUNDS" ]; do
			order=$arms; [ $((r % 2)) -eq 1 ] && order=$reversed
			for a in $order; do run_one "$a" "$mode" "$r" "$BINS/build-$a/http_idle_cost"; done
			r=$((r + 1))
		done
		if [ "$OWNERS" = 1 ]; then
			for a in $arms; do run_one "$a" "$mode" owners "$BINS/build-$a/http_idle_owners"; done
		fi
	done
	python3 bench/idle-cost/summarize.py "$OUT/runs" > "$OUT/summary.md"
	cat "$OUT/summary.md"
	if [ -n "${JOB_OUT:-}" ]; then mkdir -p "$JOB_OUT" && cp -R "$OUT/runs" "$OUT/summary.md" "$JOB_OUT/"; fi
	;;
load)
	# Throughput, latency and CPU per request under wrk; "aa" is a copy of the first arm.
	build_all
	first=${ARMS%% *}; first=${first%%=*}
	cp "$BINS/build-$first/http_idle_cost" "$BINS/aa"
	set --
	for a in $ARMS; do set -- "$@" --arm "${a%%=*}=$BINS/build-${a%%=*}/http_idle_cost"; done
	set -- "$@" --arm "aa=$BINS/aa"
	# An assignment does not field-split; the unquoted $rows does, one row per word.
	rows=${LOAD_ROWS:-tls,/,1,keep tls,/,64,keep tls,/download/1048576,8,keep tls,/,8,close plain,/download/1048576,8,keep}
	for row in $rows; do set -- "$@" --row "$row"; done
	mkdir -p "$OUT"
	python3 bench/idle-cost/load_ab.py "$@" --rounds "${LOAD_ROUNDS:-5}" --seconds "${LOAD_SECONDS:-3}" --out "$OUT/load.jsonl" > "$OUT/load.txt" ||
		{ cat "$OUT/load.txt"; exit 1; }
	cat "$OUT/load.txt"
	if [ -n "${JOB_OUT:-}" ]; then mkdir -p "$JOB_OUT" && cp "$OUT/load.jsonl" "$OUT/load.txt" "$JOB_OUT/"; fi
	;;
*) echo "usage: $0 build|run|load" >&2; exit 2 ;;
esac
