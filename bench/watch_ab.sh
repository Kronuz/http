#!/bin/sh
# The disconnect watch's cost per request, before and after a change, in one job: http_server_compare's http kind
# (a handler that never asks for its cancel token) and httpc (one that asks, which engages the watch), served by
# this checkout (a), a copy of it (aa, the A/A control) and the checkouts REFS names, through bench/ab.sh.
#
#   sh bench/watch_ab.sh build    Release builds (timed and counting) of every series, stamped with their commits
#   sh bench/watch_ab.sh run      verify, counts, the timed rounds, and one report per REFS series against a
#
# Environment (defaults): REFS="name=checkout ..." (e.g. "base=$JOB_REF_BASE"), OUT=.scratch/watch-ab/out,
#   BINS=.scratch/watch-ab (the builds, shared by runs), KINDS="http httpc" CONNS="1 8 64" ROUNDS=7 SECS=4
#   REACTORS=2. `run` builds what `build` did not, or what was built from another commit. Results:
#   $OUT/report-<name>.txt (a against <name>, aa the control), $OUT/counts-*.txt, $OUT/medians.txt; copied to
#   $JOB_OUT when a campaign job runs it.
set -eu
cd "$(dirname "$0")/.."
OUT=${OUT:-.scratch/watch-ab/out}
BINS=${BINS:-.scratch/watch-ab}
REFS=${REFS:-}
export KINDS=${KINDS:-"http httpc"} CONNS=${CONNS:-"1 8 64"} ROUNDS=${ROUNDS:-7} SECS=${SECS:-4} REACTORS=${REACTORS:-2}
BIN=$OUT/bin
mkdir -p "$BIN" "$BINS/bin"

# build_series NAME SOURCE: the timed and counting servers of SOURCE as $BIN/NAME and $BIN/NAME-count.
build_series() {
	name=$1; src=$(cd "$2" && pwd); rev=$(git -C "$src" rev-parse HEAD)
	if ! { [ -x "$BINS/bin/$name" ] && [ -x "$BINS/bin/$name-count" ] && [ "$(cat "$BINS/bin/$name.rev" 2>/dev/null)" = "$rev" ]; }; then
		build_dirs "$name" "$src" "$rev"
	fi
	cp "$BINS/bin/$name" "$BINS/bin/$name-count" "$BINS/bin/$name.rev" "$BIN/"
	echo "# $name: $rev ($(file -b "$BIN/$name"))"
}
build_dirs() {
	name=$1; src=$2; rev=$3
	dir=$(pwd)/$BINS/build/$name
	cmake -S "$src" -B "$dir" -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON -DHTTP_BENCH_COUNT_ALLOCS=OFF \
		-DFETCHCONTENT_BASE_DIR="$dir/_deps" > /dev/null
	cmake --build "$dir" --target http_server_compare
	cp "$dir/http_server_compare" "$BINS/bin/$name"
	cmake -S "$src" -B "$dir-count" -DCMAKE_BUILD_TYPE=Release -DHTTP_BUILD_BENCH=ON -DHTTP_BENCH_COUNT_ALLOCS=ON \
		-DFETCHCONTENT_BASE_DIR="$dir-count/_deps" > /dev/null
	cmake --build "$dir-count" --target http_server_compare
	cp "$dir-count/http_server_compare" "$BINS/bin/$name-count"
	echo "$rev" > "$BINS/bin/$name.rev"
}

build_all() {
	build_series a .
	for r in $REFS; do build_series "${r%%=*}" "${r#*=}"; done
}

case ${1:-} in
build) build_all ;;
run)
	build_all
	cp "$BIN/a" "$BIN/aa"
	extra=""; for r in $REFS; do extra="$extra ${r%%=*}=$BIN/${r%%=*}"; done
	extra=${extra# }
	for s in a $(for r in $REFS; do printf '%s ' "${r%%=*}"; done); do echo "# $s: $(cat "$BIN/$s.rev")"; done | tee "$OUT/series.txt"
	OUT=$OUT EXTRA="$extra" sh bench/ab.sh verify
	# allocations and Asio handlers per request, each series' counting build
	for s in a $(for r in $REFS; do printf '%s ' "${r%%=*}"; done); do
		mkdir -p "$OUT/tmp"; : > "$OUT/counts-$s.txt"
		for c in 1 8; do for k in $KINDS; do
			BIN="$BIN/$s-count" LABEL="$s-count" TMPDIR="$OUT/tmp" sh bench/run.sh "$k" "$REACTORS" "$c" 2 | grep -v '^RESULT' | tee -a "$OUT/counts-$s.txt"
		done; done
	done
	rm -rf "$OUT/runs"
	OUT=$OUT EXTRA="$extra" sh bench/ab.sh run > /dev/null
	for r in $REFS; do
		name=${r%%=*}
		OUT=$OUT EXTRA="$name=$BIN/$name" sh bench/ab.sh report > /dev/null
		mv "$OUT/report.txt" "$OUT/report-$name.txt"
		cat "$OUT/report-$name.txt"
	done
	# the medians per series, kind and connection count
	for s in a aa $(for r in $REFS; do printf '%s ' "${r%%=*}"; done); do
		cat "$OUT"/runs/"$s"/r*.txt | awk -v s="$s" '{ for (i = 1; i <= NF; i++) { if ($i == "req/s") r = $(i+1); if ($i == "CPU") c = $(i+1) }
			split($4, cc, "="); print s, $1, cc[2], r, c }'
	done | sort -k1,1 -k2,2 -k3,3n -k4,4n | awk '
		{ key = $1 " " $2 " " $3; n[key]++; rps[key, n[key]] = $4; cpu[key, n[key]] = $5; keys[key] = 1 }
		END { for (k in keys) { m = n[k]; split("", c); for (i = 1; i <= m; i++) c[i] = cpu[k, i]
			# req/s are sorted; sort CPU by insertion
			for (i = 1; i <= m; i++) for (j = i + 1; j <= m; j++) if (c[j] < c[i]) { t = c[i]; c[i] = c[j]; c[j] = t }
			printf "MEDIAN %s rounds=%d rps=%s cpu_us=%s\n", k, m, rps[k, int((m + 1) / 2)], c[int((m + 1) / 2)] } }' | sort | tee "$OUT/medians.txt"
	if [ -n "${JOB_OUT:-}" ]; then mkdir -p "$JOB_OUT/watch-ab" && cp -R "$OUT"/report-*.txt "$OUT"/counts-*.txt "$OUT"/medians.txt "$OUT"/series.txt "$OUT"/run.log "$OUT"/runs "$JOB_OUT/watch-ab/"; fi
	;;
*) sed -n '2,/^set -eu/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
