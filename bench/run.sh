#!/bin/sh
# Drives one http_server_compare server with wrk and prints one line: req/s, latency, and the server's CPU per request;
# then a RESULT line of key=value fields (microseconds, per request) for scripts. A server built with
# -DHTTP_BENCH_COUNT_ALLOCS=ON also reports allocs/req and handlers/req (operator new calls and Asio handler
# invocations per request); that build is for counting, not timing.
#   sh bench/run.sh KIND REACTORS CONNECTIONS SECONDS [close]
# KIND is asio, reactor, http, httpt, httpr or httpb (see server_compare.cc; httpb is a POST of a 1 KiB body,
# bench/post_body.lua, read as a streamed body), beast (server_beast.cc) or httplib (server_httplib.cc).
# BIN names the server binary (default: the one in $BUILD, default build, that serves KIND), LABEL what to print for it
# (default its file name); `close` sends Connection: close, one request per connection. Needs wrk on PATH.
set -u
KIND=$1; R=$2; C=$3; S=$4; CLOSE=${5:-}
BUILD=${BUILD:-build}
case $KIND in
	beast) DEFAULT_BIN=$BUILD/http_server_beast ;;
	httplib) DEFAULT_BIN=$BUILD/http_server_httplib ;;
	*) DEFAULT_BIN=$BUILD/http_server_compare ;;
esac
BIN=${BIN:-$DEFAULT_BIN}
LABEL=${LABEL:-$(basename "$BIN")}
PORT=$((20000 + $$ % 20000))
HDR=""
[ "$CLOSE" = close ] && HDR='-H Connection:close'
[ "$KIND" = httpb ] && HDR="$HDR -s $(dirname "$0")/post_body.lua"
LOG=${TMPDIR:-/tmp}/http_server_compare.$$.log
"$BIN" "$KIND" $PORT $R > $LOG 2> $LOG.err &
SP=$!
i=0; while ! grep -q ready $LOG 2>/dev/null; do sleep 0.1; i=$((i+1)); [ $i -gt 100 ] && { echo "server did not start"; kill $SP; exit 1; }; done
# warm up, then start the CPU clock
wrk -t1 -c$C -d1s $HDR http://127.0.0.1:$PORT/ > /dev/null 2>&1
: > $LOG.err
kill -USR2 $SP; sleep 0.2
T=$C; [ $T -gt 2 ] && T=2
OUT=$(wrk -t$T -c$C -d${S}s --latency $HDR http://127.0.0.1:$PORT/ 2>&1)
kill -USR2 $SP; sleep 0.2
kill $SP; wait $SP 2>/dev/null
REQS=$(echo "$OUT" | awk '/requests in/ {print $1}')
RPS=$(echo "$OUT" | awk '/Requests\/sec/ {print $2}')
L50=$(echo "$OUT" | awk '$1=="50%" {print $2}')
L99=$(echo "$OUT" | awk '$1=="99%" {print $2}')
AVG=$(echo "$OUT" | awk '$1=="Latency" && $2 ~ /[0-9]/ {print $2; exit}')
ERR=$(echo "$OUT" | grep -E "Socket errors|Non-2xx" | tr '\n' ' ')
CPU=$(awk '/^RUSAGE/ { u[n]=$2; s[n]=$3; n++ } END { if (n >= 2) printf "%.2f us/req (user %.2f sys %.2f)", ((u[n-1]-u[0]) + (s[n-1]-s[0])) / REQ, (u[n-1]-u[0]) / REQ, (s[n-1]-s[0]) / REQ }' REQ="$REQS" $LOG.err)
COUNTS=$(awk '/^ALLOCS/ { a[na++]=$2 } /^HANDLERS/ { h[nh++]=$2 } END { if (na >= 2 && nh >= 2) printf "%.2f %.2f", (a[na-1]-a[0]) / REQ, (h[nh-1]-h[0]) / REQ }' REQ="$REQS" $LOG.err)
printf "%-6s %-24s R=%s C=%-3s %-9s req/s %9s  avg %8s  p50 %8s  p99 %8s  reqs %7s  server CPU %s%s %s\n" "$KIND" "$LABEL" "$R" "$C" "${CLOSE:-keepalive}" "$RPS" "$AVG" "$L50" "$L99" "$REQS" "$CPU" "${COUNTS:+  allocs/req ${COUNTS% *}  handlers/req ${COUNTS#* }}" "$ERR"
us() { case $1 in *us) echo "${1%us}" ;; *ms) echo "${1%ms}" | awk '{printf "%.1f", $1 * 1000}' ;; *s) echo "${1%s}" | awk '{printf "%.1f", $1 * 1000000}' ;; esac; }
CPUUS=$(echo "$CPU" | awk '{print $1}')
NC=${COUNTS:-na na}
printf "RESULT kind=%s bin=%s reactors=%s conns=%s reqs=%s rps=%s p50_us=%s p99_us=%s cpu_us_per_req=%s allocs_per_req=%s handlers_per_req=%s\n" \
	"$KIND" "$LABEL" "$R" "$C" "$REQS" "$RPS" "$(us "$L50")" "$(us "$L99")" "${CPUUS:-na}" "${NC%% *}" "${NC#* }"
rm -f $LOG $LOG.err
