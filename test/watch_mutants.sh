#!/bin/sh
# Mutants of the disconnect watch (ConnectionInput in http_asio.h): each breaks one claim, and
# readahead_test must fail against it. Prints one line per mutant and exits 1 if any survives.
#   sh test/watch_mutants.sh [BUILD_DIR]     (default build-mutants; configured if missing)
set -u
cd "$(dirname "$0")/.." || exit 1
BUILD=${1:-build-mutants}
[ -f "$BUILD/CMakeCache.txt" ] || cmake -S . -B "$BUILD" -DCMAKE_BUILD_TYPE=Debug > /dev/null || exit 1
cp http_asio.h "$BUILD/http_asio.h.orig"
trap 'cp "$BUILD/http_asio.h.orig" http_asio.h' EXIT INT TERM

# mutate NAME OLD NEW: OLD (one exact line fragment of http_asio.h) becomes NEW.
survived=0
mutate() {
	cp "$BUILD/http_asio.h.orig" http_asio.h
	python3 - "$2" "$3" <<'EOF' || { echo "mutant $1: the fragment is not in http_asio.h"; survived=1; return; }
import sys
old, new = sys.argv[1], sys.argv[2]
s = open("http_asio.h").read()
if s.count(old) != 1:
    sys.exit(1)
open("http_asio.h", "w").write(s.replace(old, new))
EOF
	if ! cmake --build "$BUILD" --target readahead_test > "$BUILD/mutant.log" 2>&1; then
		echo "mutant $1: does not compile"; survived=1; return
	fi
	if timeout 120 "$BUILD/readahead_test" >> "$BUILD/mutant.log" 2>&1; then
		echo "mutant $1: SURVIVED"; survived=1
	else
		echo "mutant $1: killed ($(grep -m1 -E 'FAIL' "$BUILD/mutant.log" | cut -c1-140))"
	fi
}

# The pending read never resumes the reader waiting on it.
mutate no-resume '		resume_waiter();   // last: the reader may arm the next read' '		waiter_ = {};'
# A reader waiting on the pending read ignores its token.
mutate no-cancel '			cancel_.emplace(token_, Cancel{&input_.stream_});' '			(void)token_;'
# engage() does not decide at once: it only arms the wait.
mutate engage-lazy '			if (receive_now(error, size)) { completed(error, size); return; }' '			(void)error; (void)size;'
# engage() keeps what it read but does not end the watch on data.
mutate engage-data '			if (n > 0) { error = {}; size = static_cast<std::size_t>(n); return true; }' '			if (n > 0) { error = asio::error::connection_reset; size = 0; return true; }'
# EOF from the read that does not wait is taken for data.
mutate eof-as-data '			if (n == 0) { error = asio::error::eof; size = 0; return true; }' '			if (n == 0) { return false; }'

cp "$BUILD/http_asio.h.orig" http_asio.h
cmake --build "$BUILD" --target readahead_test > "$BUILD/mutant.log" 2>&1 && "$BUILD/readahead_test" > /dev/null 2>&1 \
	|| { echo "the unmutated header fails readahead_test"; exit 1; }
echo "unmutated: readahead_test passes"
exit $survived
