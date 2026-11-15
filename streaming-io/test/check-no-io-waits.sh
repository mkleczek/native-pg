#!/bin/sh
# One way to wait: a program waits with block, tickets and within, which the
# arrow it runs in provides. This fails when a module other than an arrow's
# own implementation waits in IO: threadWaitRead, threadWaitWrite and the
# other threadWait functions, atomically, takeMVar, readMVar, threadDelay,
# timeout or registerFd.
#
# Usage: check-no-io-waits.sh [FILE...]
# The files are paths from the repository's root. Without any, it checks every
# module in the src directories of the packages.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

if [ "$#" -gt 0 ]; then
  files=$*
else
  files=$(find streaming-io/src native-pg/src native-pg-tls/src directions/src fulmen/src sieve/src -name '*.hs' | sort)
fi

# What is allowed where: a file, what it may do (a name below, or * for all of
# them), and why.
allow_list='
streaming-io/src/Network/Streaming/Threads.hs * the Threads arrow, whose waits are IO waits
streaming-io/src/Network/Streaming/Internal/Scheduler.hs * the own wait of the scheduler
streaming-io/src/Network/Streaming/Loop.hs atomically the own wait of the Loop arrow on its watches, and its cancellation flags
streaming-io/src/Network/Streaming/Simulated.hs atomically the tries of a simulated link, which never wait
streaming-io/src/Network/Streaming/Watch.hs * the readiness registrations that the arrows wait on, and the wait of the Threads arrow
directions/src/Network/Directions/PingPong.hs threadDelay a prototype program that sleeps in IO as a stand-in for slow work
'

# What is looked for: a name, a tab, and an extended regular expression that
# matches a line of code. Comments are removed first.
patterns='
threadWait	(^|[^A-Za-z0-9_])threadWait[A-Za-z]*([^A-Za-z0-9_]|$)
atomically	(^|[^A-Za-z0-9_])atomically([^A-Za-z0-9_]|$)
takeMVar	(^|[^A-Za-z0-9_])takeMVar([^A-Za-z0-9_]|$)
readMVar	(^|[^A-Za-z0-9_])readMVar([^A-Za-z0-9_]|$)
threadDelay	(^|[^A-Za-z0-9_])threadDelay([^A-Za-z0-9_]|$)
timeout	(^|[^A-Za-z0-9_])timeout([^A-Za-z0-9_]|$)
registerFd	(^|[^A-Za-z0-9_])registerFd([^A-Za-z0-9_]|$)
'

# The code of each line without its comments, so that the output has a line for
# each line of the input. Block comments nest; "--" begins a line comment
# unless an operator character follows it. String literals are dropped.
strip='
FNR == 1 { depth = 0 }
{
  line = $0; out = ""; i = 1; n = length(line)
  while (i <= n) {
    two = substr(line, i, 2)
    if (depth > 0) {
      if (two == "-}") { depth--; i += 2 } else if (two == "{-") { depth++; i += 2 } else i++
    } else if (two == "{-") { depth++; i += 2 }
    else if (two == "--" && (i == 1 || substr(line, i - 1, 1) ~ /[ \t(\[,;]/) && (i + 1 == n || substr(line, i + 2, 1) !~ /[!#$%&*+.\/<=>?@\\^|~:]/)) break
    else if (substr(line, i, 3) == "\x27\"\x27") { out = out "\"\""; i += 3 }
    else if (substr(line, i, 1) == "\"") {
      # a string literal: dropped, with its escapes
      i++
      while (i <= n && substr(line, i, 1) != "\"") i += (substr(line, i, 1) == "\\") ? 2 : 1
      out = out "\"\""; i++
    }
    else { out = out substr(line, i, 1); i++ }
  }
  print out
}'

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
: >"$tmp/report"
tab=$(printf '\t')

allowed() { # file name
  printf '%s\n' "$allow_list" | while read -r path what _; do
    if [ "$path" = "$1" ] && { [ "$what" = '*' ] || [ "$what" = "$2" ]; }; then echo yes; fi
  done | grep -q yes
}

for file in $files; do
  [ -f "$file" ] || { echo "No such file: $file" >&2; exit 2; }
  awk "$strip" "$file" >"$tmp/code"
  while IFS="$tab" read -r name regex; do
    [ -n "$name" ] || continue
    for number in $(grep -nE -e "$regex" "$tmp/code" | cut -d: -f1); do
      allowed "$file" "$name" || printf '%s:%s: %s   [%s]\n' "$file" "$number" "$(sed -n "${number}p" "$file")" "$name" >>"$tmp/report"
    done
  done <<EOF
$patterns
EOF
done

if [ -s "$tmp/report" ]; then
  sort -u "$tmp/report" >&2
  echo 'A program waits with block, tickets and within: only the implementation of an arrow waits in IO' >&2
  exit 1
fi
echo 'No IO waits: only the implementations of the arrows wait in IO'
