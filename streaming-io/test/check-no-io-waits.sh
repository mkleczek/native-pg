#!/bin/sh
# One way to wait: a program waits with block, tickets and within, which the
# arrow it runs in provides. This fails when a module other than an arrow's
# own implementation waits in IO: threadWaitRead, threadWaitWrite and the
# other threadWait functions, atomically, takeMVar, readMVar, threadDelay,
# timeout or registerFd.
#
# Usage: check-no-io-waits.sh [FILE...]
# The files are paths from the repository's root. Without any, it checks the
# runtime's modules.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

if [ "$#" -gt 0 ]; then
  files=$*
else
  files='
    streaming-io/src/Network/Streaming/Classes.hs
    streaming-io/src/Network/Streaming/Combinators.hs
    streaming-io/src/Network/Streaming/Threads.hs
    streaming-io/src/Network/Streaming/Delimited.hs
    streaming-io/src/Network/Streaming/Compiled.hs
    streaming-io/src/Network/Streaming/Loop.hs
    streaming-io/src/Network/Streaming/Simulated.hs
    streaming-io/src/Network/Streaming/Reader.hs
    streaming-io/src/Network/Streaming/Pool.hs
    streaming-io/src/Network/Streaming/Internal/Scheduler.hs
    streaming-io/src/Network/Streaming/Internal/Timers.hs
    streaming-io/src/Network/Streaming/Internal/Tickets.hs
    streaming-io/src/Network/Streaming/Internal/Cancel.hs
    streaming-io/src/Network/Streaming/Internal/Atomic.hs
    streaming-io/src/Network/Streaming/Internal/Socket.hs
  '
fi

# What is allowed where: a file, what it may do (a name below, or * for all of
# them), and why.
allow_list='
streaming-io/src/Network/Streaming/Threads.hs * the Threads arrow, whose waits are IO waits
streaming-io/src/Network/Streaming/Internal/Scheduler.hs * the own wait of the scheduler
streaming-io/src/Network/Streaming/Loop.hs atomically the own wait of the Loop arrow on its watches, and its cancellation flags
streaming-io/src/Network/Streaming/Simulated.hs atomically the tries of a simulated link, which never wait
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
# unless an operator character follows it.
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
