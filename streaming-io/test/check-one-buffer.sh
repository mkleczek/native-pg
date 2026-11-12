#!/bin/sh
# One buffer: memory is allocated by Network.Streaming.Buffer alone. Everything
# else works in views of a Buffer (a buffer, an offset and a length) and in
# cursor state. This fails when another module allocates bytes
# (mallocForeignPtrBytes, mallocPlainForeignPtrBytes, callocBytes, mallocBytes,
# mallocByteString, create or unsafeCreate of Data.ByteString.Internal), or
# declares a ForeignPtr Word8 field in a data or newtype declaration.
#
# Usage: check-one-buffer.sh [FILE...]
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
streaming-io/src/Network/Streaming/Buffer.hs * the one buffer type
streaming-io/src/Network/Streaming/Reader.hs create the copying atom: bytes makes an owned ByteString of a value
'

# What is looked for: a name, a tab, and an extended regular expression that
# matches a line of code. Comments are removed first, and a line of a data or
# newtype declaration that has a ForeignPtr Word8 begins with @@field@@.
patterns='
mallocForeignPtrBytes	(^|[^A-Za-z0-9_])mallocForeignPtrBytes([^A-Za-z0-9_]|$)
mallocPlainForeignPtrBytes	(^|[^A-Za-z0-9_])mallocPlainForeignPtrBytes([^A-Za-z0-9_]|$)
callocBytes	(^|[^A-Za-z0-9_])callocBytes([^A-Za-z0-9_]|$)
mallocBytes	(^|[^A-Za-z0-9_])mallocBytes([^A-Za-z0-9_]|$)
mallocByteString	mallocByteString([^A-Za-z0-9_]|$)
create	([A-Za-z]+\.)+(create|createUptoN|createAndTrim|unsafeCreate|unsafeCreateUptoN)([^A-Za-z0-9_]|$)
create	^import .*Data\.ByteString(\.Internal)?[^(]*\(.*[ (,](create|createUptoN|createAndTrim|unsafeCreate|unsafeCreateUptoN)[ ),]
unsafeCreate	(^|[^A-Za-z0-9_])unsafeCreate(UptoN)?([^A-Za-z0-9_]|$)
ForeignPtr-Word8-field	^@@field@@
'

# The code of each line without its comments, so that the output has a line for
# each line of the input. Block comments nest; "--" begins a line comment
# unless an operator character follows it.
strip='
FNR == 1 { depth = 0; decl = 0 }
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
  if (out ~ /^(data|newtype)[ \t]/) decl = 1
  else if (out ~ /^[^ \t|,}]/ && out !~ /^deriving/) decl = 0
  if (decl && out ~ /ForeignPtr[ \t]+Word8/) out = "@@field@@" out
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
  echo 'Memory is allocated by Network.Streaming.Buffer alone: work in views of a Buffer' >&2
  exit 1
fi
echo 'One buffer: only Network.Streaming.Buffer allocates bytes or holds a ForeignPtr Word8'
