#!/bin/sh
# Small stack: a loop of many turns is constant in stack. This fails when a case that runs a loop of 10,000 or 100,000
# turns overflows a stack of 64 KiB (+RTS -K64K), where the development builds have one chunk of 33 KB (-K1K), which the start of
# this suite needs more than: a loop that is not a tail loop needs megabytes for 100,000 turns. The cases are the optics, which run their arrow
# on 10,000 foci, a deadline that is set 100,000 times one after the other, the rounds of a part that waits, and a group that
# spawns 100,000 parts one after the other, and the tickets: 100,000 values handed over on one ticket, and 100,000 tickets used once.
#
# Usage: check-small-stack.sh
# It builds the test suite of streaming-io if it is not built, and runs its cases from the package's directory.
set -eu

package_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$package_dir"

cabal build -v0 streaming-io:test:streaming-io-spec
spec=$(cabal list-bin -v0 streaming-io:test:streaming-io-spec)

"$spec" test/OpticsSpec.hs '[stops 100,000 deadlines]' '[polls at most once for a turn]' '[spawns 100,000 parts]' '[hands 100,000 values over]' '[makes 100,000 tickets]' --ansi never +RTS -K64K -RTS
echo "Small stack: the optics, 100,000 deadlines, the rounds of a part that waits, 100,000 spawns and 100,000 ticket exchanges run in a stack of 64 KiB"
