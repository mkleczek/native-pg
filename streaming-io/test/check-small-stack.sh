#!/bin/sh
# Small stack: a loop of many turns is constant in stack. This fails when a case that runs a loop of 10,000 or 100,000
# turns overflows a stack of one chunk (+RTS -K1K), as the development builds have it: the optics, which run their arrow
# on 10,000 foci, a deadline that is set 100,000 times one after the other, the rounds of a part that waits, and a group that
# spawns 100,000 parts one after the other.
#
# Usage: check-small-stack.sh
# It builds the test suite of streaming-io if it is not built, and runs its cases from the package's directory.
set -eu

package_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$package_dir"

cabal build -v0 streaming-io:test:streaming-io-spec
spec=$(cabal list-bin -v0 streaming-io:test:streaming-io-spec)

"$spec" test/OpticsSpec.hs '[stops 100,000 deadlines]' '[polls at most once for a turn]' '[spawns 100,000 parts]' --ansi never +RTS -K1K -RTS
echo "Small stack: the optics, 100,000 deadlines, the rounds of a part that waits and 100,000 spawns run in a stack of one chunk"
