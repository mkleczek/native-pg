#!/bin/sh
set -eu

package_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compiler=${GHC:-ghc}
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT HUP INT TERM

# No -fdefer-type-errors: a constraint that is missing would become a deferred error, and pass unnoticed.
check() {
  "$compiler" -v0 -fno-code -XGHC2021 -outputdir "$output_dir" \
    -i"$package_dir/src" "$package_dir/test/compile/$1.hs"
}

# A successful control proves failures come from the waiting classes, not the environment.
check UndoingSteps
check UndoingEach
while read -r fixture class; do
  if check "$fixture" >"$output_dir/$fixture.log" 2>&1; then
    echo "Expected $fixture to fail compilation" >&2
    exit 1
  fi
  if ! grep -Eq "Could not deduce[^A-Za-z]+$class p" "$output_dir/$fixture.log"; then
    cat "$output_dir/$fixture.log" >&2
    exit 1
  fi
done <<EOF2
StepAwaits Exchange
StepBlocks Block
StepGivesWay GiveWay
CleanupCollects Inboxes
StepWithin Within
StepEachAwaits Exchange
StepHands Exchange
StepWatches Exchange
EOF2
echo "Steps: undoing takes steps and cleanups that can't wait, and each of them; steps that wait are rejected"
