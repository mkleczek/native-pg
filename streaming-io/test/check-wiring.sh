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

# A successful control proves failures come from the wiring, not the environment: it has bare names and bindings of a
# port to a name, a name that is bound to two ports, a step that two producers feed, a path into the output of a step, a
# step that gives nothing, and a flow over a record that is not known. A name that the flow doesn't have, an output that
# a step doesn't give, a port that is not bound, a port that is bound twice, and a name that the flow has already are
# rejected, each with its own message. A choice is checked the same way: the control has a choice of three cases whose
# branches bind different names, with a name for its result and without, and the errors of a step hold in a branch, which
# has the value of its own case and no other, and a case that the step doesn't have is rejected.
check WiringFlow
check WiringChoice
while read -r fixture message; do
  if check "$fixture" >"$output_dir/$fixture.log" 2>&1; then
    echo "Expected $fixture to fail compilation" >&2
    exit 1
  fi
  if ! grep -Fq "$message" "$output_dir/$fixture.log"; then
    cat "$output_dir/$fixture.log" >&2
    exit 1
  fi
done <<EOF2
WiringNoName The flow has no name "k"
WiringNoOutput The step "a1" has no output "z"
WiringUnbound Input "j" of the step isn't bound
WiringBoundTwice Input "i" of the step is bound twice
WiringRepeated The flow already has the name "a1"
WiringRepeatedInput The flow already has the name "i"
WiringChoiceUnbound Input "j" of the step isn't bound
WiringChoiceNoName The flow has no name "k"
WiringChoiceBoundTwice Input "i" of the step is bound twice
WiringChoiceOtherCase The flow has no name "small"
WiringChoiceNoCase The choice has no case "big"
EOF2
echo "Wiring: steps and choices are wired by name; a missing or repeated name, a missing output, an unbound port are rejected"
