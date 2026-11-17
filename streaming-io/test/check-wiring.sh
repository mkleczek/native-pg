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
# has the value of its own case and no other, and a case that the step doesn't have is rejected. A list of names that
# takes the output of a step apart is checked the same way: the control takes a record apart partly and with a field
# under another name, takes pairs apart by position with a part dropped, and reads the names in a branch of a choice and
# in a flow over a record that is not known; a field that the record doesn't have, a field that the list left out, too
# few and too many names for a pair, a name that the list has twice or the flow has already, a field named for a pair,
# and a list for an output that is neither are rejected. A pattern that nests is checked the same way: the control keeps a
# whole and takes it apart, takes a field of a record apart by a list, keeps a field under a name and takes it apart, takes a
# part of a pair apart, gives the last name of a pair what is left, and takes the value of the case of a branch apart by a
# name, a list and a name that takes a record apart too; the names of a pattern are the names of a branch, a case with no
# pattern has no name from it, and a pattern with a field that a part doesn't have, a list for a part that is neither a record
# nor a pair, a name that the pattern has twice or the flow has already, a field named in a list for a pair, too many names for
# a pair that is a part, a pattern for a binding or for the output of a step as a field are rejected, and each error says where
# in the pattern it is. A step that takes its input whole is checked the same way: the control has a name for a step that takes a
# plain value, with a name for its output and without, a path, a pair of names and a triple of names, some of them paths, the
# empty list and a list of one name, names and a pair in the branches of a choice, and a flow over a record that is not known; a
# port in a name and in a tuple, a name that the flow doesn't have in a name and in a tuple, and a pattern in a tuple are
# rejected.
check WiringFlow
check WiringChoice
check WiringPattern
check WiringNested
check WiringWhole
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
WiringPatternNoField The output of the step has no field "w"
WiringPatternDropped The flow has no name "y"
WiringPatternTooMany The pattern has too many names for the output of the step
WiringPatternTwice The pattern has the name "z" twice
WiringPatternField a pair has none
WiringPatternRepeated The flow already has the name "i"
WiringPatternWhole neither a record nor a pair
WiringNestedNoField in "socket": The output of the step has no field "w"
WiringNestedWhole in "socket.fd": The output of the step is neither a record nor a pair
WiringNestedTwice The pattern has the name "fd" twice
WiringNestedRepeated The flow already has the name "i"
WiringNestedField in "_": The output of the step is a pair, taken apart by position: "x" =: "q" names a field, and a pair has none
WiringNestedTooMany in "_": The pattern has too many names for the output of the step
WiringNestedBinding takes a value apart, which is for the left of <:
WiringNestedBindingAs takes a value apart, which is for the left of <:
WiringNestedFieldOfStep The output of a step is not a field: "socket" =: has nothing to take a field of
WiringBranchNoValue The flow has no name "small"
WiringBranchNoField in "huge": The value of the case has no field "up"
WiringBranchWhole in "small": The value of the case is neither a record nor a pair
WiringBranchTwice The pattern has the name "x" twice
WiringBranchRepeated The flow already has the name "i"
WiringWholePort A step that takes its input whole has no port
WiringTuplePort A step that takes its input whole has no port
WiringWholeNoName The flow has no name "k"
WiringTupleNoName The flow has no name "k"
WiringTuplePattern takes a value apart, which is for the left of <:
EOF2
echo "Wiring: steps, choices, lists of names and names taken whole are wired by name; a missing or repeated name, a missing output, an unbound port, a port in a name or a tuple are rejected"
