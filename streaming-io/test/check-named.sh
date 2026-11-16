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

# A successful control proves failures come from the names, not the environment. A name that a record has twice is no
# error where it is read: the newer field, which is the first, shadows the older. A choice is checked the same way: the
# control puts a value in each of three cases and takes them apart, a case that the choice doesn't have is rejected, and so
# is a name that two of its cases have.
check NamedFields
check NamedRepeated
check NamedPolymorphic
check ChoiceThree
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
NamedMisspelt The record has no field "count"
NamedTwice The record has more than one field named "size"
NamedTwiceApart The record has more than one field named "a"
ChoiceMisspelt The choice has no case "smal"
ChoiceBranchMisspelt The choice has no case "huge"
ChoiceCaseMisspelt The choice has no case "faild"
ChoiceTwice The choice has more than one case named "small"
EOF2
echo "Named: a record is read by name from any step that names its fields, and a choice is taken apart by the names of its cases; a name that a record lacks, that newRecord has twice, that a choice lacks, or that two of its cases have, is rejected"
