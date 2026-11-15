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
# error where it is read: the newer field, which is the first, shadows the older.
check NamedFields
check NamedRepeated
check NamedPolymorphic
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
EOF2
echo "Named: a record is read by name from any step that names its fields; a name that it lacks, or that newRecord has twice, is rejected"
