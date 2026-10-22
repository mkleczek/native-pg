#!/bin/sh
set -eu

package_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compiler=${GHC:-ghc}
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT HUP INT TERM

check() {
  "$compiler" -v0 -fno-code -XGHC2021 -outputdir "$output_dir" \
    -i"$package_dir/src" "$package_dir/test/compile/$1.hs"
}

# A successful control proves failures come from the bounds, not the environment.
check PositiveConstants
check PositiveBuffers
for fixture in Zero TooLarge IntBufferCapacity IntBufferBudget IntBlockSize; do
  if check "$fixture" >"$output_dir/$fixture.log" 2>&1; then
    echo "Expected $fixture to fail compilation" >&2
    exit 1
  fi
  if ! grep -Eq "Cannot satisfy: .* <=|Couldn't match (expected )?type" "$output_dir/$fixture.log"; then
    cat "$output_dir/$fixture.log" >&2
    exit 1
  fi
done
echo "PositiveInt constants: minimum/maximum compile; zero/overflow and raw Int buffer budgets/sizes rejected"
