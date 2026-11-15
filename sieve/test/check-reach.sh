#!/bin/sh
set -eu

package_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compiler=${GHC:-ghc}
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT HUP INT TERM

# No -fdefer-type-errors: a constraint that is missing would become a deferred error, and pass unnoticed.
check() {
  "$compiler" -v0 -fno-code -XGHC2021 -outputdir "$output_dir" \
    -i"$package_dir/src" -i"$package_dir/../streaming-io/src" "$package_dir/test/compile/$1.hs"
}

# A successful control proves that the failure comes from the arrow's parts, not from the environment.
check OneCoreCooperative
if check OneCoreThreads >"$output_dir/OneCoreThreads.log" 2>&1; then
  echo "Expected OneCoreThreads to fail compilation" >&2
  exit 1
fi
if ! grep -q "use a shared cache" "$output_dir/OneCoreThreads.log"; then
  cat "$output_dir/OneCoreThreads.log" >&2
  exit 1
fi
echo "Reach: a cache of one core is used by an arrow whose parts switch only at waits; the arrow of threads is refused with the reason"
