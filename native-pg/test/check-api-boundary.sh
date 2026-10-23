#!/bin/sh
set -eu

# Run through cabal exec to locate the installed package databases.
package_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compiler=${GHC:-ghc}
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT HUP INT TERM

# Keep Cabal's package databases, but expose only the client's dependencies.
if [ -n "${GHC_ENVIRONMENT:-}" ] && [ -f "$GHC_ENVIRONMENT" ]; then
  sed '/^package-id /d' "$GHC_ENVIRONMENT" >"$output_dir/environment"
  export GHC_ENVIRONMENT="$output_dir/environment"
fi

check() {
  "$compiler" -v0 -fno-code -fforce-recomp -XGHC2021 -hide-all-packages \
    -package base -package bytestring -package streaming-io -package native-pg -outputdir "$output_dir" "$1"
}

check "$package_dir/test/api-boundary/Client.hs"


reject_module() {
  printf 'module Hidden where\nimport %s\n' "$1" >"$output_dir/Hidden.hs"
  if check "$output_dir/Hidden.hs" >"$output_dir/error.log" 2>&1; then
    echo "Unexpected public module: $1" >&2
    exit 1
  fi
  if ! grep -Eq 'hidden module|Could not (find|load) module' "$output_dir/error.log"; then
    cat "$output_dir/error.log" >&2
    exit 1
  fi
}

# Removed admission wrappers and redundant settings validation stay unavailable.
for removed_export in Lease tryAcquire tryAcquireSetup release leaseBuffers withBuffers withBuffer validateSettings; do
  printf 'module Hidden where\nimport Network.Streaming.Resources (%s)\n' "$removed_export" >"$output_dir/Hidden.hs"
  if check "$output_dir/Hidden.hs" >"$output_dir/error.log" 2>&1; then
    echo "Unexpected public Resources export: $removed_export" >&2
    exit 1
  fi
  if ! grep -Eq 'does not export|not exported' "$output_dir/error.log"; then
    cat "$output_dir/error.log" >&2
    exit 1
  fi
done

reject_module Database.PostgreSQL.Transaction.Pull
reject_module Database.PostgreSQL.Pipeline
reject_module Database.PostgreSQL.Statement
reject_module Network.Streaming.Resources.Internal
reject_module Network.Streaming.Internal.ListPrefix

for private_module in \
  Authentication Buffer Internal.Context Internal.Session Internal.Transport \
  Internal.ConnectionString Internal.Statement Internal.Diagnostics Internal.Some \
  Internal.Channel Internal.Exchange Internal.HostSelection Internal.Result \
  Internal.Execution Internal.Row \
  Internal.Socket Internal.Socket.Link Internal.Startup \
  Protocol Protocol.Codec Protocol.Codec.Input Protocol.Codec.Output \
  Protocol.Codec.Payload Protocol.Messages Protocol.Messages.AuthBackend \
  Protocol.Messages.AuthFrontend Protocol.Messages.BackendSession \
  Protocol.Messages.Copy Protocol.Messages.FrontendSession \
  Protocol.Messages.MessageType Protocol.Messages.Parse \
  Protocol.Messages.Startup Protocol.Messages.Types; do
  reject_module "Database.PostgreSQL.$private_module"
  case "$private_module" in
    Internal.*) ;;
    *) reject_module "Database.PostgreSQL.Internal.$private_module" ;;
  esac
done

for private_symbol in \
  'Transport (channelClose)' \
  'Transport (OutputBuffer)' \
  'Transport (connectSocket)' \
  'Diagnostics (diagnostics)' \
  'Session (Context)' \
  'Session (sessionFromContext)' \
  'Session (closeConnection)' \
  'Session (isConnectionUsable)' \
  'Transaction (withTransaction)' \
  'Transaction (withTransactionWith)' \
  'Transaction (transactionNoRetryIn)' \
  'Transaction (transactionNoRetryWithIn)' \
  'Session (connect)' \
  'Session (ConnectSettings)' \
  'Session (ConnectionError)' \
  'Session (ProtocolVersion)' \
  'Session (Phase)' \
  'Session (Ready)' \
  'Session (Execution)' \
  'Session (Statement)' \
  'Session (Batch)' \
  'Session (prepare)' \
  'Session (execute)' \
  'Session (batched)' \
  'Session (runBatch)' \
  'Session (awaitNotification)' \
  'Session (executeWithContext)' \
  'Session (Row)' \
  'Session (Column)' \
  'Session (DecodeRow)' \
  'Session (columnBytes)' \
  'Session (columnChunks)' \
  'Session (withColumnStream)' \
  'Session (decodeRow)' \
  'Session (nextColumn)' \
  'Session (backendProcessId)' \
  'Transaction (Transaction(Transaction))' \
  'Transaction (Pipeline(Pipeline))' \
  'Transaction (Result(Result))' \
  'Transaction (Field(Field))' \
  'Transaction (RowDecoder(RowDecoder))' \
  'Transaction (Runtime)' \
  'Transaction (ApplicationFailure)'; do
  printf 'module Hidden where\nimport Database.PostgreSQL.%s\n' "$private_symbol" >"$output_dir/Hidden.hs"
  if check "$output_dir/Hidden.hs" >"$output_dir/error.log" 2>&1; then
    echo "Unexpected public implementation detail: $private_symbol" >&2
    exit 1
  fi
  if ! grep -Eq 'does not export|not exported' "$output_dir/error.log"; then
    cat "$output_dir/error.log" >&2
    exit 1
  fi
done

echo 'Client API compiles; protocol, mutable state and implementation constructors are hidden'
