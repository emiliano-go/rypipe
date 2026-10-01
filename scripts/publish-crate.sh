#!/usr/bin/env bash
# Publish a workspace crate to crates.io, idempotently.
#
# A version that is already on crates.io is reported and skipped instead of
# failing, so re-running a release, or a one-time manual bootstrap publish done
# before the GitHub release, is safe. Cargo waits for the published crate to be
# indexed before returning, so callers can publish dependent crates in sequence.
set -o pipefail

crate="$1"
if [ -z "$crate" ]; then
  echo "usage: $0 <crate>" >&2
  exit 2
fi

log="$(mktemp)"
if cargo publish -p "$crate" 2>&1 | tee "$log"; then
  exit 0
fi

if grep -qE "already (exists|uploaded)" "$log"; then
  echo "::notice::$crate is already published at this version; skipping"
  exit 0
fi

exit 1
