#!/usr/bin/env bash
# scripts/check_package.sh — verify the packaged crate
#
# Steps: list the packaged files, check them against the Cargo.toml include
# whitelist, compare the packaged C sources with the files git tracks in c/
# (either direction of drift fails), compare the packaged Rust sources with
# the working tree, build the packaged crate, run a consumer smoke test from
# the unpacked .crate, build the shipped examples from the package, and
# publish-dry-run. Run from the repo root.
#
# usage: scripts/check_package.sh [--allow-dirty]
#   --allow-dirty  pass through to cargo (local runs on uncommitted trees;
#                  CI and the release workflow never pass it)
set -euo pipefail

ALLOW_DIRTY=()
if [[ "${1:-}" == "--allow-dirty" ]]; then
    ALLOW_DIRTY=(--allow-dirty)
elif [[ -n "${1:-}" ]]; then
    echo "usage: $0 [--allow-dirty]" >&2
    exit 2
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

fail() {
    echo "FAIL $1: $2"
    exit 1
}

# 1. list
cargo package --list --locked "${ALLOW_DIRTY[@]}" > "$tmp/list" \
    || fail list "cargo package --list failed"
echo "PASS list"

# 2. allowed: every packaged file is whitelisted
offenders=$(grep -vE \
    '^(\.cargo_vcs_info\.json|Cargo\.lock|Cargo\.toml|Cargo\.toml\.orig|LICENSE|README\.md|RELEASE-NOTES-[0-9.]+\.md|build\.rs|c/LICENSE)$|^src/.+\.rs$|^examples/[^/]+\.rs$|^c/src/.+\.(c|h)$|^c/include/.+\.h$' \
    "$tmp/list" || true)
[[ -z "$offenders" ]] || fail allowed "files outside the whitelist:
$offenders"
echo "PASS allowed"

# 3. c-sources: packaged vendored C == files git tracks in c/
git -C c ls-files -- 'src/*.c' 'src/*.h' 'include/*.h' | sed 's|^|c/|' | sort > "$tmp/c-git"
grep -E '^c/(src|include)/' "$tmp/list" | sort > "$tmp/c-pkg"
if ! diff -u "$tmp/c-git" "$tmp/c-pkg" > "$tmp/c-diff"; then
    fail c-sources "packaged C files != git-tracked C files (git <, package >):
$(cat "$tmp/c-diff")"
fi
echo "PASS c-sources"

# 4. rust-sources: packaged Rust == working-tree Rust (tracked + untracked)
git ls-files --cached --others --exclude-standard -- 'src/*.rs' 'examples/*.rs' | sort > "$tmp/rs-git"
grep -E '^(src|examples)/' "$tmp/list" | sort > "$tmp/rs-pkg"
if ! diff -u "$tmp/rs-git" "$tmp/rs-pkg" > "$tmp/rs-diff"; then
    fail rust-sources "packaged Rust files != working-tree Rust files (tree <, package >):
$(cat "$tmp/rs-diff")"
fi
echo "PASS rust-sources"

# 5. verify: build the packaged crate
cargo package --locked "${ALLOW_DIRTY[@]}" > "$tmp/verify.log" 2>&1 \
    || fail verify "cargo package failed:
$(tail -n 20 "$tmp/verify.log")"
echo "PASS verify"

# 6. smoke: consumer project against the unpacked .crate
version=$(cargo metadata --no-deps --format-version 1 \
    | python3 -I -c 'import json, sys
pkgs = json.load(sys.stdin)["packages"]
print(next(p["version"] for p in pkgs if p["name"] == "mmapq"))')
crate="target/package/mmapq-${version}.crate"
mkdir -p "$tmp/unpacked"
tar -xzf "$crate" -C "$tmp/unpacked" || fail smoke "could not unpack $crate"
mkdir -p "$tmp/consumer/src"
{
    echo '[package]'
    echo 'name = "mmapq-package-smoke"'
    echo 'version = "0.0.0"'
    echo 'edition = "2021"'
    echo 'publish = false'
    echo
    echo '[workspace]'
    echo
    echo '[dependencies]'
    echo "mmapq = { path = \"$tmp/unpacked/mmapq-${version}\" }"
} > "$tmp/consumer/Cargo.toml"
cp scripts/package_smoke_main.rs "$tmp/consumer/src/main.rs"
# stdout and stderr are captured separately: cargo prints warnings for path
# dependencies on stderr, and only the program's stdout is the smoke result.
if ! cargo run --quiet --manifest-path "$tmp/consumer/Cargo.toml" \
        > "$tmp/smoke.out" 2> "$tmp/smoke.err"; then
    fail smoke "consumer build/run failed:
--- stdout ---
$(tail -n 20 "$tmp/smoke.out")
--- stderr ---
$(tail -n 20 "$tmp/smoke.err")"
fi
if [[ "$(cat "$tmp/smoke.out")" != "smoke ok" ]]; then
    fail smoke "unexpected consumer output: $(cat "$tmp/smoke.out")
--- stderr ---
$(tail -n 20 "$tmp/smoke.err")"
fi
(cd "$tmp/unpacked/mmapq-${version}" \
    && cargo build --examples --quiet > "$tmp/examples.log" 2>&1) \
    || fail smoke "examples do not build from the package:
$(tail -n 20 "$tmp/examples.log")"
echo "PASS smoke"

# 7. publish-dry-run
cargo publish --dry-run --locked "${ALLOW_DIRTY[@]}" > "$tmp/publish.log" 2>&1 \
    || fail publish-dry-run "cargo publish --dry-run failed:
$(tail -n 20 "$tmp/publish.log")"
echo "PASS publish-dry-run"
