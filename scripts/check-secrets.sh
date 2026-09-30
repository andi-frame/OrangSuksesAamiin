#!/usr/bin/env bash
# Verify that no credential is hard-coded or committed (Problem 3 / "Ketentuan Implementasi").
#
#  1. .env must never be tracked; .env.example must exist and list the variables
#  2. gitleaks over the working tree AND the git history (uses the local binary,
#     or falls back to the official container image)
#  3. a plain grep for secret-looking assignments in the Go sources
set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/winenv.sh
rc=0

echo "== 1. .env handling"
if git ls-files --error-unmatch .env >/dev/null 2>&1; then echo "FAIL: .env is tracked by git"; rc=1; else echo "PASS: .env is not tracked"; fi
git check-ignore -q .env && echo "PASS: .env is git-ignored" || { echo "FAIL: .env is not ignored"; rc=1; }
if [ -f .env.example ]; then echo "PASS: .env.example present ($(grep -cE '^[A-Z0-9_]+=' .env.example) variables)"; else echo "FAIL: .env.example missing"; rc=1; fi
if [ -n "$(git log --all --diff-filter=A --name-only --format= -- .env 2>/dev/null)" ]; then echo "FAIL: .env exists somewhere in the git history"; rc=1; else echo "PASS: .env never committed"; fi

echo "== 2. gitleaks (working tree + history)"
if command -v gitleaks >/dev/null 2>&1; then
  GL=(gitleaks)
  SRC="."
elif command -v docker >/dev/null 2>&1; then
  GL=(env MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd):/repo" zricethezav/gitleaks:latest)
  SRC="/repo"
fi
if [ -n "${GL[*]:-}" ]; then
  # once over the git history (commits) and once over the working tree (uncommitted files too)
  if "${GL[@]}" detect --source "$SRC" --config "$SRC/.gitleaks.toml" --redact --no-banner \
     && "${GL[@]}" detect --no-git --source "$SRC" --config "$SRC/.gitleaks.toml" --redact --no-banner; then
    echo "PASS: gitleaks clean (git history + working tree)"
  else
    echo "FAIL: gitleaks found leaks (or could not run)"; rc=1
  fi
else
  echo "SKIP: neither gitleaks nor docker available"
fi

echo "== 3. secret-looking literals in Go code"
hits="$(grep -rniE '[a-z_]*(password|passwd|secret|api[_-]?key|token)[[:space:]]*(:=|=)[[:space:]]*"[^"]{8,}"' --include='*.go' --exclude='*_test.go' . || true)"
if [ -n "$hits" ]; then echo "FAIL: possible hard-coded credentials:"; echo "$hits"; rc=1; else echo "PASS: no credential literals in non-test Go code"; fi

exit $rc
