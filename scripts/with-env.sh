#!/usr/bin/env bash
# Run a command after restoring the Windows environment MSYS make strips (see winenv.sh).
# Usage: bash scripts/with-env.sh docker compose up -d
. "$(dirname "${BASH_SOURCE[0]}")/winenv.sh"
exec "$@"
