#!/usr/bin/env bash
# Sourced by scripts/lib.sh and scripts/with-env.sh.
#
# Windows + MSYS make hands recipes a stripped-down environment (no USERPROFILE,
# LOCALAPPDATA, TEMP ...). Native Windows programs (go, docker) then cannot find
# their cache, temp dir or CLI plugins ("docker: 'compose' is not a docker command").
# When that situation is detected, restore the variables from $HOME. Everywhere
# else (Linux, macOS, CI, a normal Git Bash) this file does nothing.
if command -v cygpath >/dev/null 2>&1 && [ -z "${USERPROFILE:-}" ] && [ -n "${HOME:-}" ]; then
  USERPROFILE="$(cygpath -w "$HOME")"
  export USERPROFILE
  export HOMEDRIVE="${USERPROFILE%%\\*}"
  export LOCALAPPDATA="$USERPROFILE\\AppData\\Local" APPDATA="$USERPROFILE\\AppData\\Roaming"
  export TEMP="$LOCALAPPDATA\\Temp" TMP="$LOCALAPPDATA\\Temp"
  export DOCKER_CONFIG="$USERPROFILE\\.docker"
  export SYSTEMROOT="${SYSTEMROOT:-C:\\Windows}" ProgramFiles="${ProgramFiles:-C:\\Program Files}" ProgramData="${ProgramData:-C:\\ProgramData}"
fi
