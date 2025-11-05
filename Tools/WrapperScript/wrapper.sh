#!/usr/bin/env bash

# === Cross-platform .NET CLI wrapper ===
# Works on macOS and Linux
# Executes: dotnet <scriptname>.dll "$@"
# With .dll located next to this wrapper script

set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$(basename "${BASH_SOURCE[0]}")"

# Extract base name: remove .sh, .bash, .cmd, etc.
BASE_NAME="${SCRIPT_PATH%%.*}"

# Look for the .dll in the same directory as this script
DLL_PATH="${SCRIPT_DIR}/${BASE_NAME}.dll"

# Check if dotnet is available
if ! command -v dotnet >/dev/null 2>&1; then
    echo "Error: 'dotnet' command not found in PATH." >&2
    echo "Please install the .NET SDK: https://dotnet.microsoft.com/download" >&2
    exit 1
fi

# Check if the DLL exists
if [[ ! -f "$DLL_PATH" ]]; then
    echo "Error: Expected DLL not found: $DLL_PATH" >&2
    echo "Make sure ${BASE_NAME}.dll is in the same directory as this wrapper." >&2
    exit 1
fi

# Execute the .NET app
exec dotnet "$DLL_PATH" "$@"