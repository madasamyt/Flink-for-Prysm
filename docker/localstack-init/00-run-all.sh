#!/bin/bash
# Run all LocalStack init scripts in order
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Prysm LocalStack Init ==="
for script in "$SCRIPT_DIR"/0*.sh; do
    if [ "$script" != "$0" ]; then
        echo "--- Running: $script ---"
        bash "$script"
    fi
done
echo "=== Init complete ==="
