#!/bin/bash
# -----------------------------------------------------------------------------
# Purpose: Custom entrypoint helper for container startup extensions.
# -----------------------------------------------------------------------------
set -euo pipefail

# Custom entrypoint logic goes here
exec "$@"