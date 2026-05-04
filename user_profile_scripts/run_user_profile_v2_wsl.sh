#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

bash "${PROJECT_ROOT}/user_profile_project_v2/bin/run_retention_model_wsl.sh"
bash "${PROJECT_ROOT}/user_profile_project_v2/bin/verify_outputs_wsl.sh"
