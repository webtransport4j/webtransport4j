#!/usr/bin/env bash
# ==============================================================================
# WebTransport4J - Pre-Commit Hooks Installer
# Configures git to use .githooks and prepares pre-commit environments.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

echo "Setting up WebTransport4J Git pre-commit hooks..."

# Ensure .githooks/pre-commit is executable
if [ -f ".githooks/pre-commit" ]; then
    chmod +x .githooks/pre-commit
fi

# 1. Configure git core.hooksPath to version-controlled .githooks directory
git config core.hooksPath .githooks
echo "✓ Configured git core.hooksPath -> .githooks"

# 2. Also copy to .git/hooks as fallback
if [ -d ".git/hooks" ]; then
    cp .githooks/pre-commit .git/hooks/pre-commit
    chmod +x .git/hooks/pre-commit
    echo "✓ Installed fallback hook to .git/hooks/pre-commit"
fi

# 3. Detect and report pre-commit framework status
if command -v pre-commit > /dev/null 2>&1; then
    echo "✓ Detected 'pre-commit' CLI (active via .githooks/pre-commit delegation)"
elif python3 -m pre_commit --version > /dev/null 2>&1; then
    echo "✓ Detected 'python3 -m pre_commit' module (active via .githooks/pre-commit delegation)"
else
    echo "ℹ 'pre-commit' Python package not installed; native shell checks will run automatically on every commit"
fi

echo ""
echo "🎉 WebTransport4J pre-commit integrity hooks successfully configured!"
echo "All future commits will automatically be checked for security, hygiene, style, and compilation."
