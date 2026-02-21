#!/usr/bin/env bash
# Setup Git Hooks for File Structure Validation
#
# Configures Git to use hooks from .githooks/ directory
# Run once after cloning the repository:
#   bash setup-hooks.sh
#
# After setup, hooks run automatically before each commit.
# A recovery hook is also installed into .git/hooks/post-checkout.
# Works on: Windows (Git Bash), macOS, Linux

set -euo pipefail

REPO_ROOT="${1:-.}"

echo ""
echo "============================================================"
echo "QuantaCandle Git Hooks Setup"
echo "============================================================"
echo ""

# Verify it's a git repository
if [ ! -d "$REPO_ROOT/.git" ]; then
  echo "ERROR: Not a Git repository" >&2
  exit 1
fi

cd "$REPO_ROOT"

# Configure Git to use .githooks directory
git config core.hooksPath .githooks
echo "OK: Configured Git to use .githooks/"

# Install post-checkout recovery hook (runs even if core.hooksPath is later missing)
HOOKS_DIR=".git/hooks"
mkdir -p "$HOOKS_DIR"
cp .githooks/post-checkout "$HOOKS_DIR/post-checkout" 2>/dev/null || true
chmod +x "$HOOKS_DIR/post-checkout" 2>/dev/null || true
echo "OK: Installed recovery hook (.git/hooks/post-checkout)"

# Make the pre-commit hook executable
if [ -f ".githooks/pre-commit" ]; then
  chmod +x .githooks/pre-commit
  echo "OK: Pre-commit hook is executable"
fi

echo ""
echo "Setup Complete!"
echo ""
echo "File Structure Validation is now enabled:"
echo "  - Triggered: Before every git commit (automatic)"
echo "  - Validates: All staged .cs files"
echo "  - Policy: One type per file, filename matches type name"
echo ""
echo "Recovery:"
echo "  - If core.hooksPath is later cleared, the next git checkout restores it"
echo ""
echo "Example:"
echo "  class MyClass -> file MUST be named: MyClass.cs"
echo ""

exit 0
