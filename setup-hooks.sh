#!/usr/bin/env bash
# Setup Git Hooks for File Structure Validation
#
# Configures Git to use hooks from .githooks/ directory
# Run once after cloning the repository:
#   bash setup-hooks.sh
#
# After setup, hooks are mandatory and run automatically before each commit
# An automatic recovery hook also runs on git checkout to re-configure if needed
# Works on: Windows (Git Bash), macOS, Linux

set -e

REPO_ROOT="${1:-.}"

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║  QuantaCandle Git Hooks Setup                              ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Verify it's a git repository
if [ ! -d "$REPO_ROOT/.git" ]; then
    echo "ERROR: Not a Git repository" >&2
    exit 1
fi

cd "$REPO_ROOT"

# Configure Git to use .githooks directory
git config core.hooksPath .githooks
echo "✓ Configured Git to use .githooks/"

# Install post-checkout recovery hook (runs on every checkout to auto-configure)
HOOKS_DIR=".git/hooks"
if [ ! -d "$HOOKS_DIR" ]; then
    mkdir -p "$HOOKS_DIR"
fi
cp .githooks/post-checkout "$HOOKS_DIR/post-checkout" 2>/dev/null || true
chmod +x "$HOOKS_DIR/post-checkout" 2>/dev/null || true
echo "✓ Installed automatic recovery hook"

# Make the pre-commit hook executable
if [ -f ".githooks/pre-commit" ]; then
    chmod +x .githooks/pre-commit
    echo "✓ Pre-commit hook is executable"
fi

echo ""
echo "Setup Complete!"
echo ""
echo "File Structure Validation is now MANDATORY:"
echo "  • Triggered: Before every git commit (automatic)"
echo "  • Validates: All staged .cs files"
echo "  • Policy: One type per file, filename matches type name"
echo ""
echo "Recovery:"
echo "  • If setup is missed, running any git checkout will auto-configure"
echo ""
echo "Example:"
echo "  class MyClass → file MUST be named: MyClass.cs"
echo ""

exit 0
