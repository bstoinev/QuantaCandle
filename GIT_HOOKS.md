# Git Hooks - File Structure Validation

This repo enforces a C# file structure policy using Git hooks. After a one-time setup per developer, validation runs automatically before every commit.

## Policy

- **One type per file**: Each `.cs` file must contain at most one top-level type (class, interface, enum, record, struct, or delegate)
- **Filename matches type name**: If a type is declared, the filename must exactly match the type name

Nested types (e.g., `private class Helper { }` inside your main class) are allowed.

### Examples

**Valid:**
```csharp
// File: MyClass.cs
public class MyClass { }
```

```csharp
// File: IMyInterface.cs
public interface IMyInterface { }
```

**Invalid:**
```csharp
// File: Class1.cs (wrong filename)
public class MyClass { }
```

```csharp
// File: MyTypes.cs (contains two types)
public class MyClass { }
public class AnotherClass { }
```

## Setup

### Quick Start (One per developer)

After cloning, run:

```bash
bash setup-hooks.sh
```

This configures Git to use the repo's version-controlled hooks in `.githooks/`.

### What Setup Does

The setup script:
1. Runs: `git config core.hooksPath .githooks` (tells Git where to find hooks)
2. Copies a recovery hook to: `.git/hooks/post-checkout`
3. Ensures hook scripts are executable

**Works on:** Windows (Git Bash), macOS, Linux

### About `post-checkout` (Recovery Hook)

`post-checkout` is a small safety net that runs after you have installed hooks once.

- If `core.hooksPath` is later cleared/missing, the copied `.git/hooks/post-checkout` hook restores it to `.githooks` on the next checkout.
- This only works if `setup-hooks.sh` was run at least once (Git does not run hooks from the repo automatically right after clone).

If someone forgets to run setup after cloning, local hooks will not run. Use CI as the real enforcement gate and treat local hooks as fast feedback.

## Mandatory Enforcement (GitHub)

Local hooks are for fast feedback, but the actual enforcement should happen in GitHub so non-compliant changes cannot be merged.

This repo includes a GitHub Actions workflow that runs the same validation on every PR and on pushes to `master`/`main`.

To make it mandatory:

1. In GitHub, go to **Settings → Branches → Branch protection rules**
2. Add (or edit) a rule for your protected branch (usually `master` or `main`)
3. Enable **Require status checks to pass before merging**
4. Select the check: `File structure validation / Validate C# file structure`

With that enabled, no PR can be merged if it violates the file naming/structure rules (even if someone bypasses local hooks).

### What Happens at Commit

When you run `git commit`, Git runs the `pre-commit` hook:

1. Gets the list of staged `.cs` files
2. Checks each file for naming/structure violations
3. Blocks the commit if violations are found
4. Allows the commit if all files are valid

### Example: Commit is Blocked

```
$ git commit -m "Add MyClass"

============================================================
FILE STRUCTURE VALIDATION FAILED - COMMIT BLOCKED
============================================================

The following file(s) violate the naming convention:
  Rule: Filename must match the type name exactly

  - File 'src/Bad.cs' declares type 'MyClass'. Filename must match type name (expected: 'MyClass.cs').

Fix the violations by renaming files to match their type names:
  Example: class MyClass should be in file MyClass.cs
```

### Fixing Violations

Simply rename the file to match the type name:

```bash
# Rename the file
git mv src/Bad.cs src/MyClass.cs

# Stage the renamed file
git add src/MyClass.cs

# Try the commit again
git commit -m "Add MyClass"
```

## Hook Configuration

### Hook Location

- **Hooks (version-controlled)**: `.githooks/`
- **Repo config**: `git config core.hooksPath .githooks`

### Updating the Hook

If you need to modify validation rules:

1. Edit `.githooks/pre-commit`
2. Commit the change
3. Your team gets the update on next pull

## Bypassing the Hook (Emergency Only)

```bash
git commit --no-verify
```
