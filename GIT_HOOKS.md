# File Structure Validation - Git Pre-Commit Hooks

This project enforces a file structure policy using Git pre-commit hooks. After one-time setup, validation is **mandatory and automatic** before every commit.

## Policy

- **One type per file**: Each `.cs` file must contain exactly one top-level type (class, interface, enum, record, struct, or delegate)
- **Filename matches type name**: The filename must exactly match the type name

### Examples

✅ **Valid:**
```csharp
// File: MyClass.cs
public class MyClass { }
```

```csharp
// File: IMyInterface.cs
public interface IMyInterface { }
```

❌ **Invalid:**
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

After cloning, run this command to enable mandatory hook validation:

```bash
bash setup-hooks.sh
```

That's it! Validation is now mandatory and automatic on every commit.

### What Setup Does

The setup script:
1. Runs: `git config core.hooksPath .githooks` (tells Git where to find hooks)
2. Installs recovery hook: `post-checkout` (auto-configures if setup is missed)
3. Verifies hook permissions are correct

**Works on:** Windows (Git Bash), macOS, Linux

### What If Setup is Forgotten?

**Don't worry—there's automatic recovery:**

- If a developer forgets to run setup and tries to commit, the pre-commit hook won't run
- But as soon as they run ANY `git checkout` or `git pull`, the post-checkout hook detects the misconfiguration
- The post-checkout hook automatically runs `git config core.hooksPath .githooks`
- Next commit will be protected ✓

This means violations can't slip through for long—the hooks self-heal on the next checkout operation.

### What Happens at Commit

When you run `git commit`, Git automatically runs the pre-commit hook from `.githooks/pre-commit`:

1. Gets the list of staged `.cs` files
2. Checks each file for naming/structure violations
3. **Blocks the commit** if violations are found
4. **Allows the commit** if all files are valid

### Example: Commit is Blocked

```
$ git commit -m "Add MyClass"

═════════════════════════════════════════════════════
FILE STRUCTURE VALIDATION FAILED - COMMIT BLOCKED
═════════════════════════════════════════════════════

The following file(s) violate the naming convention:
  Rule: Filename must match the type name exactly

  ✗ File 'src/Bad.cs' declares type 'MyClass'. Filename must match type name (expected: 'MyClass.cs').

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

- **Source (version-controlled)**: `.githooks/pre-commit`
- **Configuration**: `git config core.hooksPath .githooks`

When developers run the setup script, Git is configured to use `.githooks/` for all hooks.

### Updating the Hook

If you need to modify validation rules:

1. Edit `.githooks/pre-commit`
2. Commit the change
3. Your team **automatically gets the update** on next pull
4. No re-setup needed!

## Bypassing the Hook (Emergency Only)

If you absolutely need to commit without validation (not recommended):

```bash
git commit --no-verify
```
