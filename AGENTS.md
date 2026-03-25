# AGENTS.md

This repository is validated through Repo Lens.

When you finish a task, always end your response with exactly these sections:

1. Short summary of what changed
2. Touched files

Rules for those sections:

## Short summary of what changed
- Keep it brief and practical.

## Touched files
- List only paths relative to the project root.
- Never use absolute local filesystem paths such as `W:/...`.
- Include one URL for every touched file, using this format:
  `https://gpt.tecto.engineering/quanta-candle/file/{relative-path}/{file-name}`
- Use the real relative path exactly as it exists in the repository.
- Do not omit touched files.
- Format each entry as a markdown link where the clickable text is only the relative file name.

Example:
- [`Program.cs`](https://gpt.tecto.engineering/quanta-candle/file/src/QuantaCandle.CLI/Program.cs)
- [`validation.yml`](https://gpt.tecto.engineering/quanta-candle/file?path=.github/workflows/validation.yml)

Keep the response easy to validate from ChatGPT by always including those Repo Lens links.

## General rules
* Assume Windows environment. E.g., always end a line with `\r\n` and use `\` as a path separator.
* Remember that you need to escape the '\' symbol in code like this '\\'.

## Naming rules
* Never use underscore in method names, including test methods. Use descriptive PascalCase names instead. For example, use `CalculateTotalPrice()` instead of `Calculate_Total_Price()`.
* Never add 'Async' suffix to method names, unless both synchronous and asynchronous versions of the method exist.

## Coding rules

* A source file may contain only one type declaration at its root. The only exception to this rule is when a file contains nested types.
* Always describe the type purpose using XML comments, and if the type is complex, also describe its members.
* Prefer file-name scoped namespaces and use them whenever possible.
* In case both generic and non-generic version of a type exist, use the suffix "ClosedType" for the non-generic file name.
  For example, `RepositoryClosedType` for non-generic and `Repository<...>` for the generic type. But don't use the suffix in the actual type name, so the types should be named `Repository` and `Repository<T>`.
* Design a method by following the Single Responsibility Principle, which states that a method should have only one reason to change.
* Design a method flow so that it has only one `return` statement just before the closing bracket; multiple `throw` statements are allowed.
* Prefer adding XML comments explaining the method flow, especially if the method is complex or has multiple steps.
    Exception to this rule is when the method is simple and has only one or two steps, such as a getter or a setter, or its name is self-explanatory, eg, `GetName()`.
* When breaking a line, break before the an operator, such as `.`, `+`, `&&`, `||`, etc. This makes it easier to read and understand the code. But don't break lines shorter than least 120 characters.
* Prefer `var` delarations instead of left-hand side type declaration.
* When left-hand side type declaration is used, use the short `new()` statement on the right-hand side, instead of repeating the type name.

## Testing rules

Prefer Moq over test doubles, and test doubles over real dependencies.
