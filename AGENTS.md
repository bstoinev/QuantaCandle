# AGENTS.md

## Workflow for agents
When you finish a task, always end your response with exactly these sections:

1. Short summary of what changed
2. Touched files

Rules for those sections:

### Short summary of what changed
- Keep it brief and practical.

### Touched files
- List only paths relative to the project root.
- Never use absolute local filesystem paths such as `W:/...`.
- Use the real relative path exactly as it exists in the repository.
- Do not omit touched files.

## Design rules

* Assume Windows environment. E.g., always end a line with `\r\n` and use `\` as a path separator.
 Remember that you need to escape the '\' symbol in code like this '\\'.
* A source file may contain only one type declaration at its root. The only exception to this rule is when a file contains nested types.
* Always describe the type purpose using XML comments, and if the type is complex, also describe its members.
* Prefer file-name scoped namespaces and use them whenever possible.
* In case both generic and non-generic version of a type exist, use the suffix "ClosedType" for the non-generic file name.
  For example, `RepositoryClosedType` for non-generic and `Repository<...>` for the generic type. But don't use the suffix in the actual type name, so the types should be named `Repository` and `Repository<T>`.
* Design a method by following the Single Responsibility Principle, which states that a method should have only one reason to change.
* Design a method flow so that it has only one `return` statement just before the closing bracket; multiple `throw` statements are allowed.
 E.g, declare a variable named `result` near the beginning of the method, but after any validation that might throw, assign it a value in the method flow, and return it at the end of the method.
* Prefer adding XML comments explaining the method, especially if the method is complex or has multiple steps.
    Exception to this rule is when the method is simple and has only one or two steps, such as a getter or a setter, or its name is self-explanatory, eg, `GetName()`.
* Prefer primary over regular ctor-s
* Prefer expression-bodied members for simple methods and properties, but use regular method bodies for complex methods and properties with multiple statements.

## Logging
Make extensive use of `ILogMachina<>` for logging, and log at appropriate levels (e.g., `Debug`, `Info`, `Warning`, `Error`) based on the significance of the event being logged.

Use `Trace` level for detailed information on the execution flow.
Use `Debug` to dump technical information to log messages and include relevant context information to facilitate troubleshooting and analysis.
Use `Info` for general information about the application's operation, useful from user's perspective.
Use `Warning` for potential issues that do not prevent the application from functioning and recoverable errors.
Use `Error` for exceptions and critical issues that require immediate attention. If it is a caught error, precede the error message with a warning message describing the circumstances. Awayse include debug info. You may construct the logging block like this:
```csharp
    try {
        // code that may throw an exception
    }
    catch (Exception ex)
    {
        Log.Warning("Circumstance A is available but unable to process due to...");
        Log.Error(ex);
        Log.Debug("{dump_any_relevant_process_info}");
    }
```

Do not overengineer message construction. Most of the time it should read like this: `_log.Info($"Some message with {variable}");`.
Do not bloat the log with redundant information. E.g., if you are logging an exception, do not include the exception message in the log message, as it will be included in the log entry automatically.

 Do not hesitate to stack log messages at different levels. You might log a trace message, followed by debug and info messages, to depict the different pictures for user and developer. The noise will be controlled at runtime by regulating log level emission.

## Testing rules

Prefer Moq over test doubles and stubs.

## Coding standards

### Naming rules & coding style 
* Do not use underscore in method names, including test methods. Use descriptive PascalCase names instead. For example, use `CalculateTotalPrice()` instead of `Calculate_Total_Price()`.
* Prefer `var` delarations instead of left-hand side type declaration.
* When left-hand side type declaration is used, use the short `new()` statement on the right-hand side, instead of repeating the type name.
* When breaking a line, break before an operator, such as `.`, `+`, `&&`, `||`, etc. But don't break lines shorter than 150 characters.
* Do not add 'Async' suffix to method names, unless both synchronous and asynchronous versions of the method exist.

### Ordering rules
* Order members of a class by accessibility level, from the least accessible to the most accessible.
* Within each accessibility level, order members in the following sequence:

1. Constants
1. Fields
1. Nested types
1. Constructors
1. Properties
1. Methods

* Within each member group, order them further alpahabetically by their names.
* The static members follow the same ordering rules as instance members, but they are grouped separately at the beginning of the class.

