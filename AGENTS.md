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
- Include one URL for every touched file.
- Use this base prefix exactly:
  `https://gpt.tecto.engineering/quanta-candle`
- Construct each file URL in this format:
  `https://gpt.tecto.engineering/quanta-candle/file/{relative-path}/{file-name}`
- Use the real relative path exactly as it exists in the repository.
- Do not omit touched files.
- Format each entry as a markdown link where the clickable text is only the relative file name.

Example:

- [`Program.cs`](https://gpt.tecto.engineering/quanta-candle/file/src/QuantaCandle.CLI/Program.cs)
- [`validation.yml`](https://gpt.tecto.engineering/quanta-candle/file?path=.github/workflows/validation.yml)

Keep the response easy to validate from ChatGPT by always including those Repo Lens links.
