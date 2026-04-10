namespace QuantaCandle.CLI;

/// <summary>
/// Dispatches the CLI entrypoint into candle generation, gap scanning, or gap healing modes.
/// </summary>
internal sealed class CliApplication(IQuantaCandleRunner runner)
{
    /// <summary>
    /// Parses arguments, executes the selected mode, writes console output, and returns a process exit code.
    /// </summary>
    public async Task<int> Run(
        string[] args,
        TextWriter outputWriter,
        TextWriter errorWriter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(outputWriter);
        ArgumentNullException.ThrowIfNull(errorWriter);

        var result = 0;
        CliOptions runOptions;

        if (CliCommand.IsHelpRequest(args))
        {
            CliCommand.WriteHelp(outputWriter);
        }
        else
        {
            try
            {
                runOptions = CliCommand.Parse(args);
            }
            catch (ArgumentException ex)
            {
                await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                result = 2;
                runOptions = new CliOptions(CliMode.Unknown, string.Empty, string.Empty, string.Empty, string.Empty, []);
            }

            if (result == 0)
            {
                try
                {
                    if (runOptions.Mode is CliMode.Scan)
                    {
                        result = await runner.Scan(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else if (runOptions.Mode is CliMode.Heal)
                    {
                        result = await runner.Heal(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        result = await runner.Candlize(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (NotSupportedException ex)
                {
                    await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                    result = 2;
                }
                catch (ArgumentException ex)
                {
                    await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                    result = 2;
                }
                catch (InvalidOperationException ex)
                {
                    await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                    result = 2;
                }
            }
        }

        return result;
    }
}
