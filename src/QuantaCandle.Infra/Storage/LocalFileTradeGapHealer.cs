using System.Globalization;
using System.Text.Json;

using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Heals one bounded numeric trade gap inside a local JSONL dataset by merging fetched trades into local daily files.
/// </summary>
public sealed class LocalFileTradeGapHealer(
    ITradeGapFetchClient tradeGapFetchClient,
    ILogMachina<LocalFileTradeGapHealer> log) : ITradeGapHealer
{
    private readonly ITradeGapFetchClient _tradeGapFetchClient = tradeGapFetchClient ?? throw new ArgumentNullException(nameof(tradeGapFetchClient));
    private readonly ILogMachina<LocalFileTradeGapHealer> _log = log ?? throw new ArgumentNullException(nameof(log));

    /// <summary>
    /// Fetches missing trades for one bounded range and rewrites the affected local JSONL files safely.
    /// </summary>
    public async ValueTask<TradeGapHealResult> Heal(TradeGapHealRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        _log.Info($"Starting trade gap healing for {request.Exchange}:{request.Symbol} in range {request.MissingTradeIdStart}-{request.MissingTradeIdEnd}.");

        try
        {
            var requestedRange = new MissingTradeIdRange(request.MissingTradeIdStart, request.MissingTradeIdEnd);
            var existingFiles = ResolveCandidateFiles(request);
            _log.Info($"Resolved {existingFiles.Count} candidate local trade file(s) for {request.Exchange}:{request.Symbol}.");

            var existingTrades = await ReadExistingTradesAsync(request, existingFiles, cancellationToken).ConfigureAwait(false);
            _log.Info($"Loaded {existingTrades.Count} existing trade(s) before healing {request.Exchange}:{request.Symbol} in range {request.MissingTradeIdStart}-{request.MissingTradeIdEnd}.");

            var fetchedTrades = await _tradeGapFetchClient
                .Fetch(request.Symbol, request.MissingTradeIdStart, request.MissingTradeIdEnd, cancellationToken)
                .ConfigureAwait(false);
            _log.Info($"Fetched {fetchedTrades.Count} trade(s) for requested gap {request.MissingTradeIdStart}-{request.MissingTradeIdEnd} on {request.Exchange}:{request.Symbol}.");

            var validatedFetchedTrades = ValidateFetchedTrades(fetchedTrades, request);
            var unresolvedTradeRanges = DetermineUnresolvedTradeRanges(validatedFetchedTrades, requestedRange);
            var warnings = BuildWarnings(validatedFetchedTrades, requestedRange);

            foreach (var warning in warnings)
            {
                _log.Warn(warning);
            }

            var mergedTrades = MergeTrades(existingTrades, validatedFetchedTrades, out var insertedTradeCount);

            var affectedFiles = new List<TradeGapAffectedFile>();
            if (insertedTradeCount > 0)
            {
                affectedFiles = await RewriteFilesAsync(request, existingFiles, mergedTrades, cancellationToken).ConfigureAwait(false);
                _log.Info($"Persisted {insertedTradeCount} inserted trade(s) across {affectedFiles.Count} file(s) for {request.Exchange}:{request.Symbol}.");
            }
            else
            {
                affectedFiles.AddRange(existingFiles.Select(static file => new TradeGapAffectedFile(file.RelativePath, file.TradingDay)));
                _log.Info($"Gap healing produced no local file changes for {request.Exchange}:{request.Symbol} in range {request.MissingTradeIdStart}-{request.MissingTradeIdEnd}.");
            }

            var affectedRanges = request.AffectedRange is null ? Array.Empty<TradeGapAffectedRange>() : [request.AffectedRange];
            var hasFullRequestedCoverage = unresolvedTradeRanges.Count == 0;
            var outcome = DetermineOutcome(insertedTradeCount, hasFullRequestedCoverage);
            _log.Info($"Finished trade gap healing for {request.Exchange}:{request.Symbol} with outcome {outcome}, fetched {validatedFetchedTrades.Count}, inserted {insertedTradeCount}, fullCoverage={hasFullRequestedCoverage}.");

            var result = new TradeGapHealResult(
                request.Exchange,
                request.Symbol,
                outcome,
                requestedRange,
                validatedFetchedTrades.Count,
                insertedTradeCount,
                hasFullRequestedCoverage,
                unresolvedTradeRanges,
                warnings,
                affectedFiles,
                affectedRanges);
            return result;
        }
        catch (Exception ex)
        {
            _log.Warn($"Local trade gap healing failed for {request.Exchange}:{request.Symbol} in range {request.MissingTradeIdStart}-{request.MissingTradeIdEnd}.");
            _log.Error(ex);
            throw;
        }
    }

    private static List<string> BuildWarnings(List<TradeInfo> fetchedTrades, MissingTradeIdRange requestedRange)
    {
        var result = new List<string>();
        if (fetchedTrades.Count > 1)
        {
            var numericTradeIds = fetchedTrades
                .Select(static trade => ParseNumericTradeId(trade.Key.TradeId, "Fetched trade id must be numeric."))
                .Distinct()
                .OrderBy(static tradeId => tradeId)
                .ToArray();
            if (numericTradeIds.Length > 1)
            {
                for (var index = 1; index < numericTradeIds.Length; index++)
                {
                    if (numericTradeIds[index] > numericTradeIds[index - 1] + 1)
                    {
                        result.Add($"Fetched batch contains an internal gap between trade ids {numericTradeIds[index - 1]} and {numericTradeIds[index]} within requested range {requestedRange.FirstTradeId}-{requestedRange.LastTradeId}.");
                        break;
                    }
                }
            }
        }

        return result;
    }

    private static TradeGapHealStatus DetermineOutcome(int insertedTradeCount, bool hasFullRequestedCoverage)
    {
        var result = TradeGapHealStatus.NoChange;

        if (insertedTradeCount > 0)
        {
            result = hasFullRequestedCoverage ? TradeGapHealStatus.Full : TradeGapHealStatus.Partial;
        }

        return result;
    }

    private static List<TradeInfo> MergeTrades(
        IReadOnlyList<ExistingTrade> existingTrades,
        IReadOnlyList<TradeInfo> fetchedTrades,
        out int insertedTradeCount)
    {
        var tradesByKey = new Dictionary<TradeKey, TradeInfo>();
        foreach (var existingTrade in existingTrades)
        {
            tradesByKey.TryAdd(existingTrade.Trade.Key, existingTrade.Trade);
        }

        insertedTradeCount = 0;
        foreach (var fetchedTrade in fetchedTrades)
        {
            if (tradesByKey.TryAdd(fetchedTrade.Key, fetchedTrade))
            {
                insertedTradeCount++;
            }
        }

        var result = tradesByKey
            .Values
            .OrderBy(static trade => ParseNumericTradeId(trade.Key.TradeId, "Trade id must be numeric."))
            .ThenBy(static trade => trade.Timestamp.ToUniversalTime())
            .ThenBy(static trade => trade.Key.Exchange.Value, StringComparer.Ordinal)
            .ThenBy(static trade => trade.Key.Symbol.ToString(), StringComparer.Ordinal)
            .ToList();
        return result;
    }

    private static IReadOnlyList<MissingTradeIdRange> DetermineUnresolvedTradeRanges(
        IReadOnlyList<TradeInfo> fetchedTrades,
        MissingTradeIdRange requestedRange)
    {
        var result = new List<MissingTradeIdRange>();
        var uniqueTradeIds = fetchedTrades
            .Select(static trade => ParseNumericTradeId(trade.Key.TradeId, "Fetched trade id must be numeric."))
            .Distinct()
            .OrderBy(static tradeId => tradeId)
            .ToArray();
        var nextExpectedTradeId = requestedRange.FirstTradeId;

        foreach (var tradeId in uniqueTradeIds)
        {
            if (tradeId > nextExpectedTradeId)
            {
                result.Add(new MissingTradeIdRange(nextExpectedTradeId, tradeId - 1));
            }

            if (tradeId >= nextExpectedTradeId)
            {
                nextExpectedTradeId = tradeId + 1;
            }
        }

        if (nextExpectedTradeId <= requestedRange.LastTradeId)
        {
            result.Add(new MissingTradeIdRange(nextExpectedTradeId, requestedRange.LastTradeId));
        }

        return result;
    }

    private static List<TradeInfo> ValidateFetchedTrades(IReadOnlyList<TradeInfo> fetchedTrades, TradeGapHealRequest request)
    {
        ArgumentNullException.ThrowIfNull(fetchedTrades);

        var result = new List<TradeInfo>(fetchedTrades.Count);
        foreach (var fetchedTrade in fetchedTrades)
        {
            var tradeId = ParseNumericTradeId(fetchedTrade.Key.TradeId, "Fetched trade id must be numeric.");

            if (!fetchedTrade.Key.Exchange.Value.Equals(request.Exchange.Value, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    FormattableString.Invariant(
                        $"Fetched trade '{fetchedTrade.Key.TradeId}' returned exchange '{fetchedTrade.Key.Exchange.Value}' instead of '{request.Exchange.Value}'."));
            }

            if (!fetchedTrade.Key.Symbol.Equals(request.Symbol))
            {
                throw new InvalidOperationException(
                    FormattableString.Invariant(
                        $"Fetched trade '{fetchedTrade.Key.TradeId}' returned instrument '{fetchedTrade.Key.Symbol}' instead of '{request.Symbol}'."));
            }

            if (tradeId < request.MissingTradeIdStart || tradeId > request.MissingTradeIdEnd)
            {
                throw new InvalidOperationException(
                    FormattableString.Invariant(
                        $"Fetched trade '{tradeId}' is out of requested range {request.MissingTradeIdStart}-{request.MissingTradeIdEnd}."));
            }

            result.Add(
                new TradeInfo(
                    new TradeKey(request.Exchange, request.Symbol, tradeId.ToString(CultureInfo.InvariantCulture)),
                    fetchedTrade.Timestamp.ToUniversalTime(),
                    fetchedTrade.Price,
                    fetchedTrade.Quantity));
        }

        return result;
    }

    private static List<ResolvedFile> ResolveCandidateFiles(TradeGapHealRequest request)
    {
        var result = new List<ResolvedFile>();
        if (request.CandidateFiles.Count > 0)
        {
            foreach (var candidateFile in request.CandidateFiles.OrderBy(static file => file.Path, StringComparer.Ordinal))
            {
                var fullPath = Path.GetFullPath(Path.Combine(request.RootDirectory, candidateFile.Path));
                if (File.Exists(fullPath))
                {
                    result.Add(new ResolvedFile(fullPath, candidateFile.Path, candidateFile.TradingDay ?? TryParseTradingDay(candidateFile.Path)));
                }
            }
        }
        else
        {
            var instrumentDirectory = Path.Combine(request.RootDirectory, request.Symbol.ToString());
            if (Directory.Exists(instrumentDirectory))
            {
                foreach (var fullPath in Directory.EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly).OrderBy(static path => path, StringComparer.Ordinal))
                {
                    var relativePath = Path.GetRelativePath(request.RootDirectory, fullPath);
                    result.Add(new ResolvedFile(fullPath, relativePath, TryParseTradingDay(relativePath)));
                }
            }
        }

        return result;
    }

    private static async Task<List<ExistingTrade>> ReadExistingTradesAsync(
        TradeGapHealRequest request,
        IReadOnlyList<ResolvedFile> files,
        CancellationToken cancellationToken)
    {
        var result = new List<ExistingTrade>();
        foreach (var file in files)
        {
            var lineNumber = 0;
            await foreach (var line in File.ReadLinesAsync(file.FullPath, cancellationToken).ConfigureAwait(false))
            {
                lineNumber++;
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var trade = ParseExistingTrade(line, request, file.FullPath, lineNumber);
                result.Add(new ExistingTrade(trade, file.RelativePath, file.TradingDay));
            }
        }

        return result;
    }

    private static TradeInfo ParseExistingTrade(string line, TradeGapHealRequest request, string fullPath, int lineNumber)
    {
        try
        {
            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;
            var exchangeText = GetRequiredString(root, "exchange", fullPath, lineNumber);
            var instrumentText = GetRequiredString(root, "instrument", fullPath, lineNumber);
            var tradeId = GetRequiredString(root, "tradeId", fullPath, lineNumber);
            var timestamp = root.GetProperty("timestamp").GetDateTimeOffset().ToUniversalTime();
            var price = root.GetProperty("price").GetDecimal();
            var quantity = root.GetProperty("quantity").GetDecimal();

            if (!exchangeText.Equals(request.Exchange.Value, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    FormattableString.Invariant(
                        $"Trade at line {lineNumber} in '{fullPath}' belongs to exchange '{exchangeText}', expected '{request.Exchange.Value}'."));
            }

            var instrument = Instrument.Parse(instrumentText);
            if (!instrument.Equals(request.Symbol))
            {
                throw new InvalidOperationException(
                    FormattableString.Invariant(
                        $"Trade at line {lineNumber} in '{fullPath}' belongs to instrument '{instrument}', expected '{request.Symbol}'."));
            }

            var numericTradeId = ParseNumericTradeId(
                tradeId,
                FormattableString.Invariant($"TradeId '{tradeId}' at line {lineNumber} in '{fullPath}' is not numeric."));
            var normalizedTradeId = numericTradeId.ToString(CultureInfo.InvariantCulture);
            var result = new TradeInfo(new TradeKey(request.Exchange, request.Symbol, normalizedTradeId), timestamp, price, quantity);
            return result;
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or InvalidOperationException or JsonException or KeyNotFoundException)
        {
            throw new InvalidOperationException($"Failed to parse trade at line {lineNumber} in '{fullPath}'.", ex);
        }
    }

    private static string GetRequiredString(JsonElement root, string propertyName, string fullPath, int lineNumber)
    {
        if (!root.TryGetProperty(propertyName, out var property))
        {
            throw new InvalidOperationException($"Trade at line {lineNumber} in '{fullPath}' is missing property '{propertyName}'.");
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException($"Trade at line {lineNumber} in '{fullPath}' has non-string property '{propertyName}'.");
        }

        var result = property.GetString();
        if (string.IsNullOrWhiteSpace(result))
        {
            throw new InvalidOperationException($"Trade at line {lineNumber} in '{fullPath}' has empty property '{propertyName}'.");
        }

        return result.Trim();
    }

    private static long ParseNumericTradeId(string tradeId, string errorMessage)
    {
        if (!long.TryParse(tradeId, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException(errorMessage);
        }

        return result;
    }

    private static DateOnly? TryParseTradingDay(string relativePath)
    {
        var fileName = Path.GetFileNameWithoutExtension(relativePath);
        var result = DateOnly.TryParseExact(fileName, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var tradingDay)
            ? tradingDay
            : (DateOnly?)null;
        return result;
    }

    private static string ResolveRelativePathForDay(
        TradeGapHealRequest request,
        IReadOnlyDictionary<DateOnly, string> relativePathByDay,
        DateOnly tradingDay)
    {
        if (relativePathByDay.TryGetValue(tradingDay, out var relativePath))
        {
            return relativePath;
        }

        var result = Path.Combine(request.Symbol.ToString(), tradingDay.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".jsonl");
        return result;
    }

    private static Dictionary<DateOnly, string> BuildRelativePathByDay(IReadOnlyList<ResolvedFile> existingFiles)
    {
        var result = new Dictionary<DateOnly, string>();
        foreach (var file in existingFiles)
        {
            if (file.TradingDay is null)
            {
                continue;
            }

            if (result.TryGetValue(file.TradingDay.Value, out var existingRelativePath)
                && !existingRelativePath.Equals(file.RelativePath, StringComparison.Ordinal))
            {
                throw new NotSupportedException(
                    FormattableString.Invariant(
                        $"Healing does not yet support multiple local files for the same trading day '{file.TradingDay:yyyy-MM-dd}'."));
            }

            result[file.TradingDay.Value] = file.RelativePath;
        }

        return result;
    }

    private static List<FileRewritePlan> BuildRewritePlans(
        TradeGapHealRequest request,
        IReadOnlyList<ResolvedFile> existingFiles,
        IReadOnlyList<TradeInfo> mergedTrades)
    {
        var tradesByRelativePath = new Dictionary<string, List<TradeInfo>>(StringComparer.Ordinal);
        var relativePathByDay = BuildRelativePathByDay(existingFiles);
        foreach (var trade in mergedTrades)
        {
            var tradingDay = DateOnly.FromDateTime(trade.Timestamp.ToUniversalTime().UtcDateTime);
            var relativePath = ResolveRelativePathForDay(request, relativePathByDay, tradingDay);
            if (!tradesByRelativePath.TryGetValue(relativePath, out var dayTrades))
            {
                dayTrades = [];
                tradesByRelativePath[relativePath] = dayTrades;
            }

            dayTrades.Add(trade);
        }

        var existingFilesByPath = existingFiles.ToDictionary(static file => file.RelativePath, StringComparer.Ordinal);
        var result = new List<FileRewritePlan>(tradesByRelativePath.Count);
        foreach (var pair in tradesByRelativePath.OrderBy(static pair => pair.Key, StringComparer.Ordinal))
        {
            var payload = TradeJsonlFile.BuildPayload(
                pair.Value
                    .OrderBy(static trade => ParseNumericTradeId(trade.Key.TradeId, "Trade id must be numeric."))
                    .ThenBy(static trade => trade.Timestamp.ToUniversalTime())
                    .ToList());
            var fullPath = Path.GetFullPath(Path.Combine(request.RootDirectory, pair.Key));
            var tradingDay = TryParseTradingDay(pair.Key);
            var existingFullPath = existingFilesByPath.TryGetValue(pair.Key, out var existingFile)
                ? existingFile.FullPath
                : null;

            result.Add(new FileRewritePlan(pair.Key, fullPath, existingFullPath, tradingDay, payload));
        }

        return result;
    }

    private static async Task<List<TradeGapAffectedFile>> RewriteFilesAsync(
        TradeGapHealRequest request,
        IReadOnlyList<ResolvedFile> existingFiles,
        IReadOnlyList<TradeInfo> mergedTrades,
        CancellationToken cancellationToken)
    {
        var rewritePlans = BuildRewritePlans(request, existingFiles, mergedTrades);
        var stagedFiles = await StageTempFilesAsync(rewritePlans, cancellationToken).ConfigureAwait(false);
        await ReplaceFilesAsync(stagedFiles).ConfigureAwait(false);

        var result = stagedFiles
            .OrderBy(static file => file.Plan.RelativePath, StringComparer.Ordinal)
            .Select(static file => new TradeGapAffectedFile(file.Plan.RelativePath, file.Plan.TradingDay))
            .ToList();
        return result;
    }

    private static async Task<List<StagedFile>> StageTempFilesAsync(
        IReadOnlyList<FileRewritePlan> rewritePlans,
        CancellationToken cancellationToken)
    {
        var result = new List<StagedFile>(rewritePlans.Count);
        try
        {
            foreach (var rewritePlan in rewritePlans)
            {
                var directory = Path.GetDirectoryName(rewritePlan.FullPath);
                if (!string.IsNullOrWhiteSpace(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var tempPath = rewritePlan.FullPath + ".healing." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".tmp";
                await File.WriteAllTextAsync(tempPath, rewritePlan.Payload, cancellationToken).ConfigureAwait(false);
                result.Add(new StagedFile(rewritePlan, tempPath, rewritePlan.FullPath + ".healing." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".bak"));
            }
        }
        catch
        {
            CleanupStageArtifacts(result);
            throw;
        }

        return result;
    }

    private static Task ReplaceFilesAsync(IReadOnlyList<StagedFile> stagedFiles)
    {
        var appliedChanges = new List<AppliedChange>(stagedFiles.Count);
        try
        {
            foreach (var stagedFile in stagedFiles)
            {
                if (File.Exists(stagedFile.Plan.FullPath))
                {
                    File.Replace(stagedFile.TempPath, stagedFile.Plan.FullPath, stagedFile.BackupPath, true);
                    appliedChanges.Add(new AppliedChange(stagedFile.Plan.FullPath, stagedFile.BackupPath, false));
                }
                else
                {
                    File.Move(stagedFile.TempPath, stagedFile.Plan.FullPath);
                    appliedChanges.Add(new AppliedChange(stagedFile.Plan.FullPath, null, true));
                }
            }
        }
        catch
        {
            RollBackAppliedChanges(appliedChanges);
            CleanupStageArtifacts(stagedFiles);
            throw;
        }

        CleanupAppliedChangeBackups(appliedChanges);
        CleanupStageArtifacts(stagedFiles);
        return Task.CompletedTask;
    }

    private static void RollBackAppliedChanges(IReadOnlyList<AppliedChange> appliedChanges)
    {
        for (var index = appliedChanges.Count - 1; index >= 0; index--)
        {
            var appliedChange = appliedChanges[index];
            if (appliedChange.CreatedNewFile)
            {
                if (File.Exists(appliedChange.FullPath))
                {
                    File.Delete(appliedChange.FullPath);
                }
            }
            else if (!string.IsNullOrWhiteSpace(appliedChange.BackupPath) && File.Exists(appliedChange.BackupPath))
            {
                File.Copy(appliedChange.BackupPath, appliedChange.FullPath, true);
            }
        }
    }

    private static void CleanupAppliedChangeBackups(IReadOnlyList<AppliedChange> appliedChanges)
    {
        foreach (var appliedChange in appliedChanges)
        {
            if (!string.IsNullOrWhiteSpace(appliedChange.BackupPath) && File.Exists(appliedChange.BackupPath))
            {
                File.Delete(appliedChange.BackupPath);
            }
        }
    }

    private static void CleanupStageArtifacts(IReadOnlyList<StagedFile> stagedFiles)
    {
        foreach (var stagedFile in stagedFiles)
        {
            if (File.Exists(stagedFile.TempPath))
            {
                File.Delete(stagedFile.TempPath);
            }

            if (File.Exists(stagedFile.BackupPath))
            {
                File.Delete(stagedFile.BackupPath);
            }
        }
    }

    /// <summary>
    /// Represents one local trade line already present in the dataset together with its source file metadata.
    /// </summary>
    private sealed record ExistingTrade(TradeInfo Trade, string RelativePath, DateOnly? TradingDay);

    /// <summary>
    /// Represents one resolved candidate JSONL file.
    /// </summary>
    private sealed record ResolvedFile(string FullPath, string RelativePath, DateOnly? TradingDay);

    /// <summary>
    /// Represents one planned file rewrite.
    /// </summary>
    private sealed record FileRewritePlan(string RelativePath, string FullPath, string? ExistingFullPath, DateOnly? TradingDay, string Payload);

    /// <summary>
    /// Represents one staged temp file and its associated rollback backup path.
    /// </summary>
    private sealed record StagedFile(FileRewritePlan Plan, string TempPath, string BackupPath);

    /// <summary>
    /// Represents one applied filesystem change that may need rollback.
    /// </summary>
    private sealed record AppliedChange(string FullPath, string? BackupPath, bool CreatedNewFile);
}
