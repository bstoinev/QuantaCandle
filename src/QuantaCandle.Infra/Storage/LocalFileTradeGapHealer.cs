using System.Globalization;
using System.Text.Json;

using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Heals one bounded numeric trade gap inside a local JSONL dataset by splicing fetched trades into the local stream.
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

            var fetchedTrades = await _tradeGapFetchClient
                .Fetch(request.Symbol, request.MissingTradeIdStart, request.MissingTradeIdEnd, cancellationToken)
                .ConfigureAwait(false);
            _log.Info($"Fetched {fetchedTrades.Count} trade(s) for requested gap {request.MissingTradeIdStart}-{request.MissingTradeIdEnd} on {request.Exchange}:{request.Symbol}.");

            var validatedFetchedTrades = ValidateFetchedTrades(fetchedTrades, request);
            var warnings = BuildWarnings(validatedFetchedTrades, requestedRange);
            var fetchedTradeBatch = PrepareFetchedTradesForSplice(validatedFetchedTrades);
            var unresolvedTradeRanges = DetermineUnresolvedTradeRanges(fetchedTradeBatch, requestedRange);

            foreach (var warning in warnings)
            {
                _log.Warn(warning);
            }

            var insertedTradeCount = 0;
            IReadOnlyList<TradeGapAffectedFile> affectedFiles;
            if (fetchedTradeBatch.Count > 0)
            {
                var spliceResult = await SpliceFetchedTradesIntoLocalFiles(request, existingFiles, fetchedTradeBatch, cancellationToken).ConfigureAwait(false);
                insertedTradeCount = spliceResult.InsertedTradeCount;
                affectedFiles = spliceResult.AffectedFiles;
                _log.Info($"Persisted {insertedTradeCount} inserted trade(s) across {affectedFiles.Count} file(s) for {request.Exchange}:{request.Symbol}.");
            }
            else
            {
                affectedFiles = existingFiles
                    .Select(static file => new TradeGapAffectedFile(file.RelativePath, file.TradingDay))
                    .ToList();
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

    private static IReadOnlyList<MissingTradeIdRange> DetermineUnresolvedTradeRanges(
        IReadOnlyList<FetchedTrade> fetchedTrades,
        MissingTradeIdRange requestedRange)
    {
        var result = new List<MissingTradeIdRange>();
        var uniqueTradeIds = fetchedTrades
            .Select(static trade => trade.NumericTradeId)
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

    private static TradeGapHealStatus DetermineOutcome(int insertedTradeCount, bool hasFullRequestedCoverage)
    {
        var result = TradeGapHealStatus.NoChange;

        if (insertedTradeCount > 0)
        {
            result = hasFullRequestedCoverage ? TradeGapHealStatus.Full : TradeGapHealStatus.Partial;
        }

        return result;
    }

    private static List<FetchedTrade> PrepareFetchedTradesForSplice(IReadOnlyList<TradeInfo> fetchedTrades)
    {
        var result = fetchedTrades
            .Select(static trade => new FetchedTrade(
                trade,
                ParseNumericTradeId(trade.Key.TradeId, "Fetched trade id must be numeric.")))
            .OrderBy(static trade => trade.NumericTradeId)
            .ThenBy(static trade => trade.Trade.Timestamp.ToUniversalTime())
            .ToList();

        for (var index = 1; index < result.Count; index++)
        {
            if (result[index].NumericTradeId == result[index - 1].NumericTradeId)
            {
                throw new InvalidOperationException($"Fetched batch contains duplicate trade id '{result[index].NumericTradeId}'.");
            }
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

        var baseDirectory = request.CandidateFiles.Count > 0
            ? Path.GetDirectoryName(request.CandidateFiles[0].Path)
            : request.Symbol.ToString();
        var result = string.IsNullOrWhiteSpace(baseDirectory)
            ? tradingDay.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".jsonl"
            : Path.Combine(baseDirectory, tradingDay.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".jsonl");
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

    private static async Task<SpliceResult> SpliceFetchedTradesIntoLocalFiles(
        TradeGapHealRequest request,
        IReadOnlyList<ResolvedFile> existingFiles,
        IReadOnlyList<FetchedTrade> fetchedTrades,
        CancellationToken cancellationToken)
    {
        var outputWriter = new SpliceOutputWriter(request, existingFiles);
        var insertedTradeCount = 0;
        var fetchedTradeIndex = 0;

        try
        {
            foreach (var existingFile in existingFiles.OrderBy(static file => file.RelativePath, StringComparer.Ordinal))
            {
                var lineNumber = 0;
                await foreach (var line in File.ReadLinesAsync(existingFile.FullPath, cancellationToken).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    lineNumber++;
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    var localTrade = ParseExistingTrade(line, request, existingFile.FullPath, lineNumber);
                    var localTradeId = ParseNumericTradeId(
                        localTrade.Key.TradeId,
                        FormattableString.Invariant($"TradeId '{localTrade.Key.TradeId}' at line {lineNumber} in '{existingFile.FullPath}' is not numeric."));

                    while (fetchedTradeIndex < fetchedTrades.Count && fetchedTrades[fetchedTradeIndex].NumericTradeId < localTradeId)
                    {
                        await outputWriter.WriteTrade(fetchedTrades[fetchedTradeIndex].Trade, cancellationToken).ConfigureAwait(false);
                        insertedTradeCount++;
                        fetchedTradeIndex++;
                    }

                    if (fetchedTradeIndex < fetchedTrades.Count && fetchedTrades[fetchedTradeIndex].NumericTradeId == localTradeId)
                    {
                        throw new InvalidOperationException(
                            FormattableString.Invariant(
                                $"Fetched trade id '{localTradeId}' overlaps existing local trade id in '{existingFile.FullPath}' at line {lineNumber}."));
                    }

                    await outputWriter.WriteTrade(localTrade, cancellationToken).ConfigureAwait(false);
                }
            }

            while (fetchedTradeIndex < fetchedTrades.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await outputWriter.WriteTrade(fetchedTrades[fetchedTradeIndex].Trade, cancellationToken).ConfigureAwait(false);
                insertedTradeCount++;
                fetchedTradeIndex++;
            }

            var affectedFiles = await outputWriter.Commit().ConfigureAwait(false);
            var result = new SpliceResult(insertedTradeCount, affectedFiles);
            return result;
        }
        catch
        {
            await outputWriter.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static Task ReplaceFilesAsync(IReadOnlyList<StagedFile> stagedFiles)
    {
        var appliedChanges = new List<AppliedChange>(stagedFiles.Count);
        try
        {
            foreach (var stagedFile in stagedFiles)
            {
                if (File.Exists(stagedFile.FullPath))
                {
                    File.Replace(stagedFile.TempPath, stagedFile.FullPath, stagedFile.BackupPath, true);
                    appliedChanges.Add(new AppliedChange(stagedFile.FullPath, stagedFile.BackupPath, false));
                }
                else
                {
                    File.Move(stagedFile.TempPath, stagedFile.FullPath);
                    appliedChanges.Add(new AppliedChange(stagedFile.FullPath, null, true));
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
    /// Represents one resolved candidate JSONL file.
    /// </summary>
    private sealed record ResolvedFile(string FullPath, string RelativePath, DateOnly? TradingDay);

    /// <summary>
    /// Represents one fetched trade prepared for numeric splice ordering.
    /// </summary>
    private sealed record FetchedTrade(TradeInfo Trade, long NumericTradeId);

    /// <summary>
    /// Represents one staged temp file and its associated rollback backup path.
    /// </summary>
    private sealed record StagedFile(string RelativePath, string FullPath, DateOnly? TradingDay, string TempPath, string BackupPath);

    /// <summary>
    /// Represents one applied filesystem change that may need rollback.
    /// </summary>
    private sealed record AppliedChange(string FullPath, string? BackupPath, bool CreatedNewFile);

    /// <summary>
    /// Represents one splice operation result.
    /// </summary>
    private sealed record SpliceResult(int InsertedTradeCount, IReadOnlyList<TradeGapAffectedFile> AffectedFiles);

    /// <summary>
    /// Streams spliced JSONL output into staged temp files by UTC day.
    /// </summary>
    private sealed class SpliceOutputWriter(TradeGapHealRequest request, IReadOnlyList<LocalFileTradeGapHealer.ResolvedFile> existingFiles) : IAsyncDisposable
    {
        private readonly Dictionary<DateOnly, string> _relativePathByDay = BuildRelativePathByDay(existingFiles ?? throw new ArgumentNullException(nameof(existingFiles)));
        private readonly TradeGapHealRequest _request = request ?? throw new ArgumentNullException(nameof(request));
        private readonly Dictionary<string, StagedFile> _stagedFilesByPath = new(StringComparer.Ordinal);
        private readonly Dictionary<string, StreamWriter> _writersByPath = new(StringComparer.Ordinal);

        public async Task<IReadOnlyList<TradeGapAffectedFile>> Commit()
        {
            foreach (var writer in _writersByPath.Values)
            {
                await writer.FlushAsync().ConfigureAwait(false);
                await writer.DisposeAsync().ConfigureAwait(false);
            }

            _writersByPath.Clear();

            var stagedFiles = _stagedFilesByPath.Values
                .OrderBy(static file => file.RelativePath, StringComparer.Ordinal)
                .ToList();
            await ReplaceFilesAsync(stagedFiles).ConfigureAwait(false);

            return stagedFiles
                .Select(static file => new TradeGapAffectedFile(file.RelativePath, file.TradingDay))
                .ToList();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var writer in _writersByPath.Values)
            {
                await writer.DisposeAsync().ConfigureAwait(false);
            }

            _writersByPath.Clear();
            CleanupStageArtifacts(_stagedFilesByPath.Values.ToList());
        }

        public async Task WriteTrade(TradeInfo trade, CancellationToken terminator)
        {
            terminator.ThrowIfCancellationRequested();

            var tradingDay = DateOnly.FromDateTime(trade.Timestamp.ToUniversalTime().UtcDateTime);
            var relativePath = ResolveRelativePathForDay(_request, _relativePathByDay, tradingDay);
            var writer = await GetWriter(relativePath, tradingDay).ConfigureAwait(false);
            await writer.WriteLineAsync(TradeJsonlFile.SerializeTrade(trade)).ConfigureAwait(false);
        }

        private Task<StreamWriter> GetWriter(string relativePath, DateOnly tradingDay)
        {
            if (_writersByPath.TryGetValue(relativePath, out var existingWriter))
            {
                return Task.FromResult(existingWriter);
            }

            var fullPath = Path.GetFullPath(Path.Combine(_request.RootDirectory, relativePath));
            var directory = Path.GetDirectoryName(fullPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var tempPath = fullPath + ".healing." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".tmp";
            var backupPath = fullPath + ".healing." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".bak";
            var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
            var writer = new StreamWriter(stream);
            var stagedFile = new StagedFile(relativePath, fullPath, tradingDay, tempPath, backupPath);

            _writersByPath[relativePath] = writer;
            _stagedFilesByPath[relativePath] = stagedFile;
            return Task.FromResult(writer);
        }
    }
}
