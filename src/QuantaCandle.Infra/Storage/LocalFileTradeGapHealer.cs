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

            await using var splicer = new StreamingTradeGapSplicer(request, existingFiles, cancellationToken);
            await _tradeGapFetchClient
                .Fetch(request.Symbol, request.MissingTradeIdStart, request.MissingTradeIdEnd, splicer, request.ProgressReporter, cancellationToken)
                .ConfigureAwait(false);
            _log.Info($"Fetched {splicer.FetchedTradeCount} trade(s) for requested gap {request.MissingTradeIdStart}-{request.MissingTradeIdEnd} on {request.Exchange}:{request.Symbol}.");

            var unresolvedTradeRanges = DetermineUnresolvedTradeRanges(splicer.FetchedRanges, request.RequestedMissingTradeRanges);
            var warnings = BuildWarnings(unresolvedTradeRanges, request.RequestedMissingTradeRanges);

            foreach (var warning in warnings)
            {
                _log.Warn(warning);
            }

            var insertedTradeCount = 0;
            IReadOnlyList<TradeGapAffectedFile> affectedFiles;
            if (splicer.FetchedTradeCount > 0)
            {
                var spliceResult = await splicer.Complete().ConfigureAwait(false);
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
            _log.Info($"Finished trade gap healing for {request.Exchange}:{request.Symbol} with outcome {outcome}, fetched {splicer.FetchedTradeCount}, inserted {insertedTradeCount}, fullCoverage={hasFullRequestedCoverage}.");

            var result = new TradeGapHealResult(
                request.Exchange,
                request.Symbol,
                outcome,
                requestedRange,
                splicer.FetchedTradeCount,
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

    private static List<string> BuildWarnings(
        IReadOnlyList<MissingTradeIdRange> unresolvedTradeRanges,
        IReadOnlyList<MissingTradeIdRange> requestedMissingTradeRanges)
    {
        var result = new List<string>();

        if (unresolvedTradeRanges.Count > 0)
        {
            var requestedRangeText = string.Join(", ", requestedMissingTradeRanges.Select(FormatRange));
            var unresolvedRangeText = string.Join(", ", unresolvedTradeRanges.Select(FormatRange));
            result.Add($"Fetched batch did not fully cover requested missing range(s) [{requestedRangeText}]. Unresolved range(s): [{unresolvedRangeText}].");
        }

        return result;
    }

    private static IReadOnlyList<MissingTradeIdRange> DetermineUnresolvedTradeRanges(
        IReadOnlyList<MissingTradeIdRange> fetchedRanges,
        IReadOnlyList<MissingTradeIdRange> requestedRanges)
    {
        var result = new List<MissingTradeIdRange>();

        foreach (var requestedRange in requestedRanges)
        {
            var nextExpectedTradeId = requestedRange.FirstTradeId;

            foreach (var fetchedRange in fetchedRanges)
            {
                if (fetchedRange.LastTradeId < requestedRange.FirstTradeId)
                {
                    continue;
                }

                if (fetchedRange.FirstTradeId > requestedRange.LastTradeId)
                {
                    break;
                }

                if (fetchedRange.FirstTradeId > nextExpectedTradeId)
                {
                    result.Add(new MissingTradeIdRange(nextExpectedTradeId, fetchedRange.FirstTradeId - 1));
                }

                nextExpectedTradeId = Math.Max(nextExpectedTradeId, fetchedRange.LastTradeId + 1);
            }

            if (nextExpectedTradeId <= requestedRange.LastTradeId)
            {
                result.Add(new MissingTradeIdRange(nextExpectedTradeId, requestedRange.LastTradeId));
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

    private static TradeInfo ValidateFetchedTrade(TradeInfo fetchedTrade, TradeGapHealRequest request)
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

        var result = new TradeInfo(
            new TradeKey(request.Exchange, request.Symbol, tradeId.ToString(CultureInfo.InvariantCulture)),
            fetchedTrade.Timestamp.ToUniversalTime(),
            fetchedTrade.Price,
            fetchedTrade.Quantity);
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

    private static void EnsureFetchedTradeMatchesLocalTrade(
        TradeInfo fetchedTrade,
        TradeInfo localTrade,
        string fullPath,
        int lineNumber)
    {
        if (fetchedTrade.Timestamp.ToUniversalTime() != localTrade.Timestamp.ToUniversalTime()
            || fetchedTrade.Price != localTrade.Price
            || fetchedTrade.Quantity != localTrade.Quantity)
        {
            throw new InvalidOperationException(
                FormattableString.Invariant(
                    $"Fetched trade id '{localTrade.Key.TradeId}' conflicts with existing local trade data in '{fullPath}' at line {lineNumber}."));
        }
    }

    private static string FormatRange(MissingTradeIdRange range)
    {
        var result = range.FirstTradeId == range.LastTradeId
            ? range.FirstTradeId.ToString(CultureInfo.InvariantCulture)
            : $"{range.FirstTradeId}-{range.LastTradeId}";
        return result;
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
    /// Represents one parsed local trade and its source location.
    /// </summary>
    private sealed record ParsedLocalTrade(TradeInfo Trade, long NumericTradeId, string FullPath, int LineNumber);

    /// <summary>
    /// Streams fetched trade pages directly into staged output while preserving global trade identifier order.
    /// </summary>
    private sealed class StreamingTradeGapSplicer : ITradeGapFetchedPageSink, IAsyncDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly IReadOnlyList<ResolvedFile> _existingFiles;
        private readonly SpliceOutputWriter _outputWriter;
        private readonly TradeGapHealRequest _request;
        private IAsyncEnumerator<string>? _currentFileEnumerator;
        private ResolvedFile? _currentFile;
        private ParsedLocalTrade? _currentLocalTrade;
        private int _currentLineNumber;
        private int _existingFileIndex;
        private int _insertedTradeCount;
        private long? _lastFetchedTradeId;

        public StreamingTradeGapSplicer(TradeGapHealRequest request, IReadOnlyList<ResolvedFile> existingFiles, CancellationToken cancellationToken)
        {
            _request = request ?? throw new ArgumentNullException(nameof(request));
            _existingFiles = (existingFiles ?? throw new ArgumentNullException(nameof(existingFiles)))
                .OrderBy(static file => file.RelativePath, StringComparer.Ordinal)
                .ToList();
            _cancellationToken = cancellationToken;
            _outputWriter = new SpliceOutputWriter(request, _existingFiles);
            FetchedRanges = new List<MissingTradeIdRange>();
        }

        /// <summary>
        /// Gets the fetched trade count observed across all accepted pages.
        /// </summary>
        public int FetchedTradeCount { get; private set; }

        /// <summary>
        /// Gets the merged fetched trade identifier ranges observed during streaming.
        /// </summary>
        public List<MissingTradeIdRange> FetchedRanges { get; }

        /// <summary>
        /// Accepts one downloaded page and splices it into staged output immediately.
        /// </summary>
        public async ValueTask AcceptPage(IReadOnlyList<TradeInfo> pageTrades, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(pageTrades);

            foreach (var pageTrade in pageTrades)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var normalizedTrade = ValidateFetchedTrade(pageTrade, _request);
                var fetchedTradeId = ParseNumericTradeId(normalizedTrade.Key.TradeId, "Fetched trade id must be numeric.");

                ObserveFetchedTradeId(fetchedTradeId);
                FetchedTradeCount++;

                while (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false)
                    && _currentLocalTrade!.NumericTradeId < fetchedTradeId)
                {
                    await _outputWriter.WriteTrade(_currentLocalTrade.Trade, cancellationToken).ConfigureAwait(false);
                    _currentLocalTrade = null;
                }

                if (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false)
                    && _currentLocalTrade!.NumericTradeId == fetchedTradeId)
                {
                    EnsureFetchedTradeMatchesLocalTrade(
                        normalizedTrade,
                        _currentLocalTrade.Trade,
                        _currentLocalTrade.FullPath,
                        _currentLocalTrade.LineNumber);
                    await _outputWriter.WriteTrade(_currentLocalTrade.Trade, cancellationToken).ConfigureAwait(false);
                    _currentLocalTrade = null;
                }
                else
                {
                    await _outputWriter.WriteTrade(normalizedTrade, cancellationToken).ConfigureAwait(false);
                    _insertedTradeCount++;
                }
            }
        }

        /// <summary>
        /// Finalizes the staged output after all pages have been consumed.
        /// </summary>
        public async Task<SpliceResult> Complete()
        {
            while (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false))
            {
                await _outputWriter.WriteTrade(_currentLocalTrade!.Trade, _cancellationToken).ConfigureAwait(false);
                _currentLocalTrade = null;
            }

            var affectedFiles = await _outputWriter.Commit().ConfigureAwait(false);
            var result = new SpliceResult(_insertedTradeCount, affectedFiles);
            return result;
        }

        /// <summary>
        /// Releases any open reader or staged output resources.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_currentFileEnumerator is not null)
            {
                await _currentFileEnumerator.DisposeAsync().ConfigureAwait(false);
                _currentFileEnumerator = null;
            }

            await _outputWriter.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures the next local trade is loaded if one remains.
        /// </summary>
        private async Task<bool> EnsureCurrentLocalTradeLoaded()
        {
            var result = true;

            if (_currentLocalTrade is null)
            {
                while (true)
                {
                    if (_currentFileEnumerator is null)
                    {
                        result = await OpenNextFile().ConfigureAwait(false);
                        if (!result)
                        {
                            break;
                        }
                    }

                    if (!await _currentFileEnumerator!.MoveNextAsync().ConfigureAwait(false))
                    {
                        await _currentFileEnumerator.DisposeAsync().ConfigureAwait(false);
                        _currentFileEnumerator = null;
                        _currentFile = null;
                        _currentLineNumber = 0;
                        continue;
                    }

                    _currentLineNumber++;
                    var line = _currentFileEnumerator.Current;
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    var localTrade = ParseExistingTrade(line, _request, _currentFile!.FullPath, _currentLineNumber);
                    var localTradeId = ParseNumericTradeId(
                        localTrade.Key.TradeId,
                        FormattableString.Invariant($"TradeId '{localTrade.Key.TradeId}' at line {_currentLineNumber} in '{_currentFile.FullPath}' is not numeric."));
                    _currentLocalTrade = new ParsedLocalTrade(localTrade, localTradeId, _currentFile.FullPath, _currentLineNumber);
                    break;
                }
            }

            return result && _currentLocalTrade is not null;
        }

        /// <summary>
        /// Opens the next local file reader when one remains.
        /// </summary>
        private Task<bool> OpenNextFile()
        {
            var result = false;

            if (_existingFileIndex < _existingFiles.Count)
            {
                while (_existingFileIndex < _existingFiles.Count)
                {
                    var nextIndex = _existingFileIndex;
                    _existingFileIndex++;
                    if (nextIndex >= _existingFiles.Count)
                    {
                        break;
                    }

                    _currentFile = _existingFiles[nextIndex];
                    _currentFileEnumerator = File.ReadLinesAsync(_currentFile.FullPath, _cancellationToken).GetAsyncEnumerator(_cancellationToken);
                    _currentLineNumber = 0;
                    result = true;
                    break;
                }
            }

            return Task.FromResult(result);
        }

        /// <summary>
        /// Tracks fetched identifier coverage without storing all fetched trades.
        /// </summary>
        private void ObserveFetchedTradeId(long fetchedTradeId)
        {
            if (_lastFetchedTradeId is not null && fetchedTradeId == _lastFetchedTradeId.Value)
            {
                throw new InvalidOperationException($"Fetched batch contains duplicate trade id '{fetchedTradeId}'.");
            }

            if (_lastFetchedTradeId is not null && fetchedTradeId < _lastFetchedTradeId.Value)
            {
                throw new InvalidOperationException(
                    $"Fetched trade id '{fetchedTradeId}' is not strictly ascending after '{_lastFetchedTradeId.Value}'.");
            }

            _lastFetchedTradeId = fetchedTradeId;

            if (FetchedRanges.Count == 0)
            {
                FetchedRanges.Add(new MissingTradeIdRange(fetchedTradeId, fetchedTradeId));
            }
            else
            {
                var previousRange = FetchedRanges[^1];
                if (fetchedTradeId == previousRange.LastTradeId + 1)
                {
                    FetchedRanges[^1] = new MissingTradeIdRange(previousRange.FirstTradeId, fetchedTradeId);
                }
                else
                {
                    FetchedRanges.Add(new MissingTradeIdRange(fetchedTradeId, fetchedTradeId));
                }
            }
        }
    }

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
