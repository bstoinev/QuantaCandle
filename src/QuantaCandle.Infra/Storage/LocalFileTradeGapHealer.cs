using System.Globalization;
using System.Text.Json;

using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Heals one local JSONL trade file by splicing exact fetched gap ranges into the local stream.
/// </summary>
public sealed class LocalFileTradeGapHealer(
    ITradeGapFetchClient tradeGapFetchClient,
    ILogMachina<LocalFileTradeGapHealer> log) : ITradeGapHealer
{
    private readonly ITradeGapFetchClient _tradeGapFetchClient = tradeGapFetchClient ?? throw new ArgumentNullException(nameof(tradeGapFetchClient));
    private readonly ILogMachina<LocalFileTradeGapHealer> _log = log ?? throw new ArgumentNullException(nameof(log));

    /// <summary>
    /// Fetches exact missing trades for one local file and rewrites that file safely with a forward-only sequential splice.
    /// </summary>
    public async ValueTask<TradeGapHealResult> Heal(TradeGapHealRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        _log.Info($"Starting trade gap healing for {request.Exchange}:{request.Symbol} in range {request.MissingTradeIdStart}-{request.MissingTradeIdEnd}.");

        try
        {
            var requestedRange = new MissingTradeIdRange(request.MissingTradeIdStart, request.MissingTradeIdEnd);
            var existingFiles = ResolveCandidateFiles(request);
            var existingFile = EnsureSingleResolvedFile(existingFiles, request);
            _log.Info($"Resolved local trade file '{existingFile.RelativePath}' for {request.Exchange}:{request.Symbol}.");

            await using var splicer = new StreamingTradeGapSplicer(request, existingFile, cancellationToken);
            foreach (var missingRange in request.RequestedMissingTradeRanges)
            {
                _log.Info($"Fetching exact missing range {missingRange.FirstTradeId}-{missingRange.LastTradeId} for file '{existingFile.RelativePath}'.");
                await splicer.AdvanceToMissingRange(missingRange).ConfigureAwait(false);
                await _tradeGapFetchClient
                    .Fetch(request.Symbol, missingRange.FirstTradeId, missingRange.LastTradeId, splicer, request.ProgressReporter, cancellationToken)
                    .ConfigureAwait(false);
                await splicer.CompleteMissingRange(missingRange).ConfigureAwait(false);
            }

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
                affectedFiles = [new TradeGapAffectedFile(existingFile.RelativePath, existingFile.TradingDay)];
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

    /// <summary>
    /// Ensures the current healing request targets exactly one resolved local file.
    /// </summary>
    private static ResolvedFile EnsureSingleResolvedFile(IReadOnlyList<ResolvedFile> resolvedFiles, TradeGapHealRequest request)
    {
        if (resolvedFiles.Count != 1)
        {
            throw new InvalidOperationException(
                $"Trade gap healing requires exactly one resolved local file per request, but found {resolvedFiles.Count} for {request.Exchange}:{request.Symbol}.");
        }

        return resolvedFiles[0];
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
    /// Streams fetched trade pages directly into one staged output file while preserving strict trade identifier ordering.
    /// </summary>
    private sealed class StreamingTradeGapSplicer : ITradeGapFetchedPageSink, IAsyncDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly SpliceOutputWriter _outputWriter;
        private readonly TradeGapHealRequest _request;
        private readonly ResolvedFile _resolvedFile;
        private IAsyncEnumerator<string>? _currentFileEnumerator;
        private ParsedLocalTrade? _currentLocalTrade;
        private MissingTradeIdRange? _currentMissingRange;
        private int _currentLineNumber;
        private int _insertedTradeCount;
        private bool _hasOpenedLocalFile;
        private long? _lastFetchedTradeId;
        private long? _lastLocalTradeId;

        /// <summary>
        /// Initializes the streaming splicer for one resolved file and one sequence of exact missing ranges.
        /// </summary>
        public StreamingTradeGapSplicer(TradeGapHealRequest request, ResolvedFile resolvedFile, CancellationToken cancellationToken)
        {
            _request = request ?? throw new ArgumentNullException(nameof(request));
            _resolvedFile = resolvedFile ?? throw new ArgumentNullException(nameof(resolvedFile));
            _cancellationToken = cancellationToken;
            _outputWriter = new SpliceOutputWriter(request, _resolvedFile);
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
        /// Moves the local stream forward until the next exact missing range begins.
        /// </summary>
        public async Task AdvanceToMissingRange(MissingTradeIdRange missingRange)
        {
            if (_currentMissingRange is not null)
            {
                throw new InvalidOperationException("Cannot advance to the next missing range before the current range is completed.");
            }

            while (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false)
                && _currentLocalTrade!.NumericTradeId < missingRange.FirstTradeId)
            {
                await _outputWriter.WriteTrade(_currentLocalTrade.Trade, _cancellationToken).ConfigureAwait(false);
                _currentLocalTrade = null;
            }

            _currentMissingRange = missingRange;
        }

        /// <summary>
        /// Completes validation for the current missing range after its exact fetch finishes.
        /// </summary>
        public async Task CompleteMissingRange(MissingTradeIdRange missingRange)
        {
            if (_currentMissingRange is null || _currentMissingRange.Value != missingRange)
            {
                throw new InvalidOperationException("Missing range completion does not match the active exact fetch range.");
            }

            if (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false)
                && _currentLocalTrade!.NumericTradeId <= missingRange.LastTradeId)
            {
                throw new InvalidOperationException(
                    FormattableString.Invariant(
                        $"Local trade id '{_currentLocalTrade.NumericTradeId}' in '{_currentLocalTrade.FullPath}' at line {_currentLocalTrade.LineNumber} overlaps exact missing range {missingRange.FirstTradeId}-{missingRange.LastTradeId}."));
            }

            _currentMissingRange = null;
        }

        /// <summary>
        /// Accepts one downloaded page and writes fetched trades directly into the staged output.
        /// </summary>
        public async ValueTask AcceptPage(IReadOnlyList<TradeInfo> pageTrades, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(pageTrades);
            if (_currentMissingRange is null)
            {
                throw new InvalidOperationException("Fetched trade page arrived without an active exact missing range.");
            }

            foreach (var pageTrade in pageTrades)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var normalizedTrade = ValidateFetchedTrade(pageTrade, _request);
                var fetchedTradeId = ParseNumericTradeId(normalizedTrade.Key.TradeId, "Fetched trade id must be numeric.");
                if (fetchedTradeId < _currentMissingRange.Value.FirstTradeId || fetchedTradeId > _currentMissingRange.Value.LastTradeId)
                {
                    throw new InvalidOperationException(
                        $"Fetched trade id '{fetchedTradeId}' is outside active exact missing range {_currentMissingRange.Value.FirstTradeId}-{_currentMissingRange.Value.LastTradeId}.");
                }

                ObserveFetchedTradeId(fetchedTradeId);
                FetchedTradeCount++;

                if (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false)
                    && _currentLocalTrade!.NumericTradeId < fetchedTradeId)
                {
                    throw new InvalidOperationException(
                        FormattableString.Invariant(
                            $"Local trade id '{_currentLocalTrade.NumericTradeId}' in '{_currentLocalTrade.FullPath}' at line {_currentLocalTrade.LineNumber} overlaps exact missing range {_currentMissingRange.Value.FirstTradeId}-{_currentMissingRange.Value.LastTradeId}."));
                }

                if (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false)
                    && _currentLocalTrade!.NumericTradeId == fetchedTradeId)
                {
                    throw new InvalidOperationException(
                        FormattableString.Invariant(
                            $"Fetched trade id '{fetchedTradeId}' equals existing local trade id in '{_currentLocalTrade.FullPath}' at line {_currentLocalTrade.LineNumber}."));
                }

                await _outputWriter.WriteTrade(normalizedTrade, cancellationToken).ConfigureAwait(false);
                _insertedTradeCount++;
            }
        }

        /// <summary>
        /// Finalizes the staged output after all pages have been consumed.
        /// </summary>
        public async Task<SpliceResult> Complete()
        {
            if (_currentMissingRange is not null)
            {
                throw new InvalidOperationException("Cannot complete file splice while an exact missing range is still active.");
            }

            while (await EnsureCurrentLocalTradeLoaded().ConfigureAwait(false))
            {
                await _outputWriter.WriteTrade(_currentLocalTrade!.Trade, _cancellationToken).ConfigureAwait(false);
                _currentLocalTrade = null;
            }

            IReadOnlyList<TradeGapAffectedFile> affectedFiles = [await _outputWriter.Commit().ConfigureAwait(false)];
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
                        result = await OpenFile().ConfigureAwait(false);
                        if (!result)
                        {
                            break;
                        }
                    }

                    if (!await _currentFileEnumerator!.MoveNextAsync().ConfigureAwait(false))
                    {
                        await _currentFileEnumerator.DisposeAsync().ConfigureAwait(false);
                        _currentFileEnumerator = null;
                        _currentLineNumber = 0;
                        continue;
                    }

                    _currentLineNumber++;
                    var line = _currentFileEnumerator.Current;
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    var localTrade = ParseExistingTrade(line, _request, _resolvedFile.FullPath, _currentLineNumber);
                    var localTradeId = ParseNumericTradeId(
                        localTrade.Key.TradeId,
                        FormattableString.Invariant($"TradeId '{localTrade.Key.TradeId}' at line {_currentLineNumber} in '{_resolvedFile.FullPath}' is not numeric."));

                    if (_lastLocalTradeId is not null && localTradeId <= _lastLocalTradeId.Value)
                    {
                        throw new InvalidOperationException(
                            $"Local trade id '{localTradeId}' at line {_currentLineNumber} in '{_resolvedFile.FullPath}' is not strictly ascending after '{_lastLocalTradeId.Value}'.");
                    }

                    _lastLocalTradeId = localTradeId;
                    _currentLocalTrade = new ParsedLocalTrade(localTrade, localTradeId, _resolvedFile.FullPath, _currentLineNumber);
                    break;
                }
            }

            return result && _currentLocalTrade is not null;
        }

        /// <summary>
        /// Opens the local file reader once for the resolved file.
        /// </summary>
        private Task<bool> OpenFile()
        {
            var result = false;

            if (!_hasOpenedLocalFile)
            {
                _currentFileEnumerator = File.ReadLinesAsync(_resolvedFile.FullPath, _cancellationToken).GetAsyncEnumerator(_cancellationToken);
                _currentLineNumber = 0;
                _hasOpenedLocalFile = true;
                result = true;
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
    /// Streams spliced JSONL output into one staged temp file for atomic replacement.
    /// </summary>
    private sealed class SpliceOutputWriter(TradeGapHealRequest request, LocalFileTradeGapHealer.ResolvedFile resolvedFile) : IAsyncDisposable
    {
        private readonly TradeGapHealRequest _request = request ?? throw new ArgumentNullException(nameof(request));
        private readonly ResolvedFile _resolvedFile = resolvedFile ?? throw new ArgumentNullException(nameof(resolvedFile));
        private StagedFile? _stagedFile;
        private StreamWriter? _writer;

        public async Task<TradeGapAffectedFile> Commit()
        {
            if (_writer is not null)
            {
                await _writer.FlushAsync().ConfigureAwait(false);
                await _writer.DisposeAsync().ConfigureAwait(false);
                _writer = null;
            }

            if (_stagedFile is null)
            {
                throw new InvalidOperationException("No staged file was created for the current splice.");
            }

            await ReplaceFilesAsync([_stagedFile]).ConfigureAwait(false);
            return new TradeGapAffectedFile(_stagedFile.RelativePath, _stagedFile.TradingDay);
        }

        public async ValueTask DisposeAsync()
        {
            if (_writer is not null)
            {
                await _writer.DisposeAsync().ConfigureAwait(false);
                _writer = null;
            }

            if (_stagedFile is not null)
            {
                CleanupStageArtifacts([_stagedFile]);
            }
        }

        public async Task WriteTrade(TradeInfo trade, CancellationToken terminator)
        {
            terminator.ThrowIfCancellationRequested();
            var writer = await GetWriter().ConfigureAwait(false);
            await writer.WriteLineAsync(TradeJsonlFile.SerializeTrade(trade)).ConfigureAwait(false);
        }

        private Task<StreamWriter> GetWriter()
        {
            if (_writer is not null)
            {
                return Task.FromResult(_writer);
            }

            var fullPath = Path.GetFullPath(Path.Combine(_request.RootDirectory, _resolvedFile.RelativePath));
            var directory = Path.GetDirectoryName(fullPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var tempPath = fullPath + ".healing." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".tmp";
            var backupPath = fullPath + ".healing." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".bak";
            var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
            _writer = new StreamWriter(stream);
            _stagedFile = new StagedFile(_resolvedFile.RelativePath, fullPath, _resolvedFile.TradingDay, tempPath, backupPath);
            return Task.FromResult(_writer);
        }
    }
}
