using LogMachina;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Uploads finalized local UTC day trade files to S3.
/// </summary>
public sealed class TradeSinkS3Simple : ITradeFinalizedFileDispatcher
{
    private readonly ILogMachina<TradeSinkS3Simple> _log;
    private readonly TradeSinkS3SimpleOptions _options;
    private readonly IS3ObjectUploader _uploader;
    /// <summary>
    /// Initializes the S3 trade sink.
    /// </summary>
    public TradeSinkS3Simple(TradeSinkS3SimpleOptions options, IS3ObjectUploader uploader, ILogMachina<TradeSinkS3Simple> log)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _uploader = uploader ?? throw new ArgumentNullException(nameof(uploader));
        _log = log ?? throw new ArgumentNullException(nameof(log));

        if (string.IsNullOrWhiteSpace(_options.BucketName))
        {
            throw new ArgumentException("BucketName is required for the S3 trade sink.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(_options.LocalRootDirectory))
        {
            throw new ArgumentException("LocalRootDirectory is required for the S3 trade sink.", nameof(options));
        }
    }

    /// <summary>
    /// Uploads the supplied finalized local day file to S3 and deletes it only after success.
    /// </summary>
    public async ValueTask DispatchAsync(Instrument instrument, DateOnly utcDate, string finalizedFilePath, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(finalizedFilePath);

        cancellationToken.ThrowIfCancellationRequested();
        _ = TradeLocalDailyFilePath.ValidateFinalized(_options.LocalRootDirectory, instrument, utcDate, finalizedFilePath);

        var payload = await File.ReadAllTextAsync(finalizedFilePath, cancellationToken).ConfigureAwait(false);
        var objectKey = TradeSinkS3DailyObjectKey.Build(_options.Prefix, instrument.ToString(), utcDate);
        _log.Info($"Trade S3 upload start: bucket={_options.BucketName}, objectKey={objectKey}, path={finalizedFilePath}.");

        try
        {
            await _uploader.UploadTextAsync(_options.BucketName, objectKey, payload, cancellationToken).ConfigureAwait(false);
            _log.Info($"Trade S3 upload success: bucket={_options.BucketName}, objectKey={objectKey}.");
            File.Delete(finalizedFilePath);
            _log.Info($"Trade S3 local delete after upload: path={finalizedFilePath}.");
        }
        catch (Exception ex)
        {
            _log.Warn($"Trade S3 upload failure: bucket={_options.BucketName}, objectKey={objectKey}, path={finalizedFilePath}.");
            _log.Error(ex);
            throw;
        }
    }
}
