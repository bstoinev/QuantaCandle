using System.Threading;
using System.Threading.Tasks;

namespace QuantaCandle.Infra;

/// <summary>
/// Uploads text payloads and local files to S3 object storage.
/// </summary>
public interface IS3ObjectUploader
{
    /// <summary>
    /// Uploads a local file to the provided S3 object key.
    /// </summary>
    Task UploadFileAsync(string bucketName, string objectKey, string filePath, CancellationToken cancellationToken);

    /// <summary>
    /// Uploads a text payload to the provided S3 object key.
    /// </summary>
    Task UploadTextAsync(string bucketName, string objectKey, string payload, CancellationToken cancellationToken);
}
