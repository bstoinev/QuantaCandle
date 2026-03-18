using System.Threading;
using System.Threading.Tasks;

namespace QuantaCandle.Service.Stubs;

/// <summary>
/// Uploads text payloads to S3 object storage.
/// </summary>
public interface IS3ObjectUploader
{
    Task UploadTextAsync(string bucketName, string objectKey, string payload, CancellationToken cancellationToken);
}
