using Amazon.S3;
using Amazon.S3.Model;

namespace QuantaCandle.Infra;

/// <summary>
/// S3 uploader backed by AWSSDK.S3.
/// </summary>
public sealed class AwsS3Uploader(IAmazonS3 s3Client) : IS3ObjectUploader
{
    /// <summary>
    /// Uploads a local file to S3 without first buffering it into a managed string.
    /// </summary>
    public async Task UploadFileAsync(string bucketName, string key, string filePath, CancellationToken cancellationToken)
    {
        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = "application/x-ndjson; charset=utf-8",
            FilePath = filePath,
        };

        await s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Uploads an in-memory text payload to S3.
    /// </summary>
    public async Task UploadTextAsync(string bucketName, string key, string payload, CancellationToken cancellationToken)
    {
        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            ContentType = "application/x-ndjson; charset=utf-8",
            ContentBody = payload,
        };

        await s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
