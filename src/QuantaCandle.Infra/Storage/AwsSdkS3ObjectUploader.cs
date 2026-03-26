using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace QuantaCandle.Infra;

/// <summary>
/// S3 uploader backed by AWSSDK.S3.
/// </summary>
public sealed class AwsSdkS3ObjectUploader : IS3ObjectUploader, IDisposable
{
    private readonly IAmazonS3 s3Client;
    private readonly bool ownsClient;

    /// <summary>
    /// Creates a new uploader using default AWS SDK credential resolution.
    /// </summary>
    public AwsSdkS3ObjectUploader()
        : this(new AmazonS3Client(), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates a new uploader using an existing S3 client.
    /// </summary>
    public AwsSdkS3ObjectUploader(IAmazonS3 s3Client)
        : this(s3Client, ownsClient: false)
    {
    }

    private AwsSdkS3ObjectUploader(IAmazonS3 s3Client, bool ownsClient)
    {
        this.s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        this.ownsClient = ownsClient;
    }

    public async Task UploadTextAsync(string bucketName, string objectKey, string payload, CancellationToken cancellationToken)
    {
        PutObjectRequest request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            ContentType = "application/x-ndjson; charset=utf-8",
            ContentBody = payload,
        };

        await s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (ownsClient)
        {
            s3Client.Dispose();
        }
    }
}
